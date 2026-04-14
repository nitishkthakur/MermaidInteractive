"""Tests for cross_file_tracer.py — cross-file column lineage tracing.

Test strategy:
  - Uses the 7-file fixture DAG (see fixtures/).
  - LLM calls use the cheap model in .env.
  - Tests verify structural correctness (right table, column, steps, filenames),
    NOT exact SQL expression wording (LLMs paraphrase — that is acceptable).

Run without LLM:  pytest -m "not llm"
Run with LLM:     pytest tests/test_cross_file/test_cross_file_tracer.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from DataPipelineToMermaid.cross_file_tracer import (
    _build_lookup,
    _normalise_key,
    _trace_column,
    trace_columns,
    write_trace_json,
)
from DataPipelineToMermaid.models import ColumnLineage, IntermediateStep, SourceColumnRef


# ═══════════════════════════════════════════════════════════════════
# Unit tests — pure logic, no LLM
# ═══════════════════════════════════════════════════════════════════


class TestNormalisation:
    def test_lowercase(self):
        assert _normalise_key("DBO.Orders") == "dbo.orders"

    def test_strips_whitespace(self):
        assert _normalise_key("  tbl.col  ") == "tbl.col"

    def test_dot_preserved(self):
        assert "." in _normalise_key("schema.table.column")


class TestBuildLookup:
    def test_keys_are_normalised(self):
        cat = {"DBO.STG_ORDERS.order_id": {"produced_by_file": "a.sql", "source_refs": []}}
        lookup = _build_lookup(cat)
        assert "dbo.stg_orders.order_id" in lookup

    def test_values_preserved(self):
        cat = {"tbl.col": {"produced_by_file": "x.sql", "transformation": "raw AS col", "source_refs": []}}
        lookup = _build_lookup(cat)
        assert lookup["tbl.col"]["produced_by_file"] == "x.sql"


class TestTraceColumnUnit:
    """Unit-test the DAG traversal logic with a hand-crafted catalogue."""

    def _make_lookup(self):
        # Simulates a 3-hop chain:
        #   raw.txn_log.gross_value
        #       → stg_orders.raw_amount       (01_raw_orders.sql)
        #       → enr_orders.discounted_amount (04_enrich_orders.mp)
        #       → reconciled_orders.discounted_amount (05_reconcile_payments.sql)
        return _build_lookup({
            "stg_orders.raw_amount": {
                "produced_by_file": "01_raw_orders.sql",
                "transformation": "t.gross_value AS raw_amount",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "raw.txn_log", "source_column": "gross_value"}],
            },
            "enr_orders.discounted_amount": {
                "produced_by_file": "04_enrich_orders.mp",
                "transformation": "raw_amount * (1 - discount_pct / 100.0) AS discounted_amount",
                "transformation_type": "calculation",
                "source_refs": [{"source_table": "stg_orders", "source_column": "raw_amount"}],
            },
            "reconciled_orders.discounted_amount": {
                "produced_by_file": "05_reconcile_payments.sql",
                "transformation": "o.discounted_amount AS discounted_amount",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "enr_orders", "source_column": "discounted_amount"}],
            },
        })

    def test_three_hop_chain_returns_three_steps(self):
        lookup = self._make_lookup()
        steps = _trace_column(
            "reconciled_orders.discounted_amount", lookup, visited=set()
        )
        assert len(steps) == 3

    def test_steps_ordered_raw_to_final(self):
        lookup = self._make_lookup()
        steps = _trace_column(
            "reconciled_orders.discounted_amount", lookup, visited=set()
        )
        assert steps[0].component_name == "01_raw_orders.sql"
        assert steps[-1].component_name == "05_reconcile_payments.sql"

    def test_step_output_columns_correct(self):
        lookup = self._make_lookup()
        steps = _trace_column(
            "reconciled_orders.discounted_amount", lookup, visited=set()
        )
        assert steps[0].output_column == "raw_amount"
        assert steps[1].output_column == "discounted_amount"
        assert steps[2].output_column == "discounted_amount"

    def test_not_found_returns_empty(self):
        lookup = self._make_lookup()
        steps = _trace_column("nonexistent.col", lookup, visited=set())
        assert steps == []

    def test_cycle_guard(self):
        """A catalogue with a cycle must not recurse infinitely."""
        lookup = _build_lookup({
            "a.col": {
                "produced_by_file": "f1.sql",
                "transformation": "b.col AS col",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "b", "source_column": "col"}],
            },
            "b.col": {
                "produced_by_file": "f2.sql",
                "transformation": "a.col AS col",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "a", "source_column": "col"}],
            },
        })
        # Should not raise or loop forever
        steps = _trace_column("a.col", lookup, visited=set(), max_depth=5)
        assert isinstance(steps, list)

    def test_fan_in_multiple_sources(self):
        """A column with two source columns from two different files."""
        lookup = _build_lookup({
            "stg_a.col_x": {
                "produced_by_file": "file_a.sql",
                "transformation": "raw_x AS col_x",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "raw", "source_column": "raw_x"}],
            },
            "stg_b.col_y": {
                "produced_by_file": "file_b.sql",
                "transformation": "raw_y AS col_y",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "raw", "source_column": "raw_y"}],
            },
            "final.combined": {
                "produced_by_file": "file_c.sql",
                "transformation": "col_x + col_y AS combined",
                "transformation_type": "calculation",
                "source_refs": [
                    {"source_table": "stg_a", "source_column": "col_x"},
                    {"source_table": "stg_b", "source_column": "col_y"},
                ],
            },
        })
        steps = _trace_column("final.combined", lookup, visited=set())
        component_names = [s.component_name for s in steps]
        assert "file_a.sql" in component_names
        assert "file_b.sql" in component_names
        assert "file_c.sql" in component_names

    def test_direct_raw_source_returns_one_step(self):
        """Column produced by exactly one file from a raw table → 1 step."""
        lookup = _build_lookup({
            "stg.order_id": {
                "produced_by_file": "01.sql",
                "transformation": "txn_id AS order_id",
                "transformation_type": "direct_copy",
                "source_refs": [{"source_table": "raw.txn_log", "source_column": "txn_id"}],
            }
        })
        steps = _trace_column("stg.order_id", lookup, visited=set())
        assert len(steps) == 1
        assert steps[0].component_name == "01.sql"


class TestWriteTraceJson:
    def test_creates_file(self, tmp_path):
        cl = ColumnLineage(
            target_table="tbl",
            target_column="col",
            source_refs=[SourceColumnRef(source_table="src", source_column="c")],
            transformation="c AS col",
            transformation_type="direct_copy",
        )
        out = write_trace_json([cl], tmp_path / "out.json")
        assert out.exists()

    def test_output_is_list(self, tmp_path):
        cl = ColumnLineage(
            target_table="t", target_column="c",
            source_refs=[], transformation="x AS c",
            transformation_type="other",
        )
        out = write_trace_json([cl], tmp_path / "out.json")
        data = json.loads(out.read_text())
        assert isinstance(data, list)
        assert len(data) == 1


# ═══════════════════════════════════════════════════════════════════
# Integration tests — LLM required
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.llm
class TestTraceNetRevenue:
    """Trace fact_pipeline_metrics.net_revenue — a 4-file chain:
       01_raw_orders → 04_enrich_orders → 05_reconcile_payments → 07_final_metrics
    """

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> list[ColumnLineage]:
        return trace_columns(
            ["dbo.fact_pipeline_metrics.net_revenue"],
            all_files,
            model=llm_model,
            verbose=True,
        )

    def test_returns_one_entry(self, result):
        assert len(result) == 1

    def test_correct_target_table_and_column(self, result):
        cl = result[0]
        assert "fact_pipeline_metrics" in cl.target_table.lower()
        assert cl.target_column == "net_revenue"

    def test_has_source_filenames(self, result):
        """source_filenames lists upstream files (cross-file tracer, not intermediate_steps)."""
        cl = result[0]
        assert len(cl.source_filenames) >= 2

    def test_filename_set_to_final_file(self, result):
        cl = result[0]
        assert "07_final_metrics" in cl.filename

    def test_source_filenames_span_multiple_files(self, result):
        cl = result[0]
        # Must pass through at least 2 distinct upstream files
        assert len(set(cl.source_filenames)) >= 2

    def test_raw_source_refs_are_raw_tables(self, result):
        """source_refs should point to raw tables, not intermediate ones."""
        cl = result[0]
        intermediate_tables = {
            "stg_orders", "enr_orders", "reconciled_orders",
            "stg_customers", "stg_payments",
        }
        for ref in cl.source_refs:
            tbl = ref.source_table.lower().replace("dbo.", "")
            assert tbl not in intermediate_tables, (
                f"source_ref points to intermediate table '{ref.source_table}' "
                "— expected a raw source."
            )

    def test_transformation_type_is_valid(self, result):
        valid_types = {
            "direct_copy", "aggregation", "calculation", "case_logic",
            "join_key", "window_function", "type_cast", "concatenation",
            "lookup", "conditional", "constant", "string_manipulation",
            "date_manipulation", "coalesce", "other",
        }
        assert result[0].transformation_type in valid_types


@pytest.mark.llm
class TestTraceRiskScore:
    """Trace dbo.customer_risk_scores.risk_score — computed in 06_customer_risk.mp
    using a weighted formula over columns from 05_reconcile_payments.sql,
    which itself depends on 04_enrich_orders.mp, 01, 02, 03 SQL files.
    This is the deepest column in the DAG.
    """

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> list[ColumnLineage]:
        return trace_columns(
            ["dbo.customer_risk_scores.risk_score"],
            all_files,
            model=llm_model,
            verbose=True,
        )

    def test_returns_one_entry(self, result):
        assert len(result) == 1

    def test_not_found_note_absent(self, result):
        """risk_score IS in the files — notes should not say 'not found'."""
        assert "not found" not in result[0].notes.lower()

    def test_filename_is_risk_mp(self, result):
        assert "06_customer_risk" in result[0].filename

    def test_has_source_filenames(self, result):
        """risk_score spans multiple files — source_filenames should be populated."""
        assert len(result[0].source_filenames) >= 1

    def test_transformation_contains_risk_logic(self, result):
        """The transformation expression should reference risk components."""
        transformation = result[0].transformation.lower()
        # Should reference high_risk_count, medium_risk_count, or a scoring formula
        assert any(
            kw in transformation
            for kw in ["risk", "high_risk", "medium_risk", "least", "score"]
        ), f"Unexpected transformation: {result[0].transformation}"


@pytest.mark.llm
class TestTraceColumnNotFound:
    """Columns that don't exist in any file → empty row with note."""

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> list[ColumnLineage]:
        return trace_columns(
            ["ghost_table.ghost_column"],
            all_files,
            model=llm_model,
            verbose=True,
        )

    def test_returns_one_entry(self, result):
        assert len(result) == 1

    def test_source_refs_empty(self, result):
        assert result[0].source_refs == []

    def test_transformation_empty(self, result):
        assert result[0].transformation == ""

    def test_note_mentions_not_found(self, result):
        assert "not found" in result[0].notes.lower()


@pytest.mark.llm
class TestTraceMultipleColumns:
    """Trace three columns in one call — tests batch correctness."""

    TARGET_COLUMNS = [
        "dbo.fact_pipeline_metrics.avg_risk_score",
        "dbo.fact_pipeline_metrics.chargeback_rate",
        "dbo.customer_risk_scores.risk_tier",
    ]

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> list[ColumnLineage]:
        return trace_columns(self.TARGET_COLUMNS, all_files, model=llm_model, verbose=True)

    def test_returns_exactly_three_entries(self, result):
        assert len(result) == 3

    def test_each_entry_has_correct_target_column(self, result):
        returned_cols = [cl.target_column for cl in result]
        assert "avg_risk_score" in returned_cols
        assert "chargeback_rate" in returned_cols
        assert "risk_tier" in returned_cols

    def test_avg_risk_score_found_and_traced(self, result):
        """avg_risk_score is NOT a 'not found' column — it must have a non-empty
        transformation and source_refs pointing to customer_risk_scores or risk_score."""
        cl = next(r for r in result if r.target_column == "avg_risk_score")
        assert cl.transformation != "", "avg_risk_score transformation must be non-empty"
        assert cl.notes == "" or "not found" not in cl.notes.lower()
        # The transformation or source_refs must reference the risk domain
        combined = (cl.transformation + " ".join(
            f"{r.source_table}.{r.source_column}" for r in cl.source_refs
        )).lower()
        assert "risk" in combined, (
            f"avg_risk_score should reference risk tables/columns. Got: {combined[:200]}"
        )

    def test_risk_tier_produced_by_risk_mp(self, result):
        cl = next(r for r in result if r.target_column == "risk_tier")
        assert "06_customer_risk" in cl.filename

    def test_chargeback_rate_has_transformation(self, result):
        cl = next(r for r in result if r.target_column == "chargeback_rate")
        assert cl.transformation != ""


@pytest.mark.llm
class TestTraceUnqualifiedColumn:
    """Unqualified column name (no table prefix) should return a clear error note."""

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> list[ColumnLineage]:
        return trace_columns(["net_revenue"], all_files, model=llm_model)

    def test_returns_one_entry(self, result):
        assert len(result) == 1

    def test_notes_mention_qualification(self, result):
        note = result[0].notes.lower()
        assert "qualified" in note or "table.column" in note or "format" in note


@pytest.mark.llm
class TestTraceRenamedColumn:
    """net_order_revenue is computed in 05_reconcile_payments.sql as
    'discounted_amount + loyalty_bonus AS net_order_revenue'.
    It then feeds into 07_final_metrics as net_revenue.
    Tests the rename-tracking capability across files.
    """

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> list[ColumnLineage]:
        return trace_columns(
            ["dbo.reconciled_orders.net_order_revenue"],
            all_files,
            model=llm_model,
            verbose=True,
        )

    def test_found_in_correct_file(self, result):
        assert "05_reconcile_payments" in result[0].filename

    def test_has_source_filenames(self, result):
        """source_filenames lists all upstream files (not the final file)."""
        assert len(result[0].source_filenames) >= 1

    def test_target_file_not_in_source_filenames(self, result):
        """The target file (05_reconcile_payments) must not appear in source_filenames."""
        assert result[0].filename not in result[0].source_filenames

    def test_intermediate_steps_empty(self, result):
        """Cross-file tracer never populates intermediate_steps."""
        assert result[0].intermediate_steps == []

    def test_transformation_references_discounted_and_loyalty(self, result):
        expr = result[0].transformation.lower()
        assert "discounted_amount" in expr or "loyalty" in expr, (
            f"Expected discounted_amount or loyalty in: {expr}"
        )

    def test_transformation_is_sequential_chain(self, result):
        """Transformation string must contain SOURCES line and at least 2 → steps."""
        t = result[0].transformation
        assert "SOURCES:" in t
        assert t.count("→") >= 2


# ═══════════════════════════════════════════════════════════════════
# Stored Procedure pipeline tests — SQL SP + Ab-Initio mixed DAG
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.llm
class TestTraceWithStoredProcs:
    """Extended 9-file pipeline: original 7 files + 2 stored proc files.

    DAG additions:
      08_sp_loyalty_tiers.sql   reads stg_customers + stg_orders → customer_loyalty_tiers
      09_sp_segment_rewards.sql reads customer_loyalty_tiers + customer_risk_scores → segment_rewards

    This tests: SP syntax recognition, multi-technology pipeline (SQL + .mp + SP),
    and deep fan-in (09 depends on branches from 01→08 and 03→05→06).
    """

    @pytest.fixture(scope="class")
    def result(self, all_files_with_sp, llm_model) -> list[ColumnLineage]:
        return trace_columns(
            [
                "dbo.segment_rewards.net_reward_value",
                "dbo.customer_loyalty_tiers.loyalty_tier",
            ],
            all_files_with_sp,
            model=llm_model,
            verbose=True,
        )

    def test_returns_two_entries(self, result):
        assert len(result) == 2

    def test_net_reward_value_found(self, result):
        cl = next(r for r in result if r.target_column == "net_reward_value")
        assert "not found" not in cl.notes.lower()

    def test_net_reward_value_produced_by_sp(self, result):
        cl = next(r for r in result if r.target_column == "net_reward_value")
        assert "09_sp_segment_rewards" in cl.filename

    def test_net_reward_value_source_files_span_sp_and_mp(self, result):
        """net_reward_value depends on both the SP branch and the risk .mp branch."""
        cl = next(r for r in result if r.target_column == "net_reward_value")
        all_files = set(cl.source_filenames) | {cl.filename}
        has_sp = any(".sql" in f and "sp_" in f.lower() for f in all_files)
        has_mp = any(".mp" in f for f in all_files)
        assert has_sp, f"Expected at least one stored-proc file, got: {all_files}"
        assert has_mp, f"Expected at least one .mp file in chain, got: {all_files}"

    def test_loyalty_tier_produced_by_sp(self, result):
        cl = next(r for r in result if r.target_column == "loyalty_tier")
        assert "08_sp_loyalty_tiers" in cl.filename

    def test_loyalty_tier_reads_from_stage0_sql(self, result):
        """loyalty_tier ultimately depends on stg_orders or stg_customers (stage-0 files)."""
        cl = next(r for r in result if r.target_column == "loyalty_tier")
        raw_tables = {r.source_table.lower().replace("dbo.", "") for r in cl.source_refs}
        assert raw_tables & {"stg_customers", "stg_orders", "crm.customers", "raw.txn_log"}, (
            f"Expected raw source tables, got: {raw_tables}"
        )

    def test_transformation_contains_sources_line(self, result):
        for cl in result:
            if cl.notes:  # skip not-found entries
                continue
            assert "SOURCES:" in cl.transformation, (
                f"SOURCES line missing for {cl.target_column}"
            )

    def test_intermediate_steps_empty_for_sp_pipeline(self, result):
        for cl in result:
            assert cl.intermediate_steps == []

    def test_source_filenames_populated(self, result):
        cl = next(r for r in result if r.target_column == "net_reward_value")
        assert len(cl.source_filenames) >= 2

    def test_target_file_absent_from_source_filenames(self, result):
        for cl in result:
            if cl.filename:
                assert cl.filename not in cl.source_filenames, (
                    f"Target file '{cl.filename}' should not be in source_filenames"
                )


@pytest.mark.llm
class TestExecutionOrderWithStoredProcs:
    """Test that execution_order correctly stages the stored proc files."""

    @pytest.fixture(scope="class")
    def order_result(self, all_files_with_sp, llm_model):
        from DataPipelineToMermaid.execution_order import deduce_execution_order
        return deduce_execution_order(all_files_with_sp, model=llm_model, verbose=True)

    def test_all_9_files_staged(self, order_result, all_files_with_sp):
        from pathlib import Path
        staged = {Path(f).name for stage in order_result.stages for f in stage}
        expected = {Path(f).name for f in all_files_with_sp}
        assert staged == expected

    def test_sp_loyalty_after_stage0(self, order_result):
        from pathlib import Path
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(order_result.stages)
            for fp in stage
        }
        assert stage_of["08_sp_loyalty_tiers.sql"] > stage_of["01_raw_orders.sql"]
        assert stage_of["08_sp_loyalty_tiers.sql"] > stage_of["02_raw_customers.sql"]

    def test_sp_rewards_after_loyalty_and_risk(self, order_result):
        from pathlib import Path
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(order_result.stages)
            for fp in stage
        }
        assert stage_of["09_sp_segment_rewards.sql"] > stage_of["08_sp_loyalty_tiers.sql"]
        assert stage_of["09_sp_segment_rewards.sql"] > stage_of["06_customer_risk.mp"]
