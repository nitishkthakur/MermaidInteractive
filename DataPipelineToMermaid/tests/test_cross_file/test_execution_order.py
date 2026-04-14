"""Tests for execution_order.py — LLM-backed pipeline execution order deduction.

All tests marked with @pytest.mark.llm require OPENROUTER_API_KEY.
Run only unit tests: pytest -m "not llm"
Run all tests:       pytest -m llm  (or just pytest)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from DataPipelineToMermaid.execution_order import (
    ExecutionOrderResult,
    FileIOSummary,
    _render_mermaid,
    deduce_execution_order,
)


# ═══════════════════════════════════════════════════════════════════
# Unit tests — no LLM calls
# ═══════════════════════════════════════════════════════════════════


class TestRenderMermaid:
    """Tests for the Mermaid renderer — pure logic, no LLM."""

    def _make_result(self, stages, edges, fps):
        node_map = {fp: FileIOSummary(filepath=fp, writes=["tbl"]) for fp in fps}
        return stages, edges, node_map

    def test_single_stage_no_edges(self):
        fps = ["a.sql", "b.mp"]
        stages = [fps]
        edges = []
        node_map = {fp: FileIOSummary(filepath=fp) for fp in fps}
        mmd = _render_mermaid(stages, edges, node_map)
        assert "flowchart LR" in mmd
        assert "a_sql" in mmd or "a.sql" in mmd

    def test_edges_appear_in_output(self):
        fps = ["a.sql", "b.sql"]
        stages = [["a.sql"], ["b.sql"]]
        edges = [{"from": "a.sql", "to": "b.sql", "via": "stg_orders"}]
        node_map = {fp: FileIOSummary(filepath=fp) for fp in fps}
        mmd = _render_mermaid(stages, edges, node_map)
        assert "stg_orders" in mmd

    def test_parallel_stage_becomes_subgraph(self):
        fps = ["a.sql", "b.sql", "c.sql"]
        stages = [["a.sql", "b.sql"], ["c.sql"]]
        edges = [
            {"from": "a.sql", "to": "c.sql", "via": "x"},
            {"from": "b.sql", "to": "c.sql", "via": "y"},
        ]
        node_map = {fp: FileIOSummary(filepath=fp) for fp in fps}
        mmd = _render_mermaid(stages, edges, node_map)
        assert "subgraph" in mmd
        assert "parallel" in mmd

    def test_file_type_icons(self):
        fps = ["a.sql", "b.mp", "c.xml", "d.py"]
        stages = [fps]
        node_map = {fp: FileIOSummary(filepath=fp) for fp in fps}
        mmd = _render_mermaid(stages, [], node_map)
        assert "🗄" in mmd  # SQL icon
        assert "⚙" in mmd   # mp icon

    def test_duplicate_edges_deduplicated(self):
        fps = ["a.sql", "b.sql"]
        stages = [["a.sql"], ["b.sql"]]
        edges = [
            {"from": "a.sql", "to": "b.sql", "via": "t1"},
            {"from": "a.sql", "to": "b.sql", "via": "t2"},  # same pair
        ]
        node_map = {fp: FileIOSummary(filepath=fp) for fp in fps}
        mmd = _render_mermaid(stages, edges, node_map)
        # Only one arrow between the same pair
        count = mmd.count("a_sql --> b_sql") + mmd.count("a_sql -->|")
        assert count == 1

    def test_valid_mermaid_starts_with_flowchart(self):
        fps = ["x.sql"]
        stages = [fps]
        node_map = {fp: FileIOSummary(filepath=fp) for fp in fps}
        mmd = _render_mermaid(stages, [], node_map)
        assert mmd.strip().startswith("flowchart LR")


# ═══════════════════════════════════════════════════════════════════
# Integration tests — require OPENROUTER_API_KEY
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.llm
class TestExecutionOrderDAG:
    """Full pipeline DAG: 7 files, 2 parallel entry points, complex fan-in."""

    @pytest.fixture(scope="class")
    def result(self, all_files, llm_model) -> ExecutionOrderResult:
        return deduce_execution_order(all_files, model=llm_model, verbose=True)

    def test_returns_execution_order_result(self, result):
        assert isinstance(result, ExecutionOrderResult)

    def test_all_files_appear_in_stages(self, result, all_files):
        """Every input file must appear in exactly one stage."""
        staged = [f for stage in result.stages for f in stage]
        all_names = {Path(f).name for f in all_files}
        staged_names = {Path(f).name for f in staged}
        assert all_names == staged_names, (
            f"Missing from stages: {all_names - staged_names}\n"
            f"Extra in stages:   {staged_names - all_names}"
        )

    def test_no_file_in_multiple_stages(self, result):
        """No file should appear in more than one stage."""
        seen = {}
        for i, stage in enumerate(result.stages):
            for fp in stage:
                name = Path(fp).name
                assert name not in seen, (
                    f"'{name}' appears in both stage {seen[name]} and stage {i}"
                )
                seen[name] = i

    def test_raw_sql_files_before_downstream_mp(self, result):
        """01/02/03 (raw SQL) must precede 04 (enrich_orders.mp)."""
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(result.stages)
            for fp in stage
        }
        assert stage_of["04_enrich_orders.mp"] > stage_of["01_raw_orders.sql"]
        assert stage_of["04_enrich_orders.mp"] > stage_of["02_raw_customers.sql"]

    def test_enrich_and_payments_before_reconcile(self, result):
        """04_enrich_orders.mp and 03_raw_payments.sql must precede 05."""
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(result.stages)
            for fp in stage
        }
        assert stage_of["05_reconcile_payments.sql"] > stage_of["04_enrich_orders.mp"]
        assert stage_of["05_reconcile_payments.sql"] > stage_of["03_raw_payments.sql"]

    def test_reconcile_before_risk(self, result):
        """05 must precede 06_customer_risk.mp."""
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(result.stages)
            for fp in stage
        }
        assert stage_of["06_customer_risk.mp"] > stage_of["05_reconcile_payments.sql"]

    def test_final_metrics_is_last(self, result):
        """07_final_metrics.sql must be in the last stage."""
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(result.stages)
            for fp in stage
        }
        max_stage = max(stage_of.values())
        assert stage_of["07_final_metrics.sql"] == max_stage

    def test_parallel_raw_files_same_or_adjacent_stage(self, result):
        """01, 02, 03 have no dependency on each other — should be in same or adjacent stages."""
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(result.stages)
            for fp in stage
        }
        s1 = stage_of["01_raw_orders.sql"]
        s2 = stage_of["02_raw_customers.sql"]
        s3 = stage_of["03_raw_payments.sql"]
        # Accept same stage or at most 1 apart (some LLMs produce sub-optimal but valid orderings)
        assert abs(s1 - s2) <= 1
        assert abs(s1 - s3) <= 1
        assert abs(s2 - s3) <= 1

    def test_has_edges(self, result):
        """The LLM must detect at least some dependency edges."""
        assert len(result.edges) >= 4

    def test_edges_reference_known_tables(self, result):
        """Edge 'via' fields should contain recognisable staging table names."""
        known_tables = {
            "stg_orders", "stg_customers", "stg_payments",
            "enr_orders", "reconciled_orders", "customer_risk_scores",
        }
        edge_vias = {e["via"].lower().replace("dbo.", "") for e in result.edges}
        overlap = known_tables & edge_vias
        assert len(overlap) >= 2, (
            f"Expected ≥2 known tables in edge labels, got: {edge_vias}"
        )

    def test_mermaid_is_valid_string(self, result):
        assert result.mermaid.strip().startswith("flowchart LR")
        assert len(result.mermaid) > 100

    def test_mermaid_contains_all_filenames(self, result, all_files):
        for fp in all_files:
            name_safe = re.sub(r"[^a-zA-Z0-9]", "_", Path(fp).name)
            assert name_safe in result.mermaid, (
                f"'{Path(fp).name}' not rendered in Mermaid output"
            )


@pytest.mark.llm
class TestExecutionOrderSubset:
    """Test with a subset: just the two SQL raw files + the Ab-Initio that joins them."""

    @pytest.fixture(scope="class")
    def result(self, fixtures_dir, llm_model) -> ExecutionOrderResult:
        files = [
            str(fixtures_dir / "01_raw_orders.sql"),
            str(fixtures_dir / "02_raw_customers.sql"),
            str(fixtures_dir / "04_enrich_orders.mp"),
        ]
        return deduce_execution_order(files, model=llm_model, verbose=True)

    def test_mp_file_is_last(self, result):
        stage_of = {
            Path(fp).name: i
            for i, stage in enumerate(result.stages)
            for fp in stage
        }
        assert stage_of["04_enrich_orders.mp"] > stage_of["01_raw_orders.sql"]
        assert stage_of["04_enrich_orders.mp"] > stage_of["02_raw_customers.sql"]

    def test_correct_number_of_files(self, result):
        staged = [f for stage in result.stages for f in stage]
        assert len(staged) == 3


@pytest.mark.llm
class TestExecutionOrderSingleFile:
    """Single-file degenerate case — no ordering to infer."""

    @pytest.fixture(scope="class")
    def result(self, fixtures_dir, llm_model) -> ExecutionOrderResult:
        return deduce_execution_order(
            [str(fixtures_dir / "01_raw_orders.sql")],
            model=llm_model,
        )

    def test_one_stage_one_file(self, result):
        assert len([f for stage in result.stages for f in stage]) == 1

    def test_no_edges(self, result):
        assert result.edges == []


import re
