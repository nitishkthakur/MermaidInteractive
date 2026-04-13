"""Unit tests for DataPipelineToMermaid.mermaid_export."""

from __future__ import annotations

from pathlib import Path

import pytest

from DataPipelineToMermaid.mermaid_export import (
    _column_level_mermaid,
    _esc,
    _safe_id,
    _standalone_html,
    _table_level_mermaid,
    lineage_to_html,
    lineage_to_mermaid,
    lineage_to_mermaid_file,
)
from DataPipelineToMermaid.models import (
    ColumnInfo,
    ColumnLineage,
    DataFlowEdge,
    PipelineLineage,
    TableInfo,
    Component,
)


# ═══════════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════════


class TestSafeId:
    def test_simple_name(self):
        assert _safe_id("orders") == "orders"

    def test_dotted_name(self):
        assert _safe_id("sales.orders") == "sales_orders"

    def test_starts_with_number(self):
        assert _safe_id("123abc").startswith("n_")

    def test_special_chars(self):
        result = _safe_id("my-table (v2)")
        assert "(" not in result
        assert ")" not in result
        assert "-" not in result

    def test_empty_string(self):
        result = _safe_id("")
        assert isinstance(result, str)


class TestEsc:
    def test_basic_text(self):
        assert _esc("hello") == "hello"

    def test_double_quotes_replaced(self):
        assert '"' not in _esc('say "hello"')

    def test_truncation(self):
        long_text = "a" * 100
        result = _esc(long_text, max_len=20)
        assert len(result) == 20
        assert result.endswith("...")

    def test_newlines_removed(self):
        assert "\n" not in _esc("line1\nline2")


# ═══════════════════════════════════════════════════════════════════
# Table-level Mermaid
# ═══════════════════════════════════════════════════════════════════


class TestTableLevelMermaid:
    def test_starts_with_flowchart(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert mmd.startswith("flowchart LR")

    def test_contains_source_subgraph(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert 'subgraph Sources["📥 Source Tables"]' in mmd

    def test_contains_target_subgraph(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert 'subgraph Targets["📤 Target Tables"]' in mmd

    def test_contains_transforms_subgraph(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert 'subgraph Transforms["⚙️ Transformations"]' in mmd

    def test_contains_source_nodes(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert "sales_customers" in mmd
        assert "sales_orders" in mmd

    def test_contains_target_nodes(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert "analytics_customer_summary" in mmd

    def test_contains_component_nodes(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert "cte_active_customers" in mmd
        assert "cte_order_stats" in mmd

    def test_contains_edges(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert "-->" in mmd

    def test_contains_class_defs(self, sample_lineage):
        mmd = _table_level_mermaid(sample_lineage)
        assert "classDef source" in mmd
        assert "classDef target" in mmd
        assert "classDef transform" in mmd

    def test_no_duplicate_edges(self, sample_lineage):
        """Each edge pair should appear only once."""
        mmd = _table_level_mermaid(sample_lineage)
        edge_lines = [
            line.strip()
            for line in mmd.split("\n")
            if "-->" in line and "classDef" not in line
        ]
        # Extract from→to pairs
        pairs = []
        for line in edge_lines:
            parts = line.split("-->")
            if len(parts) == 2:
                fr = parts[0].strip()
                to_part = parts[1].strip()
                # Handle pipe labels: |"label"| node
                if "|" in to_part:
                    to_part = to_part.split("|")[-1].strip()
                pairs.append((fr, to_part))

        assert len(pairs) == len(set(pairs)), "Duplicate edges found"


# ═══════════════════════════════════════════════════════════════════
# Column-level Mermaid
# ═══════════════════════════════════════════════════════════════════


class TestColumnLevelMermaid:
    def test_starts_with_flowchart(self, sample_lineage):
        mmd = _column_level_mermaid(sample_lineage)
        assert mmd.startswith("flowchart LR")

    def test_contains_source_column_nodes(self, sample_lineage):
        mmd = _column_level_mermaid(sample_lineage)
        # sales.customers.customer_id → sales_customers_customer_id
        assert "sales_customers_customer_id" in mmd

    def test_contains_target_column_nodes(self, sample_lineage):
        mmd = _column_level_mermaid(sample_lineage)
        # analytics.customer_summary.customer_name
        assert "analytics_customer_summary_customer_name" in mmd

    def test_contains_transformation_labels(self, sample_lineage):
        mmd = _column_level_mermaid(sample_lineage)
        assert "direct_copy" in mmd
        assert "aggregation" in mmd

    def test_has_subgraphs_per_table(self, sample_lineage):
        mmd = _column_level_mermaid(sample_lineage)
        # Should have subgraphs for source + target tables
        subgraph_count = mmd.count("subgraph ")
        assert subgraph_count >= len(sample_lineage.sources) + len(
            sample_lineage.targets
        )


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════


class TestLineageToMermaid:
    def test_default_is_table_level(self, sample_lineage):
        mmd = lineage_to_mermaid(sample_lineage)
        assert "Sources" in mmd
        assert "Targets" in mmd

    def test_column_level(self, sample_lineage):
        mmd = lineage_to_mermaid(sample_lineage, detail_level="column")
        # Column level has per-column nodes
        assert "sales_customers_customer_id" in mmd


class TestLineageToMermaidFile:
    def test_writes_mmd_file(self, sample_lineage, tmp_output):
        path = lineage_to_mermaid_file(
            sample_lineage, tmp_output / "test.mmd"
        )
        assert path.exists()
        content = path.read_text()
        assert content.startswith("flowchart LR")

    def test_column_level_file(self, sample_lineage, tmp_output):
        path = lineage_to_mermaid_file(
            sample_lineage,
            tmp_output / "col.mmd",
            detail_level="column",
        )
        content = path.read_text()
        assert "sales_customers_customer_id" in content

    def test_creates_parent_dirs(self, sample_lineage, tmp_path):
        deep = tmp_path / "a" / "b" / "test.mmd"
        path = lineage_to_mermaid_file(sample_lineage, deep)
        assert path.exists()


class TestLineageToHtml:
    def test_writes_html_file(self, sample_lineage, tmp_output):
        path = lineage_to_html(
            sample_lineage, tmp_output / "test.html"
        )
        assert path.exists()
        assert path.suffix == ".html"

    def test_html_contains_mermaid_cdn(self, sample_lineage, tmp_output):
        path = lineage_to_html(
            sample_lineage, tmp_output / "test.html"
        )
        html = path.read_text()
        assert "mermaid" in html.lower()

    def test_html_contains_title(self, sample_lineage, tmp_output):
        path = lineage_to_html(
            sample_lineage, tmp_output / "test.html"
        )
        html = path.read_text()
        assert "Customer Analytics ETL" in html


# ═══════════════════════════════════════════════════════════════════
# Standalone HTML fallback
# ═══════════════════════════════════════════════════════════════════


class TestStandaloneHtml:
    def test_valid_html(self):
        html = _standalone_html("flowchart LR\n  A --> B", "Test Title")
        assert "<!DOCTYPE html>" in html
        assert "<title>Test Title" in html

    def test_escapes_html_entities(self):
        html = _standalone_html("A --> B & C", "T")
        assert "&amp;" in html

    def test_mermaid_div_present(self):
        html = _standalone_html("flowchart LR\n  A --> B", "T")
        assert '<div class="mermaid">' in html

    def test_mermaid_js_cdn_loaded(self):
        html = _standalone_html("flowchart LR\n  A --> B", "T")
        assert "cdn.jsdelivr.net/npm/mermaid" in html


# ═══════════════════════════════════════════════════════════════════
# Edge cases: Empty / minimal lineage
# ═══════════════════════════════════════════════════════════════════


class TestMermaidEdgeCases:
    def test_no_components(self):
        """Pipeline with no components — should skip Transforms subgraph."""
        lineage = PipelineLineage(
            pipeline_name="No comps",
            pipeline_type="sql_query",
            sources=[TableInfo(table_name="src", columns=[ColumnInfo(name="a")])],
            targets=[TableInfo(table_name="tgt", columns=[ColumnInfo(name="a")])],
            column_lineage=[
                ColumnLineage(
                    target_table="tgt",
                    target_column="a",
                    source_columns=["src.a"],
                    transformation="a",
                    transformation_type="direct_copy",
                )
            ],
        )
        mmd = _table_level_mermaid(lineage)
        assert "Transforms" not in mmd
        assert "Sources" in mmd

    def test_no_data_flow_edges_infers_from_lineage(self):
        """Without explicit edges, edges are inferred from column_lineage."""
        lineage = PipelineLineage(
            pipeline_name="Inferred",
            pipeline_type="sql_query",
            sources=[TableInfo(table_name="src", columns=[ColumnInfo(name="a")])],
            targets=[TableInfo(table_name="tgt", columns=[ColumnInfo(name="a")])],
            column_lineage=[
                ColumnLineage(
                    target_table="tgt",
                    target_column="a",
                    source_columns=["src.a"],
                    transformation="a",
                    transformation_type="direct_copy",
                )
            ],
        )
        mmd = _table_level_mermaid(lineage)
        assert "-->" in mmd
        # Edge from src to tgt
        assert "src" in mmd
        assert "tgt" in mmd

    def test_long_label_truncated_in_mermaid(self):
        """Very long table/column names should be truncated."""
        long_name = "a" * 200
        lineage = PipelineLineage(
            pipeline_name="Long",
            pipeline_type="sql_query",
            sources=[
                TableInfo(
                    table_name=long_name,
                    columns=[ColumnInfo(name="x")],
                )
            ],
            targets=[TableInfo(table_name="t", columns=[ColumnInfo(name="x")])],
            column_lineage=[],
        )
        mmd = _table_level_mermaid(lineage)
        # Label should be capped (esc truncates to ~80)
        assert "..." in mmd
