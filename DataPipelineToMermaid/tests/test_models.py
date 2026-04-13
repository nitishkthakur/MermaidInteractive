"""Unit tests for DataPipelineToMermaid.models (Pydantic schema)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from DataPipelineToMermaid.models import (
    ColumnInfo,
    ColumnLineage,
    Component,
    DataFlowEdge,
    IntermediateStep,
    PipelineLineage,
    TableInfo,
)


# ═══════════════════════════════════════════════════════════════════
# ColumnInfo
# ═══════════════════════════════════════════════════════════════════


class TestColumnInfo:
    def test_minimal(self):
        c = ColumnInfo(name="id")
        assert c.name == "id"
        assert c.data_type == ""
        assert c.description == ""

    def test_full(self):
        c = ColumnInfo(name="amount", data_type="DECIMAL(10,2)", description="Total $")
        assert c.data_type == "DECIMAL(10,2)"
        assert c.description == "Total $"

    def test_name_required(self):
        with pytest.raises(ValidationError):
            ColumnInfo()  # type: ignore[call-arg]


# ═══════════════════════════════════════════════════════════════════
# TableInfo
# ═══════════════════════════════════════════════════════════════════


class TestTableInfo:
    def test_full_name_with_schema(self):
        t = TableInfo(schema_name="sales", table_name="orders")
        assert t.full_name == "sales.orders"

    def test_full_name_without_schema(self):
        t = TableInfo(table_name="orders")
        assert t.full_name == "orders"

    def test_columns_list(self):
        t = TableInfo(
            table_name="t",
            columns=[
                ColumnInfo(name="a"),
                ColumnInfo(name="b", data_type="INT"),
            ],
        )
        assert len(t.columns) == 2
        assert t.columns[1].data_type == "INT"

    def test_table_name_required(self):
        with pytest.raises(ValidationError):
            TableInfo(schema_name="s")  # type: ignore[call-arg]


# ═══════════════════════════════════════════════════════════════════
# IntermediateStep & ColumnLineage
# ═══════════════════════════════════════════════════════════════════


class TestColumnLineage:
    def test_minimal_lineage(self):
        cl = ColumnLineage(
            target_table="tgt",
            target_column="col_a",
            source_columns=["src.col_a"],
            transformation="col_a",
            transformation_type="direct_copy",
        )
        assert cl.intermediate_steps == []
        assert cl.notes == ""

    def test_with_intermediate_steps(self):
        step = IntermediateStep(
            component_name="cte_agg",
            expression="SUM(amount)",
            output_column="total",
        )
        cl = ColumnLineage(
            target_table="tgt",
            target_column="total",
            source_columns=["src.amount"],
            transformation="SUM(amount)",
            transformation_type="aggregation",
            intermediate_steps=[step],
            notes="Summed up",
        )
        assert len(cl.intermediate_steps) == 1
        assert cl.intermediate_steps[0].component_name == "cte_agg"


# ═══════════════════════════════════════════════════════════════════
# Component & DataFlowEdge
# ═══════════════════════════════════════════════════════════════════


class TestComponent:
    def test_component_defaults(self):
        c = Component(name="cte_x", component_type="CTE")
        assert c.input_tables == []
        assert c.output_columns == []
        assert c.sql_text == ""

    def test_component_full(self):
        c = Component(
            name="proc_load",
            component_type="stored_procedure",
            description="Loads data",
            input_tables=["a", "b"],
            output_columns=["x", "y"],
            sql_text="INSERT INTO ...",
        )
        assert len(c.input_tables) == 2


class TestDataFlowEdge:
    def test_edge(self):
        e = DataFlowEdge(from_node="a", to_node="b", columns=["x"])
        assert e.edge_label == ""

    def test_edge_with_label(self):
        e = DataFlowEdge(
            from_node="a", to_node="b", columns=["x", "y"], edge_label="JOIN"
        )
        assert e.edge_label == "JOIN"
        assert len(e.columns) == 2


# ═══════════════════════════════════════════════════════════════════
# PipelineLineage — root model
# ═══════════════════════════════════════════════════════════════════


class TestPipelineLineage:
    def test_from_fixture(self, sample_lineage):
        """Load the full fixture and verify structure."""
        assert sample_lineage.pipeline_name == "Customer Analytics ETL"
        assert sample_lineage.pipeline_type == "sql_query"
        assert len(sample_lineage.sources) == 4
        assert len(sample_lineage.targets) == 1
        assert len(sample_lineage.components) == 4
        assert len(sample_lineage.column_lineage) == 10
        assert len(sample_lineage.data_flow_edges) == 10

    def test_source_columns(self, sample_lineage):
        """Verify source table column counts."""
        customers = next(
            s for s in sample_lineage.sources if s.table_name == "customers"
        )
        assert len(customers.columns) == 7
        assert customers.schema_name == "sales"

    def test_target_columns(self, sample_lineage):
        tgt = sample_lineage.targets[0]
        assert tgt.table_name == "customer_summary"
        assert len(tgt.columns) == 10

    def test_serialization_roundtrip(self, sample_lineage, tmp_output):
        """Serialize to JSON and load back — should be identical."""
        json_path = tmp_output / "roundtrip.json"
        sample_lineage.to_json(json_path)

        reloaded = PipelineLineage.from_json(json_path)
        assert reloaded.pipeline_name == sample_lineage.pipeline_name
        assert len(reloaded.sources) == len(sample_lineage.sources)
        assert len(reloaded.column_lineage) == len(sample_lineage.column_lineage)
        assert len(reloaded.data_flow_edges) == len(sample_lineage.data_flow_edges)

    def test_to_json_string(self, sample_lineage):
        """to_json without path returns a JSON string."""
        text = sample_lineage.to_json()
        data = json.loads(text)
        assert data["pipeline_name"] == "Customer Analytics ETL"

    def test_from_json_raw_string(self, sample_lineage):
        """from_json accepts a raw JSON string if path doesn't exist."""
        text = sample_lineage.to_json()
        reloaded = PipelineLineage.from_json(text)
        assert reloaded.pipeline_name == sample_lineage.pipeline_name

    def test_column_lineage_types(self, sample_lineage):
        """Check diverse transformation types are present."""
        types = {cl.transformation_type for cl in sample_lineage.column_lineage}
        assert "direct_copy" in types
        assert "aggregation" in types
        assert "window_function" in types
        assert "case_logic" in types

    def test_minimal_pipeline(self):
        """PipelineLineage with minimal required fields."""
        p = PipelineLineage(
            pipeline_name="test",
            pipeline_type="sql_query",
            sources=[TableInfo(table_name="src")],
            targets=[TableInfo(table_name="tgt")],
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
        assert p.components == []
        assert p.data_flow_edges == []
        assert p.source_file == ""
