"""Pydantic models for data pipeline lineage representation.

These models define the JSON schema for capturing column-level lineage from
SQL queries, stored procedures, Informatica mappings, Ab-Initio graphs, and
Pandas ETL code.  The schema is designed so that the same JSON can be
programmatically converted to:

  1. A multi-sheet Excel workbook  (see excel_export.py)
  2. A Mermaid flowchart diagram   (see mermaid_export.py)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


# ── Column & Table ──────────────────────────────────────────────────


class ColumnInfo(BaseModel):
    """A column within a table or dataset."""

    name: str = Field(description="Column name")
    data_type: str = Field(
        default="",
        description="Data type, e.g. INT, VARCHAR(100), FLOAT, TIMESTAMP",
    )
    description: str = Field(
        default="", description="Business description of the column"
    )


class TableInfo(BaseModel):
    """A database table, view, file, or dataset."""

    schema_name: str = Field(default="", description="Schema / database name")
    table_name: str = Field(description="Table or dataset name")
    columns: list[ColumnInfo] = Field(
        default_factory=list, description="Columns in this table"
    )
    table_type: str = Field(
        default="",
        description="source, target, or intermediate",
    )

    @property
    def full_name(self) -> str:
        """Return schema.table if schema is set, else just table."""
        if self.schema_name:
            return f"{self.schema_name}.{self.table_name}"
        return self.table_name


# ── Lineage detail ──────────────────────────────────────────────────


class IntermediateStep(BaseModel):
    """One step in a column's transformation journey through the pipeline."""

    component_name: str = Field(
        description="Name of CTE, stored proc, subquery, or transformation step"
    )
    expression: str = Field(
        description="The SQL / code expression at this step"
    )
    output_column: str = Field(
        description="Column name output by this step"
    )


class ColumnLineage(BaseModel):
    """Maps a single target column back to its source column(s).

    Use ``table.column`` notation everywhere so the reader always knows which
    table a column belongs to.
    """

    target_table: str = Field(
        description="Target table as schema.table or just table"
    )
    target_column: str = Field(description="Target column name")
    source_columns: list[str] = Field(
        description=(
            "Source columns as table.column or schema.table.column.  "
            "List ALL source columns that contribute to this target column."
        )
    )
    transformation: str = Field(
        description="The transformation expression / logic applied"
    )
    transformation_type: str = Field(
        description=(
            "Category: direct_copy, aggregation, calculation, case_logic, "
            "join_key, window_function, type_cast, concatenation, lookup, "
            "conditional, constant, string_manipulation, date_manipulation, "
            "coalesce, or other"
        )
    )
    intermediate_steps: list[IntermediateStep] = Field(
        default_factory=list,
        description="Ordered steps showing how the column flows through CTEs / procs",
    )
    notes: str = Field(
        default="",
        description="Additional context about this lineage path",
    )


# ── Pipeline components & graph ─────────────────────────────────────


class Component(BaseModel):
    """A processing component — CTE, stored proc, subquery, temp table, etc."""

    name: str = Field(
        description="Component name (CTE alias, proc name, mapping name, …)"
    )
    component_type: str = Field(
        description=(
            "Type: CTE, stored_procedure, subquery, temp_table, view, "
            "transformation, pandas_step, informatica_mapping, "
            "abinitio_component, or other"
        )
    )
    description: str = Field(
        default="", description="What this component does"
    )
    input_tables: list[str] = Field(
        default_factory=list,
        description="Input table references (schema.table or table)",
    )
    output_columns: list[str] = Field(
        default_factory=list,
        description="Columns produced by this component",
    )
    sql_text: str = Field(
        default="",
        description="Relevant code / SQL snippet (first ~200 chars)",
    )


class DataFlowEdge(BaseModel):
    """An edge in the data-flow graph connecting tables and components."""

    from_node: str = Field(
        description="Source node (table full_name or component name)"
    )
    to_node: str = Field(
        description="Target node (table full_name or component name)"
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Columns flowing along this edge",
    )
    edge_label: str = Field(default="", description="Optional edge label")


# ── Root model ──────────────────────────────────────────────────────


class PipelineLineage(BaseModel):
    """Root model — complete lineage for a data pipeline.

    This is the JSON schema the LLM must produce and the converters consume.
    """

    pipeline_name: str = Field(description="Name of the pipeline / process")
    pipeline_type: str = Field(
        description=(
            "Type: sql_query, stored_procedures, informatica, "
            "abinitio, pandas_etl, or mixed"
        )
    )
    source_file: str = Field(
        default="", description="Original source file path"
    )
    description: str = Field(
        default="",
        description="High-level description of what the pipeline does",
    )
    sources: list[TableInfo] = Field(
        description="All source / input tables"
    )
    targets: list[TableInfo] = Field(
        description="All target / output tables"
    )
    components: list[Component] = Field(
        default_factory=list,
        description="Processing components (CTEs, procs, transforms, …)",
    )
    column_lineage: list[ColumnLineage] = Field(
        description="Column-level lineage mappings"
    )
    data_flow_edges: list[DataFlowEdge] = Field(
        default_factory=list,
        description="Data-flow graph edges between tables and components",
    )

    # ── Serialization helpers ───────────────────────────────────────

    def to_json(self, path: str | Path | None = None, **kw: Any) -> str:
        """Serialize to JSON string. Optionally write to *path*."""
        text = self.model_dump_json(indent=2, **kw)
        if path is not None:
            Path(path).write_text(text, encoding="utf-8")
        return text

    @classmethod
    def from_json(cls, path_or_text: str | Path) -> "PipelineLineage":
        """Load from a JSON file path or raw JSON string."""
        raw = str(path_or_text).strip()
        # If it looks like JSON, parse directly
        if raw.startswith("{"):
            return cls(**json.loads(raw))
        # Otherwise treat as a file path
        p = Path(raw)
        text = p.read_text(encoding="utf-8")
        return cls(**json.loads(text))
