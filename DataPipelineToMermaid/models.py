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

from pydantic import BaseModel, Field, model_validator


# ── Column & Table ──────────────────────────────────────────────────


class SourceColumnRef(BaseModel):
    """A reference to a single source column, split into table and column name.

    Mirrors the ``target_table`` / ``target_column`` split on the target side
    so that source-column documentation has the same granularity.
    Use ``schema.table`` notation for ``source_table`` when a schema is
    available.
    """

    source_table: str = Field(
        description="Source table (schema.table or just table)"
    )
    source_column: str = Field(description="Source column name")


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

    Each ``source_refs`` entry carries both the source table and source column
    as separate fields, matching the granularity of ``target_table`` /
    ``target_column``.  ``transformation`` must be expressed as a SQL
    expression ending with ``AS <output_column_name>``.
    """

    target_table: str = Field(
        description="Target table as schema.table or just table"
    )
    target_column: str = Field(description="Target column name")
    source_refs: list[SourceColumnRef] = Field(
        description=(
            "Source column references.  Each entry has ``source_table`` "
            "(schema.table notation) and ``source_column`` (column name). "
            "List ALL source columns that contribute to this target column."
        )
    )
    transformation: str = Field(
        description=(
            "The COMPLETE SQL expression for this mapping, always ending "
            "with ``AS <output_column_name>``.  For non-SQL sources "
            "(Informatica, Ab-Initio, Pandas) translate the logic into "
            "equivalent SQL.  Example: "
            "``CONCAT(first_name, ' ', last_name) AS customer_name``"
        )
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
    source_filenames: list[str] = Field(
        default_factory=list,
        description=(
            "All input files traversed to produce this column (cross-file lineage). "
            "Populated by cross_file_tracer; empty for single-file extractions."
        ),
    )
    filename: str = Field(
        default="",
        description=(
            "The file that produces the final output column (target file). "
            "For cross-file lineage this is the last file in the chain; "
            "for single-file extractions it is the only file."
        ),
    )
    notes: str = Field(
        default="",
        description="Additional context about this lineage path",
    )

    @model_validator(mode="before")
    @classmethod
    def _migrate_source_columns(cls, data: Any) -> Any:
        """Accept the legacy ``source_columns: list[str]`` format.

        Converts each ``"schema.table.column"`` string into a
        ``SourceColumnRef`` dict by splitting on the last ``.``.
        """
        if not isinstance(data, dict):
            return data
        if "source_columns" in data and "source_refs" not in data:
            source_refs = []
            for sc in data.pop("source_columns", []):
                parts = str(sc).rsplit(".", 1)
                if len(parts) == 2:
                    source_refs.append(
                        {"source_table": parts[0], "source_column": parts[1]}
                    )
                else:
                    source_refs.append(
                        {"source_table": "", "source_column": sc}
                    )
            data["source_refs"] = source_refs
        return data


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

    def to_json(
        self,
        path: str | Path | None = None,
        skip_lineage_fields: list[str] | None = None,
        **kw: Any,
    ) -> str:
        """Serialize to JSON string.  Optionally write to *path*.

        Parameters
        ----------
        path:
            If given, write the JSON to this file path.
        skip_lineage_fields:
            Optional list of ``ColumnLineage`` field names to omit from each
            entry in the ``column_lineage`` array.  Use this to save tokens
            or reduce output verbosity.  Example::

                lineage.to_json("out.json", skip_lineage_fields=["notes", "intermediate_steps"])

            Allowed values: ``notes``, ``filename``, ``intermediate_steps``,
            ``transformation_type``.  Core identity fields (``target_table``,
            ``target_column``, ``source_refs``, ``transformation``) are always
            included.
        """
        if skip_lineage_fields:
            data = json.loads(self.model_dump_json(**kw))
            for cl_dict in data.get("column_lineage", []):
                for field in skip_lineage_fields:
                    cl_dict.pop(field, None)
            text = json.dumps(data, indent=2)
        else:
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
