"""Unit tests for DataPipelineToMermaid.excel_export."""

from __future__ import annotations

from pathlib import Path

import pytest
from openpyxl import load_workbook

from DataPipelineToMermaid.excel_export import lineage_to_excel
from DataPipelineToMermaid.models import (
    ColumnInfo,
    ColumnLineage,
    Component,
    DataFlowEdge,
    PipelineLineage,
    TableInfo,
)


# ═══════════════════════════════════════════════════════════════════
# Full fixture export
# ═══════════════════════════════════════════════════════════════════


class TestExcelExportFull:
    """Test Excel export using the full sample_lineage.json fixture."""

    def test_creates_xlsx_file(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        assert path.exists()
        assert path.suffix == ".xlsx"

    def test_returns_path_object(self, sample_lineage, tmp_output):
        result = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        assert isinstance(result, Path)

    def test_has_six_sheets(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        assert wb.sheetnames == [
            "Overview",
            "Source Tables",
            "Target Tables",
            "Column Lineage",
            "Components",
            "Data Flow",
        ]
        wb.close()

    def test_overview_sheet_content(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Overview"]

        # Row 1 = headers
        assert ws.cell(row=1, column=1).value == "Property"
        assert ws.cell(row=1, column=2).value == "Value"

        # Row 2 = pipeline name
        assert ws.cell(row=2, column=1).value == "Pipeline Name"
        assert ws.cell(row=2, column=2).value == "Customer Analytics ETL"

        wb.close()

    def test_source_tables_row_count(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Source Tables"]

        # Count non-header rows (each column of each source table = 1 row)
        total_cols = sum(len(s.columns) for s in sample_lineage.sources)
        # +1 for header row
        assert ws.max_row == total_cols + 1

        wb.close()

    def test_target_tables_row_count(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Target Tables"]

        total_cols = sum(len(t.columns) for t in sample_lineage.targets)
        assert ws.max_row == total_cols + 1

        wb.close()

    def test_column_lineage_row_count(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Column Lineage"]

        assert ws.max_row == len(sample_lineage.column_lineage) + 1

        wb.close()

    def test_column_lineage_headers(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Column Lineage"]

        headers = [ws.cell(row=1, column=c).value for c in range(1, 8)]
        assert headers == [
            "Target Table",
            "Target Column",
            "Source Column(s)",
            "Transformation",
            "Type",
            "Intermediate Steps",
            "Notes",
        ]

        wb.close()

    def test_components_row_count(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Components"]

        assert ws.max_row == len(sample_lineage.components) + 1

        wb.close()

    def test_data_flow_row_count(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Data Flow"]

        assert ws.max_row == len(sample_lineage.data_flow_edges) + 1

        wb.close()

    def test_data_flow_headers(self, sample_lineage, tmp_output):
        path = lineage_to_excel(sample_lineage, tmp_output / "test.xlsx")
        wb = load_workbook(path)
        ws = wb["Data Flow"]

        headers = [ws.cell(row=1, column=c).value for c in range(1, 5)]
        assert headers == ["From", "To", "Columns", "Label"]

        wb.close()


# ═══════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════


class TestExcelExportEdgeCases:
    def test_empty_components_and_edges(self, tmp_output):
        """Pipeline with no components or data_flow_edges."""
        lineage = PipelineLineage(
            pipeline_name="Minimal",
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
        path = lineage_to_excel(lineage, tmp_output / "minimal.xlsx")
        wb = load_workbook(path)
        assert "Components" in wb.sheetnames
        # Header only for components
        assert wb["Components"].max_row == 1
        assert wb["Data Flow"].max_row == 1
        wb.close()

    def test_tables_without_columns(self, tmp_output):
        """Tables with no column details still produce rows."""
        lineage = PipelineLineage(
            pipeline_name="No Cols",
            pipeline_type="sql_query",
            sources=[TableInfo(table_name="src")],
            targets=[TableInfo(table_name="tgt")],
            column_lineage=[],
        )
        path = lineage_to_excel(lineage, tmp_output / "nocols.xlsx")
        wb = load_workbook(path)
        # Each table without columns → 1 data row (empty columns)
        assert wb["Source Tables"].max_row == 2  # header + 1
        assert wb["Target Tables"].max_row == 2
        wb.close()

    def test_creates_parent_directories(self, tmp_path):
        """Output path with nonexistent parent directories."""
        deep_path = tmp_path / "a" / "b" / "c" / "test.xlsx"
        lineage = PipelineLineage(
            pipeline_name="Deep",
            pipeline_type="sql_query",
            sources=[TableInfo(table_name="s")],
            targets=[TableInfo(table_name="t")],
            column_lineage=[],
        )
        path = lineage_to_excel(lineage, deep_path)
        assert path.exists()

    def test_header_styling_applied(self, sample_lineage, tmp_output):
        """Verify headers have bold white font on blue background."""
        path = lineage_to_excel(sample_lineage, tmp_output / "styled.xlsx")
        wb = load_workbook(path)
        cell = wb["Overview"].cell(row=1, column=1)
        assert cell.font.bold is True
        assert cell.fill.start_color.rgb == "FF2F5496" or cell.fill.start_color.rgb == "002F5496"
        wb.close()
