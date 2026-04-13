"""Convert a PipelineLineage JSON into a multi-sheet Excel workbook.

Sheets
------
1. Overview       — pipeline metadata & counts
2. Source Tables   — schema, table, column, type, description
3. Target Tables   — same layout as Source Tables
4. Column Lineage  — target ← source(s), transformation, type, steps
5. Components      — CTEs, procs, transforms
6. Data Flow       — edge list (from → to, columns)
"""

from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

from .models import PipelineLineage

# ── Style constants ─────────────────────────────────────────────────

_HEADER_FONT = Font(bold=True, color="FFFFFF", size=11, name="Calibri")
_HEADER_FILL = PatternFill(
    start_color="2F5496", end_color="2F5496", fill_type="solid"
)
_HEADER_ALIGN = Alignment(
    horizontal="center", vertical="center", wrap_text=True
)
_ALT_ROW_FILL = PatternFill(
    start_color="D6E4F0", end_color="D6E4F0", fill_type="solid"
)
_THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)
_WRAP = Alignment(vertical="top", wrap_text=True)


# ── Helpers ─────────────────────────────────────────────────────────


def _style_header(ws: Worksheet, ncols: int) -> None:
    """Apply header styling to row 1."""
    for c in range(1, ncols + 1):
        cell = ws.cell(row=1, column=c)
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
        cell.alignment = _HEADER_ALIGN
        cell.border = _THIN_BORDER


def _auto_width(ws: Worksheet, *, min_w: int = 14, max_w: int = 55) -> None:
    """Set column widths based on content length."""
    for col_cells in ws.columns:
        col_letter = get_column_letter(col_cells[0].column)
        length = max(
            (len(str(c.value)) if c.value else 0 for c in col_cells),
            default=0,
        )
        ws.column_dimensions[col_letter].width = min(
            max(length + 3, min_w), max_w
        )


def _stripe_rows(ws: Worksheet, ncols: int) -> None:
    """Apply alternating row colours and borders from row 2 onward."""
    for row_idx, row in enumerate(ws.iter_rows(min_row=2, max_col=ncols), 2):
        for cell in row:
            cell.border = _THIN_BORDER
            cell.alignment = _WRAP
            if row_idx % 2 == 0:
                cell.fill = _ALT_ROW_FILL


# ── Main export function ───────────────────────────────────────────


def lineage_to_excel(
    lineage: PipelineLineage,
    output_path: str | Path,
) -> Path:
    """Write *lineage* to a ``.xlsx`` workbook at *output_path*.

    Returns the resolved output path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()

    # ── Sheet 1: Overview ───────────────────────────────────────────

    ws = wb.active
    ws.title = "Overview"
    headers = ["Property", "Value"]
    ws.append(headers)
    _style_header(ws, len(headers))

    ws.append(["Pipeline Name", lineage.pipeline_name])
    ws.append(["Pipeline Type", lineage.pipeline_type])
    ws.append(["Source File", lineage.source_file])
    ws.append(["Description", lineage.description])
    ws.append([""])
    ws.append(["Source Tables", len(lineage.sources)])
    ws.append(["Target Tables", len(lineage.targets)])
    ws.append(["Components (CTEs / Procs / …)", len(lineage.components)])
    ws.append(["Column Lineage Mappings", len(lineage.column_lineage)])
    ws.append(["Data Flow Edges", len(lineage.data_flow_edges)])
    _stripe_rows(ws, len(headers))
    _auto_width(ws)

    # ── Sheet 2: Source Tables ──────────────────────────────────────

    ws_src = wb.create_sheet("Source Tables")
    src_hdr = ["Schema", "Table", "Column", "Data Type", "Description"]
    ws_src.append(src_hdr)
    _style_header(ws_src, len(src_hdr))

    for tbl in lineage.sources:
        if tbl.columns:
            for col in tbl.columns:
                ws_src.append([
                    tbl.schema_name,
                    tbl.table_name,
                    col.name,
                    col.data_type,
                    col.description,
                ])
        else:
            ws_src.append([tbl.schema_name, tbl.table_name, "", "", ""])
    _stripe_rows(ws_src, len(src_hdr))
    _auto_width(ws_src)

    # ── Sheet 3: Target Tables ──────────────────────────────────────

    ws_tgt = wb.create_sheet("Target Tables")
    tgt_hdr = ["Schema", "Table", "Column", "Data Type", "Description"]
    ws_tgt.append(tgt_hdr)
    _style_header(ws_tgt, len(tgt_hdr))

    for tbl in lineage.targets:
        if tbl.columns:
            for col in tbl.columns:
                ws_tgt.append([
                    tbl.schema_name,
                    tbl.table_name,
                    col.name,
                    col.data_type,
                    col.description,
                ])
        else:
            ws_tgt.append([tbl.schema_name, tbl.table_name, "", "", ""])
    _stripe_rows(ws_tgt, len(tgt_hdr))
    _auto_width(ws_tgt)

    # ── Sheet 4: Column Lineage ─────────────────────────────────────

    ws_lin = wb.create_sheet("Column Lineage")
    lin_hdr = [
        "Target Table",
        "Target Column",
        "Source Column(s)",
        "Transformation",
        "Type",
        "Intermediate Steps",
        "Notes",
    ]
    ws_lin.append(lin_hdr)
    _style_header(ws_lin, len(lin_hdr))

    for cl in lineage.column_lineage:
        steps_str = (
            " → ".join(
                f"[{s.component_name}] {s.expression} → {s.output_column}"
                for s in cl.intermediate_steps
            )
            if cl.intermediate_steps
            else ""
        )
        ws_lin.append([
            cl.target_table,
            cl.target_column,
            "\n".join(cl.source_columns),
            cl.transformation,
            cl.transformation_type,
            steps_str,
            cl.notes,
        ])
    _stripe_rows(ws_lin, len(lin_hdr))
    _auto_width(ws_lin)

    # ── Sheet 5: Components ─────────────────────────────────────────

    ws_comp = wb.create_sheet("Components")
    comp_hdr = [
        "Name",
        "Type",
        "Description",
        "Input Tables",
        "Output Columns",
        "SQL / Code Snippet",
    ]
    ws_comp.append(comp_hdr)
    _style_header(ws_comp, len(comp_hdr))

    for comp in lineage.components:
        ws_comp.append([
            comp.name,
            comp.component_type,
            comp.description,
            "\n".join(comp.input_tables),
            "\n".join(comp.output_columns),
            comp.sql_text[:200] if comp.sql_text else "",
        ])
    _stripe_rows(ws_comp, len(comp_hdr))
    _auto_width(ws_comp)

    # ── Sheet 6: Data Flow ──────────────────────────────────────────

    ws_flow = wb.create_sheet("Data Flow")
    flow_hdr = ["From", "To", "Columns", "Label"]
    ws_flow.append(flow_hdr)
    _style_header(ws_flow, len(flow_hdr))

    for edge in lineage.data_flow_edges:
        ws_flow.append([
            edge.from_node,
            edge.to_node,
            ", ".join(edge.columns),
            edge.edge_label,
        ])
    _stripe_rows(ws_flow, len(flow_hdr))
    _auto_width(ws_flow)

    # ── Save ────────────────────────────────────────────────────────

    wb.save(str(output_path))
    return output_path
