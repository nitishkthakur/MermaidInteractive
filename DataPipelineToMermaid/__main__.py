"""Allow ``python -m DataPipelineToMermaid`` with two modes.

Default mode (no args): runs the CLI defined in main.py.

Cross-file tracer mode: edit the USER CONFIGURATION block below, then run:
    python -m DataPipelineToMermaid trace
"""

import sys
from pathlib import Path as _Path

# Resolve paths relative to this package directory so the command works
# regardless of which directory you run it from.
_PKG_DIR = _Path(__file__).resolve().parent

# ══════════════════════════════════════════════════════════════════════
# USER CONFIGURATION — edit this block for cross-file column tracing
# ══════════════════════════════════════════════════════════════════════

# Fully-qualified target columns to trace: "table_name.column_name"
TARGET_COLUMNS: list[str] = [
    "dbo.fact_pipeline_metrics.net_revenue",
    "dbo.fact_pipeline_metrics.avg_risk_score",
    "dbo.customer_risk_scores.risk_score",
]

# All pipeline files that form the workflow (any mix of .sql, .mp, .xml, .py)
# Paths are relative to the DataPipelineToMermaid/ package directory.
SOURCE_FILES: list[str] = [
    str(_PKG_DIR / "tests/test_cross_file/fixtures/01_raw_orders.sql"),
    str(_PKG_DIR / "tests/test_cross_file/fixtures/02_raw_customers.sql"),
    str(_PKG_DIR / "tests/test_cross_file/fixtures/03_raw_payments.sql"),
    str(_PKG_DIR / "tests/test_cross_file/fixtures/04_enrich_orders.mp"),
    str(_PKG_DIR / "tests/test_cross_file/fixtures/05_reconcile_payments.sql"),
    str(_PKG_DIR / "tests/test_cross_file/fixtures/06_customer_risk.mp"),
    str(_PKG_DIR / "tests/test_cross_file/fixtures/07_final_metrics.sql"),
]

# Output directory for trace results (resolved relative to this package directory)
OUTPUT_DIR: str = str(_PKG_DIR / "output" / "cross_file_trace")

# ══════════════════════════════════════════════════════════════════════


def _run_trace() -> None:
    """Run the cross-file column tracer and execution order deducer."""
    import json
    from pathlib import Path

    from .config import get_artifact_mode
    from .cross_file_tracer import trace_columns, write_trace_json
    from .execution_order import deduce_execution_order, write_mermaid_html
    from .excel_export import lineage_to_excel
    from .models import (
        DataFlowEdge, PipelineLineage, TableInfo, ColumnInfo
    )

    mode = get_artifact_mode()

    # Wide mode gets its own output directory; regular/long use OUTPUT_DIR.
    if mode == "wide":
        out_dir = _Path(__file__).resolve().parent / "output" / "wide_trace"
    else:
        out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("━" * 60)
    print("  DataPipelineToMermaid — Cross-File Column Tracer")
    print("━" * 60)
    print(f"  Target columns : {len(TARGET_COLUMNS)}")
    print(f"  Source files   : {len(SOURCE_FILES)}")
    print(f"  Output dir     : {out_dir}")
    print()

    # ── Step 1: Execution order ──────────────────────────────────────
    print("─── Step 1/3: Deducing execution order ───")
    order_result = deduce_execution_order(SOURCE_FILES, verbose=True)

    order_json = out_dir / "execution_order.json"
    payload = {
        "stages": [
            {"stage": i, "files": [Path(f).name for f in stage]}
            for i, stage in enumerate(order_result.stages)
        ],
        "edges": [
            {
                "from": Path(e["from"]).name,
                "to": Path(e["to"]).name,
                "via": e["via"],
            }
            for e in order_result.edges
        ],
        "node_io": {
            Path(fp).name: {
                "reads": s.reads,
                "writes": s.writes,
                "notes": s.notes,
            }
            for fp, s in order_result.node_map.items()
        },
        "warnings": order_result.warnings,
    }
    order_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  📄 Order JSON    : {order_json}")

    order_html = write_mermaid_html(
        order_result.mermaid,
        out_dir / "execution_order.html",
        title="Pipeline Execution Order",
    )
    print(f"  🌐 Order diagram : {order_html}")

    if order_result.warnings:
        for w in order_result.warnings:
            print(f"  ⚠  {w}")
    print()

    # ── Step 2: Trace columns ────────────────────────────────────────
    print("─── Step 2/3: Tracing column lineage across files ───")
    lineage_rows = trace_columns(TARGET_COLUMNS, SOURCE_FILES, verbose=True)

    trace_json = write_trace_json(lineage_rows, out_dir / "column_trace.json")
    print(f"\n  📄 Trace JSON    : {trace_json}")
    print()

    # ── Step 3: Excel export ─────────────────────────────────────────
    print("─── Step 3/3: Writing Excel workbook ───")

    # Wrap in PipelineLineage for the shared Excel exporter
    all_sources: set[str] = set()
    all_targets: set[str] = set()
    for cl in lineage_rows:
        if cl.target_table:
            all_targets.add(cl.target_table)
        for ref in cl.source_refs:
            if ref.source_table:
                all_sources.add(ref.source_table)

    pseudo_lineage = PipelineLineage(
        pipeline_name="Cross-File Column Trace",
        pipeline_type="mixed",
        source_file=", ".join(Path(f).name for f in SOURCE_FILES),
        description=(
            f"Cross-file lineage trace for {len(TARGET_COLUMNS)} target columns "
            f"across {len(SOURCE_FILES)} pipeline files."
        ),
        sources=[TableInfo(table_name=t) for t in sorted(all_sources)],
        targets=[TableInfo(table_name=t) for t in sorted(all_targets)],
        components=[],
        column_lineage=lineage_rows,
        data_flow_edges=[
            DataFlowEdge(
                from_node=Path(e["from"]).name,
                to_node=Path(e["to"]).name,
                edge_label=e.get("via", ""),
            )
            for e in order_result.edges
        ],
    )

    xlsx = lineage_to_excel(pseudo_lineage, out_dir / "column_trace.xlsx")
    print(f"  📊 Excel         : {xlsx}")
    print()
    print("✅ Done!")
    print()
    print("Summary:")
    for cl in lineage_rows:
        found = "✓" if cl.source_refs else "✗"
        n_src_files = len(cl.source_filenames)
        print(
            f"  {found} {cl.target_table}.{cl.target_column}"
            f"  ({n_src_files} source file{'s' if n_src_files != 1 else ''})"
            + (f"  ← via {cl.filename}" if cl.filename else "  [not found]")
        )


if __name__ == "__main__" or (len(sys.argv) > 1 and sys.argv[1] == "trace"):
    if len(sys.argv) > 1 and sys.argv[1] == "trace":
        _run_trace()
    else:
        from .main import main
        main()
