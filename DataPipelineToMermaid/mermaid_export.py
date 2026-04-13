"""Convert a PipelineLineage JSON into a Mermaid flowchart and interactive HTML.

Two detail levels are supported:

* **table** — one node per table / component, edges labelled with column counts
* **column** — one node per table.column, full column-level graph

The interactive HTML reuses the parent project's ``generate_interactive_html``
for click-to-highlight behaviour.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from .models import PipelineLineage

# ── Helpers ─────────────────────────────────────────────────────────


def _safe_id(name: str) -> str:
    """Make a string safe for use as a Mermaid node ID."""
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Ensure it starts with a letter
    if safe and not safe[0].isalpha():
        safe = "n_" + safe
    return safe


def _esc(text: str, max_len: int = 50) -> str:
    """Escape and truncate text for a Mermaid label."""
    text = text.replace('"', "'").replace("\n", " ").replace("\\", "/")
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


# ── Table-level Mermaid ─────────────────────────────────────────────


def _table_level_mermaid(lineage: PipelineLineage) -> str:
    """Generate a flowchart LR at the table / component level."""
    lines: list[str] = ["flowchart LR"]

    # --- Sources subgraph ---
    if lineage.sources:
        lines.append('    subgraph Sources["📥 Source Tables"]')
        for tbl in lineage.sources:
            tid = _safe_id(tbl.full_name)
            col_preview = ", ".join(c.name for c in tbl.columns[:6])
            if len(tbl.columns) > 6:
                col_preview += f", …+{len(tbl.columns) - 6}"
            label = f"{tbl.full_name}"
            if col_preview:
                label += f"<br/><i>{_esc(col_preview, 60)}</i>"
            lines.append(f'        {tid}["{_esc(label, 80)}"]')
        lines.append("    end")

    # --- Components subgraph ---
    if lineage.components:
        lines.append('    subgraph Transforms["⚙️ Transformations"]')
        for comp in lineage.components:
            cid = _safe_id(comp.name)
            label = f"{comp.name}<br/><i>{comp.component_type}</i>"
            # Use diamond for CTE / proc, trapezoid for others
            if comp.component_type in ("CTE", "stored_procedure", "subquery"):
                lines.append(f'        {cid}{{"{_esc(label, 60)}"}}')
            else:
                lines.append(f'        {cid}[/"{_esc(label, 60)}"\\]')
        lines.append("    end")

    # --- Targets subgraph ---
    if lineage.targets:
        lines.append('    subgraph Targets["📤 Target Tables"]')
        for tbl in lineage.targets:
            tid = _safe_id(tbl.full_name)
            col_preview = ", ".join(c.name for c in tbl.columns[:6])
            if len(tbl.columns) > 6:
                col_preview += f", …+{len(tbl.columns) - 6}"
            label = f"{tbl.full_name}"
            if col_preview:
                label += f"<br/><i>{_esc(col_preview, 60)}</i>"
            lines.append(f'        {tid}["{_esc(label, 80)}"]')
        lines.append("    end")

    # --- Edges ---
    seen: set[tuple[str, str]] = set()

    if lineage.data_flow_edges:
        for edge in lineage.data_flow_edges:
            fid = _safe_id(edge.from_node)
            tid = _safe_id(edge.to_node)
            key = (fid, tid)
            if key in seen:
                continue
            seen.add(key)
            ncols = len(edge.columns)
            label = edge.edge_label or (f"{ncols} cols" if ncols else "")
            if label:
                lines.append(f'    {fid} -->|"{_esc(label)}"| {tid}')
            else:
                lines.append(f"    {fid} --> {tid}")
    else:
        # Infer edges from column_lineage
        edge_map: dict[tuple[str, str], set[str]] = {}
        for cl in lineage.column_lineage:
            tgt_table = cl.target_table
            for src_col in cl.source_columns:
                parts = src_col.rsplit(".", 1)
                src_table = parts[0] if len(parts) > 1 else src_col
                col_name = parts[-1] if len(parts) > 1 else src_col
                edge_map.setdefault((src_table, tgt_table), set()).add(
                    col_name
                )

        for (src, tgt), cols in sorted(edge_map.items()):
            fid = _safe_id(src)
            tid = _safe_id(tgt)
            key = (fid, tid)
            if key not in seen:
                seen.add(key)
                lines.append(f'    {fid} -->|"{len(cols)} cols"| {tid}')

    # --- Styles ---
    lines.append("")
    lines.append(
        "    classDef source fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px"
    )
    lines.append(
        "    classDef target fill:#e3f2fd,stroke:#1565c0,stroke-width:2px"
    )
    lines.append(
        "    classDef transform fill:#fff3e0,stroke:#e65100,stroke-width:2px"
    )

    for tbl in lineage.sources:
        lines.append(f"    class {_safe_id(tbl.full_name)} source")
    for tbl in lineage.targets:
        lines.append(f"    class {_safe_id(tbl.full_name)} target")
    for comp in lineage.components:
        lines.append(f"    class {_safe_id(comp.name)} transform")

    return "\n".join(lines)


# ── Column-level Mermaid ────────────────────────────────────────────


def _column_level_mermaid(lineage: PipelineLineage) -> str:
    """Generate a detailed column-level flowchart."""
    lines: list[str] = ["flowchart LR"]

    # One subgraph per source table
    for tbl in lineage.sources:
        sg_id = _safe_id(tbl.full_name) + "_sg"
        lines.append(f'    subgraph {sg_id}["{tbl.full_name}"]')
        for col in tbl.columns:
            nid = _safe_id(f"{tbl.full_name}.{col.name}")
            lines.append(f'        {nid}["{col.name}"]')
        lines.append("    end")

    # One subgraph per target table
    for tbl in lineage.targets:
        sg_id = _safe_id(tbl.full_name) + "_sg"
        lines.append(f'    subgraph {sg_id}["{tbl.full_name}"]')
        for col in tbl.columns:
            nid = _safe_id(f"{tbl.full_name}.{col.name}")
            lines.append(f'        {nid}["{col.name}"]')
        lines.append("    end")

    # Edges from column lineage
    for cl in lineage.column_lineage:
        tgt_nid = _safe_id(f"{cl.target_table}.{cl.target_column}")
        for src_col in cl.source_columns:
            src_nid = _safe_id(src_col)
            label = _esc(cl.transformation_type, 25)
            lines.append(f'    {src_nid} -->|"{label}"| {tgt_nid}')

    # Styles
    lines.append("")
    lines.append(
        "    classDef source fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px"
    )
    lines.append(
        "    classDef target fill:#e3f2fd,stroke:#1565c0,stroke-width:2px"
    )
    for tbl in lineage.sources:
        for col in tbl.columns:
            nid = _safe_id(f"{tbl.full_name}.{col.name}")
            lines.append(f"    class {nid} source")
    for tbl in lineage.targets:
        for col in tbl.columns:
            nid = _safe_id(f"{tbl.full_name}.{col.name}")
            lines.append(f"    class {nid} target")

    return "\n".join(lines)


# ── Public API ──────────────────────────────────────────────────────


def lineage_to_mermaid(
    lineage: PipelineLineage,
    detail_level: str = "table",
) -> str:
    """Return a Mermaid flowchart string.

    Parameters
    ----------
    lineage : PipelineLineage
    detail_level : str
        ``"table"`` for table-level overview (default), or
        ``"column"`` for full column-level graph.
    """
    if detail_level == "column":
        return _column_level_mermaid(lineage)
    return _table_level_mermaid(lineage)


def lineage_to_mermaid_file(
    lineage: PipelineLineage,
    output_path: str | Path,
    detail_level: str = "table",
) -> Path:
    """Write the Mermaid text to a ``.mmd`` file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mmd = lineage_to_mermaid(lineage, detail_level=detail_level)
    output_path.write_text(mmd, encoding="utf-8")
    return output_path


def lineage_to_html(
    lineage: PipelineLineage,
    output_path: str | Path,
    detail_level: str = "table",
) -> Path:
    """Generate an interactive HTML file using the parent project's renderer.

    Falls back to a standalone HTML with Mermaid.js CDN if the parent
    module ``mermaid_interactive`` is not available.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    mmd_text = lineage_to_mermaid(lineage, detail_level=detail_level)

    # Try the parent project's interactive generator
    try:
        parent_dir = str(Path(__file__).resolve().parent.parent)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        from mermaid_interactive import generate_interactive_html

        html = generate_interactive_html(
            mmd_text, title=lineage.pipeline_name
        )
    except (ImportError, TypeError):
        # Standalone fallback
        html = _standalone_html(mmd_text, lineage.pipeline_name)

    output_path.write_text(html, encoding="utf-8")
    return output_path


# ── Standalone HTML fallback ────────────────────────────────────────


def _standalone_html(mermaid_text: str, title: str) -> str:
    """Minimal self-contained HTML with Mermaid.js from CDN."""
    escaped = mermaid_text.replace("&", "&amp;").replace("<", "&lt;")
    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>{title} — Data Lineage</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      margin: 0; padding: 2rem; background: #fafafa;
    }}
    h1 {{ color: #2F5496; margin-bottom: .5rem; }}
    .subtitle {{ color: #666; margin-bottom: 2rem; }}
    .mermaid {{ background: #fff; border: 1px solid #ddd; border-radius: 8px;
               padding: 2rem; overflow-x: auto; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <p class="subtitle">Data Lineage Diagram</p>
  <div class="mermaid">
{escaped}
  </div>
  <script>mermaid.initialize({{ startOnLoad: true, theme: 'default',
    flowchart: {{ curve: 'basis', useMaxWidth: false }} }});</script>
</body>
</html>
"""
