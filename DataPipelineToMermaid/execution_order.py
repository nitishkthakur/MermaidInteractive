"""Deduce the execution order of a set of pipeline files using an LLM.

Works for any mix of SQL, Ab-Initio (.mp), Informatica (.xml), and
Pandas/Python (.py) files.  Uses a two-phase LLM approach:

  Phase 1 — extract:  For each file, extract which tables/datasets it
                       READS and which it WRITES.  One LLM call per file
                       (cheap model, short prompt).

  Phase 2 — order:    Send the full read/write map to the LLM and ask it
                       to produce a topological sort as JSON.  One call.

Output
------
ExecutionOrderResult — dataclass with:
  • ``stages``  : list of stages, each a list of file paths that can run
                  in parallel within that stage
  • ``edges``   : list of {from, to, via} dicts (file → file, via table)
  • ``node_map``: {filename → {reads, writes}} for debugging
  • ``mermaid`` : ready-to-render Mermaid flowchart string

The ``stages`` / ``edges`` structure maps directly onto a Mermaid graph.

Compatible with both the LangGraph deep_agent workflow and the GitHub
Copilot ReAct agent — see Guide.md for usage instructions.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


# ── Types ────────────────────────────────────────────────────────────


@dataclass
class FileIOSummary:
    """What a single file reads and writes."""

    filepath: str
    reads: list[str] = field(default_factory=list)   # table/dataset names
    writes: list[str] = field(default_factory=list)  # table/dataset names
    notes: str = ""


@dataclass
class ExecutionOrderResult:
    """Full result returned by ``deduce_execution_order``."""

    stages: list[list[str]]          # [[file_a, file_b], [file_c], ...]
    edges: list[dict[str, str]]      # [{from, to, via}, ...]
    node_map: dict[str, FileIOSummary]  # filepath → FileIOSummary
    mermaid: str                     # ready-to-render Mermaid diagram
    warnings: list[str] = field(default_factory=list)


# ── LLM helpers ─────────────────────────────────────────────────────


def _load_env() -> None:
    pkg_dir = Path(__file__).resolve().parent
    for candidate in [pkg_dir / ".env", Path.cwd() / ".env"]:
        if candidate.exists():
            load_dotenv(candidate)
            return
    load_dotenv()


def _get_model() -> Any:
    _load_env()
    if not os.getenv("OPENROUTER_API_KEY", "").strip():
        raise EnvironmentError(
            "OPENROUTER_API_KEY is not set. Copy .env.example → .env and fill it in."
        )
    from langchain_openrouter import ChatOpenRouter
    return ChatOpenRouter(
        model=os.getenv("LLM_MODEL", "anthropic/claude-haiku-4.5").strip(),
        temperature=0,
        max_tokens=int(os.getenv("LLM_MAX_TOKENS", "4096")),
    )


def _call_llm(model: Any, prompt: str) -> str:
    """Invoke the model and return the text content."""
    resp = model.invoke(prompt)
    content = resp.content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and "text" in block:
                parts.append(block["text"])
            elif hasattr(block, "text"):
                parts.append(block.text)
        content = "\n".join(parts)
    return str(content).strip()


def _extract_json(text: str) -> Any:
    """Pull the first JSON object or array from an LLM response."""
    # markdown fence
    for fence in ("```json", "```"):
        if fence in text:
            start = text.index(fence) + len(fence)
            if fence == "```json":
                pass
            else:
                # skip optional language tag
                nl = text.find("\n", start)
                if nl != -1:
                    start = nl
            end = text.index("```", start)
            return json.loads(text[start:end].strip())
    # bare JSON object
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        return json.loads(m.group())
    # bare JSON array
    m = re.search(r"\[[\s\S]*\]", text)
    if m:
        return json.loads(m.group())
    raise ValueError(f"No JSON found in LLM response: {text[:300]!r}")


# ── Phase 1: per-file read/write extraction ──────────────────────────

_PHASE1_PROMPT = """\
You are a data-pipeline analyst. Analyze the following {file_type} source code \
and identify every table, view, file, or dataset that this code READS FROM and \
every table, view, file, or dataset that this code WRITES TO / PRODUCES.

Rules:
- Use the exact table/dataset names as they appear in the code (schema.table or just table).
- Include temp tables, CTEs that are referenced by downstream steps, staging tables, \
  file outputs, and any named intermediate results that another process could consume.
- Do NOT include CTEs that are only used internally within this single file.
- If the file both reads and writes the same table (update/merge), list it in both.
- Ignore system tables, dual, information_schema.

Source file: {filepath}

```{file_ext}
{source_code}
```

Respond with ONLY valid JSON in this exact format (no commentary):
{{
  "reads":  ["table_or_dataset_name", ...],
  "writes": ["table_or_dataset_name", ...],
  "notes":  "one sentence describing what this file does"
}}
"""


def _extract_file_io(
    filepath: str, model: Any, verbose: bool = False
) -> FileIOSummary:
    """Ask the LLM what a single file reads and writes."""
    p = Path(filepath)
    source_code = p.read_text(encoding="utf-8", errors="replace")
    ext = p.suffix.lstrip(".").lower() or "text"
    file_type_map = {
        "sql": "SQL",
        "mp": "Ab-Initio (.mp graph)",
        "xml": "Informatica PowerCenter mapping",
        "py": "Python/Pandas ETL",
    }
    file_type = file_type_map.get(ext, ext.upper())

    prompt = _PHASE1_PROMPT.format(
        file_type=file_type,
        filepath=p.name,
        file_ext=ext,
        source_code=source_code[:12000],  # cap to avoid token overflow
    )

    if verbose:
        print(f"  [phase1] extracting I/O from {p.name} …", file=sys.stderr)

    raw = _call_llm(model, prompt)
    try:
        data = _extract_json(raw)
        return FileIOSummary(
            filepath=filepath,
            reads=[str(r).strip() for r in data.get("reads", [])],
            writes=[str(w).strip() for w in data.get("writes", [])],
            notes=str(data.get("notes", "")),
        )
    except Exception as exc:
        if verbose:
            print(f"    WARNING: could not parse I/O for {p.name}: {exc}", file=sys.stderr)
        return FileIOSummary(filepath=filepath, notes=f"parse error: {exc}")


# ── Phase 2: topological ordering ────────────────────────────────────

_PHASE2_PROMPT = """\
You are a data-pipeline architect. Below is a JSON map describing what each \
file in a pipeline READS and WRITES.

Your task:
1. Build a dependency graph: file A must run BEFORE file B if A writes a \
   table/dataset that B reads.
2. Perform a topological sort of this graph.
3. Group files that have NO dependency on each other into the same stage \
   (they can run in parallel).
4. List the dependency edges (file → file, via shared table/dataset).
5. Flag any CYCLES or AMBIGUITIES as warnings.

Key rules:
- A file with no dependencies goes in stage 0.
- Within a stage, order does not matter (parallel).
- A file may depend on multiple files in earlier stages.
- If a table name from "reads" does not match any "writes" entry, that table \
  is an external source — do not create an edge for it.
- Match table names case-insensitively and ignore schema prefixes when \
  the bare table name matches (e.g. "dbo.orders" matches "orders").

File I/O map:
{io_map_json}

Respond with ONLY valid JSON in this exact format (no commentary):
{{
  "stages": [
    {{"stage": 0, "files": ["file_a.sql", "file_b.mp"]}},
    {{"stage": 1, "files": ["file_c.sql"]}},
    ...
  ],
  "edges": [
    {{"from": "file_a.sql", "to": "file_c.sql", "via": "staging_orders"}},
    ...
  ],
  "warnings": ["optional warning strings if cycles or ambiguities detected"]
}}
"""


def _deduce_order(
    node_map: dict[str, FileIOSummary], model: Any, verbose: bool = False
) -> tuple[list[list[str]], list[dict[str, str]], list[str]]:
    """Phase 2: ask the LLM to topologically sort the dependency graph."""
    io_map = {
        Path(fp).name: {"reads": s.reads, "writes": s.writes, "notes": s.notes}
        for fp, s in node_map.items()
    }
    prompt = _PHASE2_PROMPT.format(io_map_json=json.dumps(io_map, indent=2))

    if verbose:
        print("  [phase2] deducing execution order …", file=sys.stderr)

    raw = _call_llm(model, prompt)
    data = _extract_json(raw)

    stages_raw = data.get("stages", [])
    # Normalize: map short filenames back to full filepaths
    name_to_fp = {Path(fp).name: fp for fp in node_map}

    stages: list[list[str]] = []
    for stage_entry in sorted(stages_raw, key=lambda s: s.get("stage", 0)):
        group = []
        for fname in stage_entry.get("files", []):
            group.append(name_to_fp.get(fname, fname))
        if group:
            stages.append(group)

    edges_raw = data.get("edges", [])
    edges: list[dict[str, str]] = []
    for e in edges_raw:
        frm = name_to_fp.get(e.get("from", ""), e.get("from", ""))
        to = name_to_fp.get(e.get("to", ""), e.get("to", ""))
        edges.append({"from": frm, "to": to, "via": e.get("via", "")})

    warnings = [str(w) for w in data.get("warnings", [])]
    return stages, edges, warnings


# ── Mermaid renderer ─────────────────────────────────────────────────


def _render_mermaid(
    stages: list[list[str]],
    edges: list[dict[str, str]],
    node_map: dict[str, FileIOSummary],
) -> str:
    """Build a Mermaid flowchart from stages and edges."""

    def _mid(filepath: str) -> str:
        """Mermaid node ID: alphanumeric only."""
        name = Path(filepath).name
        safe = re.sub(r"[^a-zA-Z0-9]", "_", name)
        return safe if safe[0].isalpha() else "f_" + safe

    def _mlabel(filepath: str) -> str:
        p = Path(filepath)
        ext_icons = {".sql": "🗄", ".mp": "⚙", ".xml": "📋", ".py": "🐍"}
        icon = ext_icons.get(p.suffix.lower(), "📄")
        return f"{icon} {p.name}"

    lines = ["flowchart LR"]

    for i, stage in enumerate(stages):
        if len(stage) == 1:
            fp = stage[0]
            mid = _mid(fp)
            label = _mlabel(fp)
            summary = node_map.get(fp)
            writes = (
                ", ".join(summary.writes[:3])
                if summary and summary.writes
                else ""
            )
            if writes:
                lines.append(f'    {mid}["{label}<br/><i>→ {writes}</i>"]')
            else:
                lines.append(f'    {mid}["{label}"]')
        else:
            sg_id = f"stage_{i}"
            lines.append(f'    subgraph {sg_id}["Stage {i} — parallel"]')
            for fp in stage:
                mid = _mid(fp)
                label = _mlabel(fp)
                lines.append(f'        {mid}["{label}"]')
            lines.append("    end")

    lines.append("")
    seen_edges: set[tuple[str, str]] = set()
    for edge in edges:
        frm_id = _mid(edge["from"])
        to_id = _mid(edge["to"])
        key = (frm_id, to_id)
        if key in seen_edges:
            continue
        seen_edges.add(key)
        via = edge.get("via", "")
        if via:
            lines.append(f'    {frm_id} -->|"{via}"| {to_id}')
        else:
            lines.append(f"    {frm_id} --> {to_id}")

    lines.append("")
    # Style by file type
    ext_styles = {
        ".sql": "fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px",
        ".mp":  "fill:#fff3e0,stroke:#e65100,stroke-width:2px",
        ".xml": "fill:#e3f2fd,stroke:#1565c0,stroke-width:2px",
        ".py":  "fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px",
    }
    classified: dict[str, list[str]] = {}
    for fp in node_map:
        ext = Path(fp).suffix.lower()
        style = ext_styles.get(ext, "fill:#fafafa,stroke:#555,stroke-width:1px")
        classified.setdefault(style, []).append(_mid(fp))

    for style, mids in classified.items():
        cls_name = re.sub(r"[^a-z]", "", style[:10])
        lines.append(f"    classDef {cls_name} {style}")
        lines.append(f"    class {','.join(mids)} {cls_name}")

    return "\n".join(lines)


# ── Public API ───────────────────────────────────────────────────────


def deduce_execution_order(
    filepaths: list[str],
    *,
    model: Any | None = None,
    verbose: bool = False,
) -> ExecutionOrderResult:
    """Deduce the execution order of a mixed set of pipeline files.

    Parameters
    ----------
    filepaths:
        Absolute or relative paths to the pipeline files.
    model:
        Optional pre-built LangChain chat model.  Built automatically if None.
    verbose:
        Print progress to stderr.

    Returns
    -------
    ExecutionOrderResult
        Contains ``stages``, ``edges``, ``node_map``, ``mermaid``, ``warnings``.
    """
    if model is None:
        model = _get_model()

    # Phase 1: extract I/O for each file in parallel (sequential here for simplicity)
    node_map: dict[str, FileIOSummary] = {}
    for fp in filepaths:
        node_map[fp] = _extract_file_io(fp, model, verbose=verbose)

    # Phase 2: topological sort
    stages, edges, warnings = _deduce_order(node_map, model, verbose=verbose)

    # Fallback: if LLM returned no stages, put all files in stage 0
    if not stages:
        stages = [list(filepaths)]
        warnings.append("Could not determine order — all files placed in stage 0.")

    mermaid = _render_mermaid(stages, edges, node_map)

    return ExecutionOrderResult(
        stages=stages,
        edges=edges,
        node_map=node_map,
        mermaid=mermaid,
        warnings=warnings,
    )


def write_mermaid_html(mermaid_text: str, output_path: str | Path, title: str = "Pipeline Execution Order") -> Path:
    """Write a standalone HTML file rendering the Mermaid execution-order diagram."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped = mermaid_text.replace("&", "&amp;").replace("<", "&lt;")
    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>{title}</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           margin: 0; padding: 2rem; background: #fafafa; }}
    h1 {{ color: #2F5496; }}
    .mermaid {{ background: #fff; border: 1px solid #ddd; border-radius: 8px;
               padding: 2rem; overflow-x: auto; margin-top: 1rem; }}
    .legend {{ display: flex; gap: 1.5rem; margin: 1rem 0; flex-wrap: wrap; }}
    .legend-item {{ display: flex; align-items: center; gap: .4rem; font-size: .9rem; }}
    .swatch {{ width: 16px; height: 16px; border-radius: 3px; border: 1px solid #888; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div class="legend">
    <div class="legend-item"><div class="swatch" style="background:#e8f5e9"></div> SQL</div>
    <div class="legend-item"><div class="swatch" style="background:#fff3e0"></div> Ab-Initio (.mp)</div>
    <div class="legend-item"><div class="swatch" style="background:#e3f2fd"></div> Informatica (.xml)</div>
    <div class="legend-item"><div class="swatch" style="background:#f3e5f5"></div> Python/Pandas (.py)</div>
  </div>
  <div class="mermaid">
{escaped}
  </div>
  <script>mermaid.initialize({{ startOnLoad: true, theme: 'default',
    flowchart: {{ curve: 'basis', useMaxWidth: false }} }});</script>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")
    return output_path


# ── CLI convenience ──────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Deduce execution order of pipeline files."
    )
    parser.add_argument("files", nargs="+", help="Pipeline files to analyze")
    parser.add_argument("-o", "--output-dir", default=".", help="Output directory")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    result = deduce_execution_order(args.files, verbose=args.verbose)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "execution_order.json"
    payload = {
        "stages": [
            {"stage": i, "files": [Path(f).name for f in stage]}
            for i, stage in enumerate(result.stages)
        ],
        "edges": [
            {"from": Path(e["from"]).name, "to": Path(e["to"]).name, "via": e["via"]}
            for e in result.edges
        ],
        "node_io": {
            Path(fp).name: {"reads": s.reads, "writes": s.writes, "notes": s.notes}
            for fp, s in result.node_map.items()
        },
        "warnings": result.warnings,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  JSON: {json_path}")

    mmd_path = out_dir / "execution_order.mmd"
    mmd_path.write_text(result.mermaid, encoding="utf-8")
    print(f"  Mermaid: {mmd_path}")

    html_path = write_mermaid_html(result.mermaid, out_dir / "execution_order.html")
    print(f"  HTML: {html_path}")

    if result.warnings:
        print("\nWarnings:")
        for w in result.warnings:
            print(f"  ⚠ {w}")

    print("\nExecution order:")
    for i, stage in enumerate(result.stages):
        names = [Path(f).name for f in stage]
        print(f"  Stage {i}: {' | '.join(names)}")
