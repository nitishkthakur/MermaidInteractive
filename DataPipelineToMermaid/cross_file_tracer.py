"""Cross-file column-level lineage tracer.

Given a list of TARGET COLUMNS (as ``table.column``) and a list of SOURCE
FILES (any mix of .sql / .mp / .xml / .py), this module traces each column
upstream across ALL files, following the data wherever it flows — through
any number of files and any graph topology — until it reaches raw source
tables that are not produced by any file in the list.

Two-phase approach
------------------
Phase 1 — Catalogue
    Send all file contents to the LLM in one call.  Ask it to build a
    comprehensive catalogue: for every table/dataset column produced by any
    file, record which file produces it and what the transformation is.
    Output: ``{table.column → {file, transformation, source_refs, type}}``.

Phase 2 — Trace
    For each target column, perform a DAG traversal of the catalogue:
      • Start at the target ``table.column``.
      • Look up its entry in the catalogue → record the transformation step
        and the source columns.
      • For each source column, check if it is itself in the catalogue
        (i.e., produced by another file in the list).
      • If yes → recurse upstream.  If no → it is a raw source; stop that branch.
      • Repeat until ALL branches terminate at raw sources.
    Collapse into a flat list of ``IntermediateStep`` objects ordered from
    raw-source inward to the final output, with ``filename`` on every step.
    If a target column is not found in the catalogue → return an empty
    ``ColumnLineage`` with an explanatory ``notes`` field.

Compatible with both the LangGraph deep_agent and the GitHub Copilot ReAct
agent — see Guide.md for full usage instructions.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .models import ColumnLineage, IntermediateStep, SourceColumnRef


# ── LLM helpers ──────────────────────────────────────────────────────


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
            "OPENROUTER_API_KEY not set. Copy .env.example → .env."
        )
    from langchain_openrouter import ChatOpenRouter
    return ChatOpenRouter(
        model=os.getenv("LLM_MODEL", "anthropic/claude-haiku-4.5").strip(),
        temperature=0,
        max_tokens=int(os.getenv("LLM_MAX_TOKENS", "16384")),
    )


def _call_llm(model: Any, prompt: str) -> str:
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
    for fence in ("```json", "```"):
        if fence in text:
            start = text.index(fence) + len(fence)
            nl = text.find("\n", start)
            if nl != -1:
                start = nl
            end = text.index("```", start)
            return json.loads(text[start:end].strip())
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        return json.loads(m.group())
    raise ValueError(f"No JSON found in response: {text[:300]!r}")


# ── Phase 1: build the cross-file catalogue ───────────────────────────

_CATALOGUE_PROMPT = r"""
You are a senior data-engineer performing cross-file data lineage analysis.

You have been given {n_files} pipeline files below. These files are part of
the SAME workflow — outputs of earlier files are consumed as inputs by later
files.  The files may be ANY combination of pipeline technologies: plain SQL
queries, SQL stored procedures, Ab-Initio .mp graphs, Informatica PowerCenter
XML mappings, Pandas/Python ETL scripts, dbt models, Spark jobs, shell scripts
that call BTEQ/SQLLDR, or anything else.

Your job: build a complete COLUMN-LEVEL CATALOGUE that maps every output
column produced by any file back to its transformation and source columns.

════════════════════════════════════════════════════════════════════
CATALOGUE RULES
════════════════════════════════════════════════════════════════════
1.  For EVERY column that any file writes to a named output table/dataset:
      • Record the OUTPUT key as "table.column" using the EXACT names from
        the code (preserve schema prefix if present, e.g. "dbo.stg_orders").
      • Record which FILE produces it (filename only, not path).
      • Record the TRANSFORMATION EXPRESSION — the actual formula or logic
        that computes it, written ending with "AS column_name".
        For a direct copy write: source_col AS column_name.
      • Record the TRANSFORMATION TYPE from this fixed vocabulary:
          direct_copy | aggregation | calculation | case_logic | join_key |
          window_function | type_cast | concatenation | lookup | conditional |
          constant | string_manipulation | date_manipulation | coalesce | other
      • Record EVERY SOURCE COLUMN that feeds this output as
        "source_table.source_column".
        — For CTEs or subqueries used only WITHIN the same file, trace
          through them transparently to the real input table.
        — If the source table is produced by ANOTHER file in this list,
          still record "that_table.source_column" as-is; the tracer will
          follow it upstream automatically.

2.  DO NOT skip any column.  Every column in every output table/dataset
    must appear — even simple pass-through columns.

3.  Identify output columns using these per-technology heuristics
    (adapt to any technology not listed here using common sense):

    SQL / Stored Procedures
      • Output = INSERT INTO <table> (...col list...) or SELECT INTO or
        MERGE INTO target or CREATE TABLE AS SELECT.
      • For stored procedures, treat the procedure's INSERT/MERGE targets
        as outputs; treat its SELECT FROM tables as inputs.
      • Trace through WITH (CTE) blocks, temp tables (#tmp / ##tmp),
        table variables (@tbl), and EXEC sub-calls transparently.

    Ab-Initio (.mp graphs)
      • Output = "output-table" component; trace through transform /
        join / rollup / reformat / scan / fuse components.

    Informatica PowerCenter (.xml)
      • Output = TARGET instance; trace through mapping transformations.

    Python / Pandas / PySpark
      • Output = .to_sql(), .to_csv(), .write.save(), DataFrame returned
        from a function, or any named dataset written to storage.

    dbt / other templated SQL
      • Output = the model's final SELECT columns written to the
        materialised table/view.

4.  Column names in the catalogue MUST exactly match those in the output
    table definition (the INSERT column list, CREATE TABLE spec, or
    output-table component field list).

════════════════════════════════════════════════════════════════════
OUTPUT FORMAT — respond with ONLY valid JSON, no commentary:
════════════════════════════════════════════════════════════════════
{{
  "catalogue": {{
    "output_table.column_name": {{
      "produced_by_file": "filename.sql",
      "transformation":   "expression AS column_name",
      "transformation_type": "direct_copy",
      "source_refs": [
        {{"source_table": "src_table", "source_column": "src_col"}},
        ...
      ]
    }},
    ...
  }},
  "file_outputs": {{
    "filename.sql": ["output_table.col1", "output_table.col2", ...]
  }}
}}

════════════════════════════════════════════════════════════════════
SOURCE FILES
════════════════════════════════════════════════════════════════════
{file_blocks}
"""


def _build_catalogue(
    filepaths: list[str],
    model: Any,
    verbose: bool = False,
) -> dict[str, Any]:
    """Phase 1: ask the LLM to catalogue all output columns across all files."""
    file_blocks = []
    for fp in filepaths:
        p = Path(fp)
        ext = p.suffix.lstrip(".").lower()
        code = p.read_text(encoding="utf-8", errors="replace")[:10000]
        file_blocks.append(
            f"--- FILE: {p.name} ---\n```{ext}\n{code}\n```\n"
        )

    prompt = _CATALOGUE_PROMPT.format(
        n_files=len(filepaths),
        file_blocks="\n".join(file_blocks),
    )

    if verbose:
        print(
            f"  [phase1] building catalogue from {len(filepaths)} files "
            f"({sum(len(b) for b in file_blocks):,} chars) …",
            file=sys.stderr,
        )

    raw = _call_llm(model, prompt)

    try:
        data = _extract_json(raw)
        return data
    except Exception as exc:
        raise RuntimeError(
            f"Phase 1 catalogue parse failed: {exc}\n"
            f"Raw response (first 500 chars):\n{raw[:500]}"
        ) from exc


# ── Phase 2: DAG traversal ────────────────────────────────────────────

_NOT_FOUND_NOTE = (
    "Column not found in any provided file. "
    "It may come from a raw source table or a file not included in the list."
)


def _normalise_key(key: str) -> str:
    """Lowercase and strip for case-insensitive catalogue lookups."""
    return key.strip().lower()


def _build_lookup(catalogue: dict[str, Any]) -> dict[str, Any]:
    """Build a normalised key → entry lookup for fast access."""
    return {_normalise_key(k): v for k, v in catalogue.items()}


def _trace_column(
    target: str,
    lookup: dict[str, Any],
    visited: set[str],
    depth: int = 0,
    max_depth: int = 20,
    _seen_steps: set[tuple[str, str]] | None = None,
) -> list[IntermediateStep]:
    """Recursively trace a table.column upstream through the catalogue.

    Returns ordered list of IntermediateStep, from deepest raw source → target.
    Duplicate (component_name, output_column) pairs are suppressed — when the
    DAG has fan-out nodes that are referenced by multiple downstream columns,
    each unique step appears only once (first occurrence kept).
    """
    if _seen_steps is None:
        _seen_steps = set()

    if depth > max_depth:
        return []

    norm_key = _normalise_key(target)
    if norm_key in visited:
        # Cycle guard
        return []
    visited = visited | {norm_key}

    entry = lookup.get(norm_key)
    if entry is None:
        # Raw source — nothing to add (caller handles this)
        return []

    filename = entry.get("produced_by_file", "unknown")
    transformation = entry.get("transformation", f"{target}")
    output_col = target.split(".")[-1] if "." in target else target

    # Recurse into each source column
    upstream_steps: list[IntermediateStep] = []
    for ref in entry.get("source_refs", []):
        src_key = f"{ref.get('source_table','')}.{ref.get('source_column','')}"
        upstream = _trace_column(
            src_key, lookup, visited, depth + 1, max_depth, _seen_steps
        )
        upstream_steps.extend(upstream)

    # Append THIS step only if we haven't seen this (file, column) pair before.
    # This prevents fan-out nodes from appearing multiple times when multiple
    # downstream columns all reference the same upstream step.
    step_key = (filename, output_col)
    if step_key not in _seen_steps:
        _seen_steps.add(step_key)
        this_step = IntermediateStep(
            component_name=filename,
            expression=transformation,
            output_column=output_col,
        )
        return upstream_steps + [this_step]

    return upstream_steps


def _build_source_refs(
    entry: dict[str, Any],
    lookup: dict[str, Any],
) -> list[SourceColumnRef]:
    """Extract the OUTERMOST source refs for a column.

    Walks back through catalogue entries to find the true raw-source columns
    (those not produced by any file in the catalogue).
    """
    raw_refs: list[SourceColumnRef] = []

    def _collect(refs: list[dict], seen: set[str]) -> None:
        for ref in refs:
            key = _normalise_key(
                f"{ref.get('source_table','')}.{ref.get('source_column','')}"
            )
            if key in seen:
                continue
            seen.add(key)
            upstream = lookup.get(key)
            if upstream is None:
                # This is a raw source
                raw_refs.append(
                    SourceColumnRef(
                        source_table=ref.get("source_table", ""),
                        source_column=ref.get("source_column", ""),
                    )
                )
            else:
                # Keep going upstream
                _collect(upstream.get("source_refs", []), seen)

    _collect(entry.get("source_refs", []), set())
    return raw_refs or [
        SourceColumnRef(
            source_table=ref.get("source_table", ""),
            source_column=ref.get("source_column", ""),
        )
        for ref in entry.get("source_refs", [])
    ]


# ── Full-chain transformation string ─────────────────────────────────


def _build_full_transformation(
    raw_source_refs: list[SourceColumnRef],
    steps: list[IntermediateStep],
) -> str:
    """Build a single self-contained lineage string from sources → final output.

    Steps are listed in SEQUENTIAL EXECUTION ORDER — each step's output feeds
    directly into the next step.  The reader can follow the chain from the raw
    source columns all the way to the final output without consulting any other
    column.

    Format:
      SOURCES: raw_table.col1, raw_table.col2
      → [01_raw_orders.sql] t.gross_value AS raw_amount
      → [04_enrich_orders.mp] raw_amount * (1 - discount_pct/100) AS discounted_amount
      → [07_final_metrics.sql] SUM(net_order_revenue) AS net_revenue

    Each [filename] tag identifies which pipeline file performs that step.
    """
    parts: list[str] = []

    if raw_source_refs:
        src_list = ", ".join(
            f"{r.source_table}.{r.source_column}" for r in raw_source_refs
        )
        parts.append(f"SOURCES: {src_list}")

    for step in steps:
        parts.append(f"→ [{step.component_name}] {step.expression}")

    return "\n".join(parts)


# ── Public API ────────────────────────────────────────────────────────


def trace_columns(
    target_columns: list[str],
    source_files: list[str],
    *,
    model: Any | None = None,
    verbose: bool = False,
) -> list[ColumnLineage]:
    """Trace each target column across all source files.

    Parameters
    ----------
    target_columns:
        List of fully-qualified column names: ``["table.column", ...]``.
    source_files:
        List of file paths (any mix of .sql, .mp, .xml, .py).
    model:
        Optional pre-built LangChain chat model.
    verbose:
        Print progress to stderr.

    Returns
    -------
    list[ColumnLineage]
        Exactly ``len(target_columns)`` entries — one per requested column.
        Columns not found anywhere get an empty entry with an explanatory note.
    """
    if model is None:
        model = _get_model()

    # ── Phase 1: catalogue ──────────────────────────────────────────
    catalogue_data = _build_catalogue(source_files, model, verbose=verbose)
    raw_catalogue: dict[str, Any] = catalogue_data.get("catalogue", {})

    if verbose:
        print(
            f"  [phase1] catalogue has {len(raw_catalogue)} entries.",
            file=sys.stderr,
        )

    lookup = _build_lookup(raw_catalogue)

    # ── Phase 2: trace each target column ──────────────────────────
    results: list[ColumnLineage] = []

    for target in target_columns:
        if "." not in target:
            results.append(
                ColumnLineage(
                    target_table="",
                    target_column=target,
                    source_refs=[],
                    transformation="",
                    transformation_type="other",
                    notes=(
                        f"Column '{target}' is not fully qualified. "
                        "Please use 'table.column' format."
                    ),
                )
            )
            continue

        parts = target.rsplit(".", 1)
        target_table, target_column = parts[0], parts[1]
        norm_key = _normalise_key(target)
        entry = lookup.get(norm_key)

        if entry is None:
            if verbose:
                print(f"  [phase2] '{target}' — NOT FOUND", file=sys.stderr)
            results.append(
                ColumnLineage(
                    target_table=target_table,
                    target_column=target_column,
                    source_refs=[],
                    transformation="",
                    transformation_type="other",
                    notes=_NOT_FOUND_NOTE,
                    filename="",
                )
            )
            continue

        if verbose:
            print(f"  [phase2] tracing '{target}' …", file=sys.stderr)

        # Full DAG traversal — steps ordered raw-source → final output (sequential)
        steps = _trace_column(target, lookup, visited=set())

        # Raw source refs (deepest upstream raw tables)
        raw_source_refs = _build_source_refs(entry, lookup)

        final_file = entry.get("produced_by_file", "")
        final_type = entry.get("transformation_type", "other")

        # source_filenames: all files traversed BEFORE the final output file,
        # in order of first appearance, deduplicated.
        source_filenames = list(
            dict.fromkeys(
                s.component_name for s in steps[:-1]
                if s.component_name != final_file
            )
        )

        # Full chain transformation string — completely self-contained.
        # Steps are in sequential execution order (each feeds the next).
        full_transformation = _build_full_transformation(raw_source_refs, steps)

        if verbose:
            print(
                f"    → {len(steps)} steps across "
                f"{len(set(s.component_name for s in steps))} file(s), "
                f"produced by '{final_file}'",
                file=sys.stderr,
            )

        results.append(
            ColumnLineage(
                target_table=target_table,
                target_column=target_column,
                source_refs=raw_source_refs,
                transformation=full_transformation,
                transformation_type=final_type,
                intermediate_steps=[],   # not used — full chain is in transformation
                source_filenames=source_filenames,
                filename=final_file,
                notes="",
            )
        )

    return results


def write_trace_json(
    lineage: list[ColumnLineage],
    output_path: str | Path,
) -> Path:
    """Serialise the trace result to a JSON file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = [json.loads(cl.model_dump_json()) for cl in lineage]
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return output_path
