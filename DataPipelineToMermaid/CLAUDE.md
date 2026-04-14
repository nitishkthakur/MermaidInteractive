# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Full pipeline: source file → JSON + Excel + HTML
python -m DataPipelineToMermaid full  path/to/query.sql  -o output/

# Extract only (LLM call; requires OPENROUTER_API_KEY)
python -m DataPipelineToMermaid extract  path/to/query.sql  -o lineage.json

# Convert existing JSON → Excel + Mermaid (no LLM call)
python -m DataPipelineToMermaid convert  lineage.json  -o output_dir/

# Column-level diagram (one node per table.column)
python -m DataPipelineToMermaid full  query.sql  -o out/  --detail column

# Skip specific outputs
python -m DataPipelineToMermaid convert  lineage.json  --no-excel --no-html

# Omit optional ColumnLineage fields from JSON output
python -m DataPipelineToMermaid full  query.sql  -o out/  --skip-lineage-fields notes filename

# Run all tests
python -m pytest tests/ -v

# Run a single test class or method
python -m pytest tests/test_models.py::TestColumnLineage -v
python -m pytest tests/test_mermaid_export.py::TestTableLevelMermaid::test_has_three_subgraphs -v

# With coverage (run from the DataPipelineToMermaid directory)
pytest tests/ --cov=DataPipelineToMermaid
```

## Environment setup

```bash
cp .env.example .env
# Set OPENROUTER_API_KEY in .env
```

| Variable | Default | Notes |
|---|---|---|
| `OPENROUTER_API_KEY` | *(required for LLM steps)* | From openrouter.ai |
| `LLM_MODEL` | `anthropic/claude-sonnet-4` | Model slug passed to OpenRouter |
| `LLM_MAX_TOKENS` | `16384` | Increase for very large pipelines |
| `LLM_TEMPERATURE` | `0` | Keep at 0 for deterministic extraction |

## Architecture and data flow

```
Source file (SQL / XML / .mp / .py)
          │
          │  [agent.py]  extract_lineage()
          │  Deep Agent (LangGraph) + OpenRouter
          ▼
  PipelineLineage (models.py)   ←── Pydantic v2 validation
          │
          ├── [models.py]  .to_json()  ──────────► .lineage.json
          ├── [excel_export.py]  lineage_to_excel()  ► .xlsx
          └── [mermaid_export.py]  lineage_to_html()  ► .html
                              └── lineage_to_mermaid_file()  ► .mmd
```

### `agent.py` — LLM extraction

`extract_lineage(source_path)` reads a file, invokes `create_lineage_agent()` (a `deepagents.create_deep_agent` backed by `ChatOpenAI` pointing at OpenRouter), and parses the response. Three response formats are handled by `_extract_json_from_text()`: raw JSON, markdown-fenced JSON, and `WRITTEN_TO_FILE:/output/lineage.json` (used when output exceeds the token limit, with JSON recovered from the agent's virtual filesystem).

`extract_lineage_from_text(code, code_type, name)` accepts source code already in memory.

### `models.py` — Pydantic v2 schema

`PipelineLineage` is the root model and the contract between the LLM and the converters. Key hierarchy:

```
PipelineLineage
├── sources / targets   list[TableInfo]  → columns: list[ColumnInfo]
├── components          list[Component]
├── column_lineage      list[ColumnLineage]
│     ├── source_refs   list[SourceColumnRef]  (source_table + source_column)
│     ├── transformation  SQL ending with "AS <output_col>"
│     ├── transformation_type  (14 allowed categories)
│     └── intermediate_steps  list[IntermediateStep]
└── data_flow_edges     list[DataFlowEdge]
```

`TableInfo.full_name` returns `schema.table` or just `table`. Backward compat: old `source_columns: list[str]` format is auto-migrated to `source_refs` by a `model_validator(mode="before")`.

`PipelineLineage.to_json(path, skip_lineage_fields)` and `.from_json(path_or_text)` are the serialisation entry points.

### `mermaid_export.py` — Mermaid generation

Two detail levels, selected by `--detail`:

- **`table`** (default): `_table_level_mermaid()` — `flowchart LR` with three subgraphs (Sources / Transformations / Targets). Edges from `data_flow_edges` or inferred from `column_lineage`. Node IDs via `_safe_id()` (non-alphanum → `_`, digit-leading names prefixed `n_`), labels via `_esc()` (truncates to `max_len`, escapes `"` and `\n`).
- **`column`**: `_column_level_mermaid()` — one subgraph per table, one node per column, edges labelled with `transformation_type`.

`lineage_to_html()` tries to import `generate_interactive_html` from the parent `mermaid_interactive` module; falls back to `_standalone_html()` (CDN-embedded Mermaid.js, non-interactive).

### `excel_export.py` — Six-sheet workbook

`lineage_to_excel()` writes: **Overview**, **Source Tables**, **Target Tables**, **Column Lineage**, **Components**, **Data Flow**. Header style: bold white on `#2F5496`. Alternating row fill `#D6E4F0` / white. Column widths auto-fitted (14–55 chars). `sql_text` in Components is capped at 200 chars.

### `prompt_template.py` — LLM system prompt

`LINEAGE_EXTRACTION_PROMPT` has five sections: output JSON schema, critical extraction principles, language-specific guidance (SQL, Informatica, Ab-Initio, Pandas), an 11-item self-validation checklist, and output instructions. The prompt tells the agent to use `write_file` for large responses.

## Tests

All tests are pytest class-based. `conftest.py` provides fixtures: `fixtures_dir`, `sample_lineage_path`, `sample_lineage_dict`, `sample_lineage` (a `PipelineLineage`), and `tmp_output`.

`test_agent_helpers.py` tests `_extract_json_from_text()` without any LLM calls. `test_models.py` covers Pydantic validation and round-trip serialisation. `test_mermaid_export.py` and `test_excel_export.py` cover the converters end-to-end using `test_fixtures/sample_lineage.json` (a "Customer Analytics ETL" fixture with 4 sources, 1 target, 2 CTEs).

## Supported source types

SQL queries, stored procedures (`.sql`), Informatica PowerCenter mappings (`.xml`), Ab-Initio graphs (`.mp`), and Pandas/Python ETL scripts (`.py`). Real example outputs are in `output/` for all five types.
