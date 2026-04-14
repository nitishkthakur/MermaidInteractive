# DataPipelineToMermaid

A Python package that uses an LLM agent to extract **column-level data lineage** from source code (SQL, stored procedures, Informatica mappings, Ab-Initio graphs, Pandas ETL scripts) and converts it into three output artefacts:

1. **Lineage JSON** — structured, machine-readable, schema-validated intermediate representation
2. **Excel workbook** — six-sheet human-readable documentation file
3. **Interactive Mermaid HTML** — click-to-highlight flowchart diagram

The package is a sub-module of the `MermaidInteractive` project, and the HTML step leverages the parent project's interactive renderer when available.

---

## Directory layout

```
DataPipelineToMermaid/
├── __init__.py
├── __main__.py               # python -m DataPipelineToMermaid entry point
├── main.py                   # CLI: argparse wiring for extract / convert / full
├── models.py                 # Pydantic v2 JSON schema for PipelineLineage
├── prompt_template.py        # System prompt given to the LLM agent
├── agent.py                  # Deep Agent harness (LangGraph + OpenRouter)
├── mermaid_export.py         # PipelineLineage → .mmd + interactive HTML
├── excel_export.py           # PipelineLineage → .xlsx workbook
├── requirements.txt
├── test_fixtures/
│   ├── sample_lineage.json   # Canonical JSON fixture used by all tests
│   ├── complex_sql_20_ctes.sql
│   ├── stored_procs_12.sql
│   ├── informatica_mapping.xml
│   ├── abinitio_graph.mp
│   └── pandas_etl.py
├── tests/
│   ├── conftest.py           # Shared fixtures (sample_lineage, tmp_output)
│   ├── test_models.py        # Pydantic schema unit tests
│   ├── test_agent_helpers.py # JSON-extraction helper tests (no LLM)
│   ├── test_mermaid_export.py
│   └── test_excel_export.py
└── output/                   # Generated artefacts from real runs
    ├── abinitio/
    ├── complex_sql/
    ├── informatica/
    ├── pandas/
    └── stored_procs/
```

---

## Installation & requirements

```bash
pip install -r DataPipelineToMermaid/requirements.txt
```

Runtime dependencies:

| Package | Purpose |
|---|---|
| `deepagents >= 0.4.0` | LangGraph-based Deep Agent framework |
| `langchain >= 1.2.0` | Agent/chain orchestration |
| `langchain-openrouter >= 0.2.0` | `ChatOpenAI` pointed at OpenRouter |
| `openpyxl >= 3.1` | Excel workbook generation |
| `pydantic >= 2.0` | Data model validation and JSON serialisation |
| `python-dotenv >= 1.0` | `.env` file loading for API keys |

Dev/test only: `pytest >= 7.0`, `pytest-cov >= 4.0`.

### API key

The LLM step requires an [OpenRouter](https://openrouter.ai/) API key:

```bash
cp DataPipelineToMermaid/.env.template DataPipelineToMermaid/.env
# Edit .env and set OPENROUTER_API_KEY=<your key>
```

Optional environment variables (`.env` or shell):

| Variable | Default | Description |
|---|---|---|
| `OPENROUTER_API_KEY` | *(required)* | OpenRouter secret key |
| `LLM_MODEL` | `anthropic/claude-sonnet-4` | Model slug passed to OpenRouter |
| `LLM_MAX_TOKENS` | `16384` | Maximum output tokens |
| `LLM_TEMPERATURE` | `0` | Sampling temperature (0 = deterministic) |

---

## CLI usage

```bash
# Full pipeline: source file → JSON + Excel + HTML
python -m DataPipelineToMermaid full  path/to/query.sql  -o output/

# Extract only (produces .lineage.json; requires API key)
python -m DataPipelineToMermaid extract  path/to/query.sql  -o lineage.json

# Convert only (no LLM call — reads existing JSON, writes Excel + HTML)
python -m DataPipelineToMermaid convert  lineage.json  -o output_dir/

# Column-level detail (one node per table.column in the Mermaid diagram)
python -m DataPipelineToMermaid full  query.sql  -o out/  --detail column

# Skip specific outputs
python -m DataPipelineToMermaid convert  lineage.json  --no-excel --no-html

# Omit optional fields from the lineage JSON (core fields are always included)
python -m DataPipelineToMermaid full  query.sql  -o out/  --skip-lineage-fields notes filename
python -m DataPipelineToMermaid convert  lineage.json  --skip-lineage-fields notes filename intermediate_steps transformation_type
```

`--skip-lineage-fields` is available on all three sub-commands. Accepted values: `notes`, `filename`, `intermediate_steps`, `transformation_type`. Core fields (`target_table`, `target_column`, `source_refs`, `transformation`) are always included.

All three sub-commands accept `-v` / `--verbose` to print progress to stderr.

---

## Architecture and data flow

```
Source file (SQL / XML / .mp / .py)
          │
          │  [agent.py]  extract_lineage()
          │  LLM agent (Deep Agent + OpenRouter)
          │  guided by LINEAGE_EXTRACTION_PROMPT
          ▼
  PipelineLineage (models.py)   ←──  validated by Pydantic v2
          │
          ├──[models.py]  .to_json()  ──────────►  .lineage.json
          │
          ├──[excel_export.py]  lineage_to_excel()  ►  .xlsx
          │
          └──[mermaid_export.py]  lineage_to_html()  ►  .html
                             └──  lineage_to_mermaid_file()  ►  .mmd
```

### Step 1 — LLM extraction (`agent.py`)

`extract_lineage(source_path)` does:

1. Reads the source file from disk.
2. Calls `create_lineage_agent()` which builds a `deepagents.create_deep_agent` with a `ChatOpenAI` model pointed at `https://openrouter.ai/api/v1`.
3. Sends a single user message containing the file suffix, path, and full source code.
4. Handles three response formats out of the Deep Agent:
   - Raw JSON in the message
   - Markdown-fenced JSON (` ```json ... ``` `)
   - Virtual file reference (`WRITTEN_TO_FILE:/output/lineage.json`) — used when the agent writes a large JSON to its virtual filesystem instead of the message body
5. Uses `_extract_json_from_text()` to peel the JSON string out of whichever format was returned.
6. Deserialises the JSON into a `PipelineLineage` via Pydantic.

There is also `extract_lineage_from_text(code, code_type, name)` for callers that already have the code in memory.

### Step 2 — Data model (`models.py`)

All models are **Pydantic v2 `BaseModel`** subclasses. The full hierarchy:

```
PipelineLineage
├── pipeline_name      str
├── pipeline_type      str  (sql_query | stored_procedures | informatica | abinitio | pandas_etl | mixed)
├── source_file        str
├── description        str
├── sources            list[TableInfo]
│     └── columns      list[ColumnInfo]   (name, data_type, description)
├── targets            list[TableInfo]
│     └── columns      list[ColumnInfo]
├── components         list[Component]
│     ├── name, component_type, description
│     ├── input_tables  list[str]
│     ├── output_columns list[str]
│     └── sql_text       str  (up to ~200 chars)
├── column_lineage     list[ColumnLineage]
│     ├── target_table, target_column
│     ├── source_refs     list[SourceColumnRef]
│     │     ├── source_table  str   (schema.table or table)
│     │     └── source_column str   (column name only)
│     ├── transformation  str         (SQL ending with AS <output_column_name>)
│     ├── transformation_type str     (14 allowed categories)
│     ├── intermediate_steps list[IntermediateStep]
│     ├── filename        str         (source file where transformation is defined)
│     └── notes           str
└── data_flow_edges    list[DataFlowEdge]
      ├── from_node, to_node
      ├── columns     list[str]
      └── edge_label  str
```

`TableInfo.full_name` is a property returning `schema.table` if a schema is set, otherwise just `table`.

`PipelineLineage` has two serialisation helpers:
- `.to_json(path=None, skip_lineage_fields=None)` — returns a pretty-printed JSON string and optionally writes it to a file. Pass a list of field names (`"notes"`, `"filename"`, `"intermediate_steps"`, `"transformation_type"`) to omit optional fields from every `column_lineage` entry.
- `.from_json(path_or_text)` — accepts a file path or a raw JSON string

Backward compatibility: JSON files with the old `source_columns: list[str]` format are automatically migrated to `source_refs` by a `model_validator(mode="before")` on load.

### Step 3 — Mermaid export (`mermaid_export.py`)

Two detail levels are supported, selected by the `--detail` CLI flag:

#### `table` level (default)

`_table_level_mermaid()` generates a `flowchart LR` with three Mermaid subgraphs:

- **📥 Source Tables** — one rectangular node per source; label includes the table name and a preview of up to six column names
- **⚙️ Transformations** — one node per `Component`; CTEs and stored procedures use diamond `{}` shape, others use trapezoid `/\` shape
- **📤 Target Tables** — one rectangular node per target; same column preview

Edges are drawn from `data_flow_edges` if present; otherwise inferred by grouping `column_lineage[*].source_columns` by table. Edge deduplication is enforced via a `seen` set. Edge labels come from `edge_label` or the column count (`"N cols"`).

Class definitions apply CSS-like colour coding:
- Source nodes: `fill:#e8f5e9` (green tint), `stroke:#2e7d32`
- Target nodes: `fill:#e3f2fd` (blue tint), `stroke:#1565c0`
- Transform nodes: `fill:#fff3e0` (orange tint), `stroke:#e65100`

#### `column` level

`_column_level_mermaid()` generates one subgraph per table (sources and targets), with one node per column inside each subgraph. Edges trace back from each `ColumnLineage` entry: every `source_refs` entry gets an edge from `{source_table}.{source_column}` to the `target_column`, labelled with the `transformation_type`.

#### HTML generation

`lineage_to_html()` attempts to call `generate_interactive_html()` from the parent `mermaid_interactive` module (click-to-highlight interaction). If that import fails (e.g., the package is used standalone), it falls back to `_standalone_html()`, which embeds Mermaid.js from the CDN and produces a basic, non-interactive page.

Node and edge IDs are sanitised through `_safe_id()` (non-alphanumeric chars → `_`, digit-leading names prefixed with `n_`) and labels through `_esc()` (double quotes → single quotes, newlines → spaces, truncation to `max_len`).

### Step 4 — Excel export (`excel_export.py`)

`lineage_to_excel()` creates an `openpyxl` workbook with six sheets:

| Sheet | Columns | Content |
|---|---|---|
| **Overview** | Property, Value | Pipeline metadata + counts |
| **Source Tables** | Schema, Table, Column, Data Type, Description | One row per column of each source table |
| **Target Tables** | Schema, Table, Column, Data Type, Description | One row per column of each target table |
| **Column Lineage** | Target Table, Target Column, Source Table(s), Source Column(s), Transformation (SQL), Type, Filename, Intermediate Steps, Notes | One row per `ColumnLineage` entry; source tables and source columns are newline-joined from `source_refs`; intermediate steps serialised as `[component] expr → output_col → …` |
| **Components** | Name, Type, Description, Input Tables, Output Columns, SQL / Code Snippet | One row per `Component`; `sql_text` capped at 200 chars |
| **Data Flow** | From, To, Columns, Label | One row per `DataFlowEdge` |

Styling constants:
- Header row: bold white text on `#2F5496` fill, centred, thin borders
- Data rows: alternating `#D6E4F0` / white fill, thin borders, top-aligned with wrap
- Column widths: auto-fitted (14–55 chars) based on maximum cell content length

---

## The LLM prompt (`prompt_template.py`)

`LINEAGE_EXTRACTION_PROMPT` is the system prompt injected into the Deep Agent. It is structured into five sections:

1. **Output format** — the full JSON schema the LLM must produce, with field-level instructions
2. **Critical principles** — `THE #1 RULE: Every piece of business logic must be captured`. Specific rules for:
   - Table identification using `table.column` notation everywhere
   - `source_refs` must split each source reference into separate `source_table` and `source_column` fields (no combined `table.column` strings)
   - Transformations must be complete SQL expressions ending with `AS <output_column_name>` (non-SQL sources are translated to equivalent SQL)
   - `filename` must be populated with the source file path where the transformation is defined
   - 14 `transformation_type` categories with definitions
   - Component descriptions written at a business level, not a technical one
   - Descriptive data-flow edge labels
3. **Language-specific guidance** — detailed extraction rules per source type:
   - SQL / Stored Procedures: CTEs, temp tables, MERGE branches, window functions, CASE branches
   - Informatica PowerCenter / IICS: each transformation type and its port-level lineage
   - Ab-Initio `.mp` graphs: ten component types with explicit field semantics and the `::` DML assignment syntax
   - Pandas / Python ETL: `read_csv`/`pd.merge`/`groupby().agg()`/`assign` patterns
4. **Self-validation checklist** — 11 checks the LLM must run three times before emitting the JSON
5. **Output instructions** — emit raw JSON only; use `write_file` for large responses

---

## Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run a single class
python -m pytest tests/test_parser.py::TestBasicArrows -v

# With coverage
pytest tests/ --cov=DataPipelineToMermaid
```

### Test structure

All tests are **pytest class-based** and use fixtures from `conftest.py`.

**`conftest.py`** provides:
- `fixtures_dir` — path to `test_fixtures/`
- `sample_lineage_path` — path to `test_fixtures/sample_lineage.json`
- `sample_lineage_dict` — the JSON file loaded as a raw `dict`
- `sample_lineage` — a fully deserialised `PipelineLineage` model
- `tmp_output` — a `tmp_path / "output"` directory for file output tests

**`test_models.py`** — Pydantic schema validation:
- `TestColumnInfo` — required vs optional fields, type coercion
- `TestTableInfo` — `full_name` property with/without schema, column list
- `TestColumnLineage` — minimal construction, `intermediate_steps`, `to_json`/`from_json` round-trip

**`test_agent_helpers.py`** — `_extract_json_from_text()` with no API calls:
- Raw JSON, markdown-fenced JSON, plain-text-embedded JSON, nested JSON
- Raises `ValueError` when no JSON object is found
- Large fixture wrapped in markdown fences

**`test_mermaid_export.py`**:
- `TestSafeId` / `TestEsc` — helper function edge cases
- `TestTableLevelMermaid` — starts with `flowchart LR`, contains all three subgraphs, correct node IDs, edge deduplication, `classDef` lines
- `TestColumnLevelMermaid` — subgraph-per-table structure, column nodes, typed edge labels
- `TestLineageToMermaid` — dispatch between detail levels
- `TestLineageToMermaidFile` — writes `.mmd` file to disk
- `TestLineageToHtml` — writes `.html` file, fallback HTML structure
- `TestStandaloneHtml` — CDN script tag, escaped Mermaid source, title in `<h1>`

**`test_excel_export.py`**:
- `TestExcelExportFull` — file creation, six sheet names, header content, row counts matching model field counts, column lineage headers, component sheet structure

### Test fixture

`test_fixtures/sample_lineage.json` contains a "Customer Analytics ETL" pipeline with:
- 4 source tables (`sales.customers`, `sales.orders`, `sales.order_items`, `products.products`)
- 1 target table (`analytics.customer_summary`)
- 2 CTE components (`cte_active_customers`, `cte_order_stats`)
- Multiple column lineage entries representing aggregations, direct copies, CASE logic, and calculations

---

## Real output examples

The `output/` directory contains artefacts generated from the five test fixtures:

| Fixture | Type | Description |
|---|---|---|
| `complex_sql/` | SQL | 20-CTE query; extensive column lineage with window functions and aggregations |
| `stored_procs/` | SQL | 12 stored procedures; multi-proc lineage with temp tables |
| `informatica/` | XML | Informatica PowerCenter mapping with Expression, Joiner, Aggregator transforms |
| `abinitio/` | `.mp` | Ab-Initio graph with Lookup, Filter, Rollup, Reformat, and Dedup components |
| `pandas/` | Python | Pandas ETL script; `read_csv`/`merge`/`groupby`/`to_sql` lineage |

Each folder contains a `.lineage.json`, a `.mmd`, and an interactive `.html` file.

---

## Design decisions

### Why Pydantic v2?
The JSON schema is the contract between the LLM and the downstream converters. Pydantic validates the LLM's output at the boundary (catching hallucinated field names, wrong types, missing required fields) and provides free serialisation via `model_dump_json`. The `Field(description=...)` annotations also serve as inline schema documentation for the LLM prompt.

### Why Deep Agent (LangGraph) instead of a plain API call?
`deepagents.create_deep_agent` gives the LLM a tool (`write_file`) that allows it to write the JSON to a virtual filesystem when the response would exceed the output token limit. The agent harness then reads the file back. This handles very large pipelines (e.g., 20 CTEs, 12 stored procedures) without truncation.

### Why OpenRouter?
OpenRouter provides a unified endpoint for multiple model providers (Anthropic Claude, OpenAI, etc.) with a single API key. The model is configurable via `LLM_MODEL` so users can swap models without code changes.

### Why two Mermaid detail levels?
Table level is the default because it produces a diagram that fits on screen and is legible for stakeholders. Column level is available for detailed technical review but produces very large graphs for pipelines with many columns.

### Why a standalone HTML fallback?
The `lineage_to_html()` function is designed to work both inside the `MermaidInteractive` project (where `generate_interactive_html` is available) and as a standalone package. The fallback ensures the converter is always usable even without the parent project installed.

### Why strip `sql_text` to 200 chars in components?
The full SQL can be retrieved from the source file. Storing only the first 200 chars keeps the JSON and Excel files human-scannable without embedding the entire source code.

### Security considerations
- All user-supplied strings are HTML-escaped before embedding in generated HTML (no XSS exposure).
- The LLM prompt instructs the model to output ONLY JSON — no system commands or code that could be executed.
- API keys are loaded from `.env` files and never logged or embedded in output artefacts.
- The `.env` file is conventionally gitignored; a `.env.template` is provided instead.
