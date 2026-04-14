# Cross-File Column Lineage — Usage Guide

This guide explains how to use the cross-file column lineage tracer with two
different agent backends:

1. **LangGraph deep_agent** (Python, runs locally via the `DataPipelineToMermaid` package)
2. **GitHub Copilot agent** (VS Code, ReAct-style, runs in the editor)

Both backends use the same two prompts under the hood — only the wiring differs.

---

## What the tool does

Given:
- A **list of target columns** (`table.column` format)
- A **list of pipeline files** (any mix of `.sql`, `.mp`, `.xml`, `.py`)

It produces:
1. **`execution_order.json`** + **`execution_order.html`** — The DAG execution
   order of your files (which file must run before which), as a Mermaid diagram.
2. **`column_trace.json`** — For each target column, the full cross-file lineage:
   every transformation step, every file it passed through, and the raw source columns.
3. **`column_trace.xlsx`** — Six-sheet Excel workbook with the same data in
   analyst-friendly format.

---

## Option A — LangGraph deep_agent (Python)

### Prerequisites

```bash
pip install -r requirements.txt
cp .env.example .env
# Fill in OPENROUTER_API_KEY and LLM_MODEL in .env
```

### Quick start

Edit `__main__.py` — find the **USER CONFIGURATION** block:

```python
TARGET_COLUMNS: list[str] = [
    "dbo.fact_pipeline_metrics.net_revenue",
    "dbo.customer_risk_scores.risk_score",
]

SOURCE_FILES: list[str] = [
    "path/to/01_raw_orders.sql",
    "path/to/04_enrich_orders.mp",
    "path/to/07_final_metrics.sql",
    # ... all files in the pipeline
]

OUTPUT_DIR: str = "output/my_trace"
```

Run:

```bash
# From the MermaidInteractive/ directory (parent of DataPipelineToMermaid/)
python -m DataPipelineToMermaid trace
```

Outputs land in `output/my_trace/`.

### Using the modules directly

```python
from DataPipelineToMermaid.execution_order import deduce_execution_order, write_mermaid_html
from DataPipelineToMermaid.cross_file_tracer import trace_columns, write_trace_json

files = ["01_raw.sql", "04_enrich.mp", "07_final.sql"]

# Step 1: execution order
order = deduce_execution_order(files, verbose=True)
write_mermaid_html(order.mermaid, "order.html")
for i, stage in enumerate(order.stages):
    print(f"Stage {i}: {stage}")

# Step 2: column lineage
rows = trace_columns(
    target_columns=["dbo.fact.net_revenue", "dbo.risk.risk_score"],
    source_files=files,
    verbose=True,
)
write_trace_json(rows, "column_trace.json")
for cl in rows:
    print(f"{cl.target_table}.{cl.target_column}: {len(cl.intermediate_steps)} steps")
```

### How the LLM is called

The deep_agent approach uses **two sequential LLM calls** (no tool loop needed —
the prompts are designed to complete in one shot each):

| Call | What it does | Typical tokens |
|---|---|---|
| Phase 1 — Catalogue | Reads all files; builds `{table.column → file + transformation}` map | ~8–20K input, ~4–8K output |
| Phase 2 — Order | Reads the I/O map; returns topologically-sorted stages + edges JSON | ~2–4K input, ~1K output |

The DAG traversal (Phase 2 of the tracer) is pure Python — no additional LLM call.

---

## Option B — GitHub Copilot Agent (VS Code)

The GitHub Copilot agent follows a **ReAct** loop: it reads files, reasons,
takes actions (file reads), and iterates. Use this when you do not have
Python/OpenRouter available or prefer to work interactively in VS Code.

### Setup

1. Open the repository in VS Code.
2. Open GitHub Copilot Chat (Ctrl+Shift+I / ⌘⇧I).
3. Switch to **Agent mode** (click the dropdown next to the chat input → "Agent").

### Prompt for execution order

Paste the following into Copilot Agent chat, replacing the file list:

```
@workspace

You are a data-pipeline architect performing execution order analysis.

I have a set of pipeline files that are part of the same ETL workflow:
- 01_raw_orders.sql
- 02_raw_customers.sql
- 03_raw_payments.sql
- 04_enrich_orders.mp
- 05_reconcile_payments.sql
- 06_customer_risk.mp
- 07_final_metrics.sql

PHASE 1 — For each file, read its contents and identify:
  a) Every table/dataset it READS FROM
  b) Every table/dataset it WRITES TO / PRODUCES
  c) A one-sentence description of what it does

PHASE 2 — Using the read/write map from Phase 1:
  a) Build a dependency graph: file A must run before file B if A writes
     something that B reads.
  b) Topologically sort the graph into stages (parallel files in the same stage).
  c) List the dependency edges as {from_file → to_file, via: shared_table}.
  d) Flag any cycles or ambiguities.

Output a JSON block in this format:
{
  "stages": [{"stage": 0, "files": [...]}, {"stage": 1, "files": [...]}, ...],
  "edges":  [{"from": "file_a.sql", "to": "file_b.sql", "via": "table_name"}, ...],
  "warnings": []
}

Then render the result as a Mermaid flowchart LR diagram.
```

### Prompt for cross-file column lineage

```
@workspace

You are a data-lineage expert performing cross-file column tracing.

TARGET COLUMNS to trace (fully qualified as table.column):
- dbo.fact_pipeline_metrics.net_revenue
- dbo.customer_risk_scores.risk_score

PIPELINE FILES (part of the same workflow — outputs of earlier files feed
into later files):
- 01_raw_orders.sql
- 02_raw_customers.sql
- 03_raw_payments.sql
- 04_enrich_orders.mp
- 05_reconcile_payments.sql
- 06_customer_risk.mp
- 07_final_metrics.sql

INSTRUCTIONS — perform a TWO-PHASE analysis:

PHASE 1 — Build a cross-file catalogue:
  Read every file. For every column that any file writes to an output
  table/dataset, record:
    • output_key: "output_table.column_name"
    • produced_by_file: which file
    • transformation: the SQL/logic expression (ending AS column_name)
    • transformation_type: one of [direct_copy, aggregation, calculation,
        case_logic, join_key, window_function, type_cast, concatenation,
        lookup, conditional, constant, string_manipulation,
        date_manipulation, coalesce, other]
    • source_refs: every source column as {"source_table": ..., "source_column": ...}
  Trace through CTEs transparently — attribute their columns to the raw
  input table, not the CTE alias.

PHASE 2 — Trace each target column:
  For each target column:
    1. Look it up in the catalogue. If not found, output an empty entry
       with notes: "Column not found in any provided file."
    2. Record the transformation and source columns.
    3. For each source column, check if it appears in the catalogue as an
       output of another file. If yes, recurse upstream into that file.
    4. Continue until every branch reaches a column NOT produced by any
       file in the list (raw source). Stop that branch.
    5. Collect ALL intermediate steps ordered from raw-source inward to
       the final output. Each step records: component_name (filename),
       expression, output_column.

Output a JSON array — exactly one entry per target column:
[
  {
    "target_table": "...",
    "target_column": "...",
    "source_refs": [{"source_table": "...", "source_column": "..."}],
    "transformation": "... AS column_name",
    "transformation_type": "...",
    "filename": "the file that produces the final output",
    "notes": "",
    "intermediate_steps": [
      {"component_name": "filename.sql", "expression": "...", "output_column": "..."},
      ...
    ]
  }
]
```

### Tips for Copilot agent usage

- If the agent stops reading files mid-way (context limit), split your file
  list into smaller batches and ask it to "merge the catalogue" in a follow-up.
- For very large files (>500 lines), prepend: *"For each file, focus only on
  INSERT/SELECT statements and output-table definitions. Skip comments."*
- You can ask the agent to render the lineage as a Mermaid diagram after
  producing the JSON.
- Use `#file:01_raw_orders.sql` syntax to attach specific files if `@workspace`
  does not pick them up automatically.

---

## Understanding the outputs

### `execution_order.json`

```json
{
  "stages": [
    {"stage": 0, "files": ["01_raw_orders.sql", "02_raw_customers.sql", "03_raw_payments.sql"]},
    {"stage": 1, "files": ["04_enrich_orders.mp"]},
    {"stage": 2, "files": ["05_reconcile_payments.sql"]},
    {"stage": 3, "files": ["06_customer_risk.mp"]},
    {"stage": 4, "files": ["07_final_metrics.sql"]}
  ],
  "edges": [
    {"from": "01_raw_orders.sql", "to": "04_enrich_orders.mp", "via": "stg_orders"},
    ...
  ]
}
```

Files in the **same stage** can run in parallel. Files in a **later stage**
depend on at least one file in an earlier stage.

### `column_trace.json`

```json
[
  {
    "target_table": "dbo.fact_pipeline_metrics",
    "target_column": "net_revenue",
    "source_refs": [
      {"source_table": "raw.txn_log", "source_column": "gross_value"},
      {"source_table": "raw.txn_log", "source_column": "disc_pct"}
    ],
    "transformation": "SUM(net_order_revenue) AS net_revenue",
    "transformation_type": "aggregation",
    "filename": "07_final_metrics.sql",
    "notes": "",
    "intermediate_steps": [
      {"component_name": "01_raw_orders.sql",
       "expression": "t.gross_value AS raw_amount", "output_column": "raw_amount"},
      {"component_name": "04_enrich_orders.mp",
       "expression": "raw_amount * (1 - discount_pct/100) AS discounted_amount",
       "output_column": "discounted_amount"},
      {"component_name": "05_reconcile_payments.sql",
       "expression": "o.discounted_amount + o.loyalty_bonus AS net_order_revenue",
       "output_column": "net_order_revenue"},
      {"component_name": "07_final_metrics.sql",
       "expression": "SUM(net_order_revenue) AS net_revenue",
       "output_column": "net_revenue"}
    ]
  }
]
```

`intermediate_steps` are ordered **raw-source → final output**. Each step's
`component_name` is the file where that transformation lives.

### `execution_order.html`

Open in a browser. Displays the pipeline DAG as an interactive Mermaid diagram
colour-coded by file type:
- Green — SQL
- Orange — Ab-Initio (.mp)
- Blue — Informatica (.xml)
- Purple — Python/Pandas (.py)

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `OPENROUTER_API_KEY not set` | Copy `.env.example` → `.env`, fill in your key |
| Phase 1 returns empty catalogue | The files may be too large — set `LLM_MAX_TOKENS=32768` in `.env` |
| Column marked "not found" but it exists | Check that the column name matches exactly (case-sensitive table.column) |
| Copilot stops before reading all files | Reduce the file list per message; ask it to continue with remaining files |
| Mermaid diagram not rendering | Open in Chrome/Firefox; requires internet for CDN |
| Wrong execution order | Add a `--verbose` flag and inspect the `node_io` section in `execution_order.json` |
