# Cross-File Lineage Tests

Tests for `execution_order.py` and `cross_file_tracer.py`.

## Fixture DAG

Seven interconnected files forming a realistic pipeline:

```
Stage 0 (parallel):
  01_raw_orders.sql    — reads raw.txn_log      → writes dbo.stg_orders
  02_raw_customers.sql — reads crm.customers    → writes dbo.stg_customers
  03_raw_payments.sql  — reads payment_gw.*     → writes dbo.stg_payments

Stage 1:
  04_enrich_orders.mp  — joins stg_orders + stg_customers → writes dbo.enr_orders

Stage 2:
  05_reconcile_payments.sql — joins enr_orders + stg_payments → writes dbo.reconciled_orders

Stage 3:
  06_customer_risk.mp  — aggregates reconciled_orders → writes dbo.customer_risk_scores

Stage 4:
  07_final_metrics.sql — joins reconciled_orders + customer_risk_scores → writes dbo.fact_pipeline_metrics
```

Hard cases tested:
- 4-hop column chain (net_revenue spans 4 files)
- Fan-in: `avg_risk_score` merges two independent upstream branches
- Column rename: `raw_amount` → `discounted_amount` → `net_order_revenue` → `net_revenue`
- Missing column: `ghost_table.ghost_column` → empty row + note
- Unqualified column name → validation error note
- Parallel entry-point detection (01, 02, 03 can run simultaneously)
- Complex formula in Ab-Initio: `risk_score` weighted composite

## Running tests

```bash
# Unit tests only (no LLM calls, runs instantly)
pytest tests/test_cross_file/ -m "not llm" -v

# All tests including LLM integration (requires OPENROUTER_API_KEY in .env)
pytest tests/test_cross_file/ -v

# Single class
pytest tests/test_cross_file/test_cross_file_tracer.py::TestTraceRiskScore -v
```
