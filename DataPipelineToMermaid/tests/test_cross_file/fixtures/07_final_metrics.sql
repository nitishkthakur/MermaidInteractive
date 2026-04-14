-- 07_final_metrics.sql
-- Terminal aggregation step. Reads from BOTH dbo.reconciled_orders AND
-- dbo.customer_risk_scores (two parallel upstream branches merge here).
-- Produces the final executive-level metrics fact table.
-- This is the most downstream file in the pipeline.
-- Writes: dbo.fact_pipeline_metrics

INSERT INTO dbo.fact_pipeline_metrics (
    snapshot_date,
    region_code,
    customer_segment,
    total_orders,
    total_customers,
    gross_revenue,
    net_revenue,
    avg_risk_score,
    high_risk_customer_count,
    revenue_at_risk,
    net_revenue_per_customer,
    chargeback_rate
)
WITH order_summary AS (
    SELECT
        CURRENT_DATE                                            AS snapshot_date,
        region_code,
        customer_segment,
        COUNT(DISTINCT order_id)                                AS total_orders,
        COUNT(DISTINCT customer_id)                             AS order_customers,
        SUM(discounted_amount)                                  AS gross_revenue,
        SUM(net_order_revenue)                                  AS net_revenue,
        SUM(CASE WHEN payment_status = 'CHARGEBACK'
                 THEN 1 ELSE 0 END)                             AS chargeback_count
    FROM dbo.reconciled_orders
    GROUP BY region_code, customer_segment
),
risk_summary AS (
    SELECT
        region_code,
        customer_segment,
        COUNT(DISTINCT customer_id)                             AS total_customers,
        AVG(risk_score)                                         AS avg_risk_score,
        SUM(CASE WHEN risk_tier IN ('CRITICAL','ELEVATED')
                 THEN 1 ELSE 0 END)                             AS high_risk_customer_count,
        SUM(CASE WHEN risk_tier IN ('CRITICAL','ELEVATED')
                 THEN lifetime_revenue ELSE 0 END)              AS revenue_at_risk
    FROM dbo.customer_risk_scores
    GROUP BY region_code, customer_segment
)
SELECT
    o.snapshot_date,
    o.region_code,
    o.customer_segment,
    o.total_orders,
    r.total_customers,
    o.gross_revenue,
    o.net_revenue,
    ROUND(COALESCE(r.avg_risk_score, 0), 2)                     AS avg_risk_score,
    COALESCE(r.high_risk_customer_count, 0)                     AS high_risk_customer_count,
    COALESCE(r.revenue_at_risk, 0)                              AS revenue_at_risk,
    CASE WHEN r.total_customers > 0
         THEN ROUND(o.net_revenue / r.total_customers, 2)
         ELSE 0
    END                                                         AS net_revenue_per_customer,
    CASE WHEN o.total_orders > 0
         THEN ROUND(o.chargeback_count * 100.0 / o.total_orders, 4)
         ELSE 0
    END                                                         AS chargeback_rate
FROM order_summary o
JOIN risk_summary r
  ON r.region_code = o.region_code
 AND r.customer_segment = o.customer_segment;
