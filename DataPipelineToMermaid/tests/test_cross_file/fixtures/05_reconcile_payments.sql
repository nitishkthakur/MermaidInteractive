-- 05_reconcile_payments.sql
-- Reads dbo.enr_orders (from the Ab-Initio graph) and dbo.stg_payments.
-- Joins enriched orders with payments, computing revenue and risk metrics.
-- Note: order_revenue is renamed to net_order_revenue in this file.
-- Writes: dbo.reconciled_orders

INSERT INTO dbo.reconciled_orders (
    order_id,
    customer_id,
    order_date,
    customer_segment,
    region_code,
    discounted_amount,
    loyalty_bonus,
    net_payment_usd,
    payment_status,
    payment_method,
    net_order_revenue,
    payment_gap,
    risk_flag
)
WITH order_payment AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.customer_segment,
        o.region_code,
        o.discounted_amount,
        o.loyalty_bonus,
        p.net_payment_usd,
        p.payment_status,
        p.payment_method,
        -- net_order_revenue: what the business actually earned after discount + loyalty
        o.discounted_amount + o.loyalty_bonus                   AS net_order_revenue,
        -- payment_gap: difference between what was owed vs what was received
        (o.discounted_amount + o.loyalty_bonus) - p.net_payment_usd AS payment_gap
    FROM dbo.enr_orders o
    LEFT JOIN dbo.stg_payments p ON p.order_id = o.order_id
),
risk_assessment AS (
    SELECT
        *,
        CASE
            WHEN payment_status IN ('CHARGEBACK','FULLY_REFUNDED') THEN 'HIGH'
            WHEN payment_gap > 500  THEN 'MEDIUM'
            WHEN payment_status = 'PARTIALLY_REFUNDED'             THEN 'LOW'
            ELSE 'NONE'
        END                                                      AS risk_flag
    FROM order_payment
)
SELECT
    order_id,
    customer_id,
    order_date,
    customer_segment,
    region_code,
    discounted_amount,
    loyalty_bonus,
    net_payment_usd,
    payment_status,
    payment_method,
    net_order_revenue,
    payment_gap,
    risk_flag
FROM risk_assessment;
