-- 08_sp_loyalty_tiers.sql
-- Stored Procedure: dbo.sp_calculate_loyalty_tiers
-- Reads: dbo.stg_customers (from 02_raw_customers.sql)
--        dbo.stg_orders    (from 01_raw_orders.sql)
-- Writes: dbo.customer_loyalty_tiers
--
-- Computes a loyalty tier and loyalty points for each active customer
-- based on their total spend and order history.  This proc runs in
-- parallel with 04_enrich_orders.mp (both depend on stage-0 outputs).

CREATE OR ALTER PROCEDURE dbo.sp_calculate_loyalty_tiers
AS
BEGIN
    SET NOCOUNT ON;

    -- Truncate and reload (full refresh)
    TRUNCATE TABLE dbo.customer_loyalty_tiers;

    INSERT INTO dbo.customer_loyalty_tiers (
        customer_id,
        loyalty_tier,
        loyalty_points,
        lifetime_orders,
        lifetime_spend,
        avg_order_value,
        first_order_date,
        last_order_date
    )
    WITH order_agg AS (
        -- Aggregate non-cancelled orders per customer
        SELECT
            o.customer_id,
            COUNT(o.order_id)                       AS lifetime_orders,
            SUM(o.raw_amount * (1 - o.discount_pct / 100.0)) AS lifetime_spend,
            AVG(o.raw_amount * (1 - o.discount_pct / 100.0)) AS avg_order_value,
            MIN(o.order_date)                       AS first_order_date,
            MAX(o.order_date)                       AS last_order_date
        FROM dbo.stg_orders o
        WHERE o.is_cancelled = 0
        GROUP BY o.customer_id
    ),
    loyalty_calc AS (
        SELECT
            c.customer_id,
            COALESCE(a.lifetime_orders, 0)          AS lifetime_orders,
            COALESCE(a.lifetime_spend, 0)           AS lifetime_spend,
            COALESCE(a.avg_order_value, 0)          AS avg_order_value,
            a.first_order_date,
            a.last_order_date,
            -- Tier based on total lifetime spend
            CASE
                WHEN COALESCE(a.lifetime_spend, 0) >= 100000 THEN 'PLATINUM'
                WHEN COALESCE(a.lifetime_spend, 0) >= 50000  THEN 'GOLD'
                WHEN COALESCE(a.lifetime_spend, 0) >= 10000  THEN 'SILVER'
                ELSE 'BRONZE'
            END                                     AS loyalty_tier,
            -- Points: 1 point per $10 spent, capped at 99999
            LEAST(
                99999,
                FLOOR(COALESCE(a.lifetime_spend, 0) / 10.0)
            )                                       AS loyalty_points
        FROM dbo.stg_customers c
        LEFT JOIN order_agg a ON a.customer_id = c.customer_id
        WHERE c.is_active = 1
    )
    SELECT
        customer_id,
        loyalty_tier,
        loyalty_points,
        lifetime_orders,
        lifetime_spend,
        avg_order_value,
        first_order_date,
        last_order_date
    FROM loyalty_calc;

END;
GO

-- Execute immediately after definition
EXEC dbo.sp_calculate_loyalty_tiers;
