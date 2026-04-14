-- 01_raw_orders.sql
-- Reads from raw transaction log and produces a cleaned orders staging table.
-- Writes: dbo.stg_orders

INSERT INTO dbo.stg_orders (
    order_id,
    customer_id,
    order_date,
    raw_amount,
    currency_code,
    discount_pct,
    region_code,
    is_cancelled
)
SELECT
    t.txn_id                                                AS order_id,
    t.cust_ref                                              AS customer_id,
    CAST(t.created_at AS DATE)                              AS order_date,
    t.gross_value                                           AS raw_amount,
    UPPER(TRIM(t.currency))                                 AS currency_code,
    COALESCE(t.disc_pct, 0.0)                              AS discount_pct,
    CASE
        WHEN t.ship_region IN ('NA','US','CA') THEN 'NORTH_AMERICA'
        WHEN t.ship_region IN ('EU','UK','DE','FR') THEN 'EUROPE'
        ELSE 'OTHER'
    END                                                     AS region_code,
    CASE WHEN t.status = 'CANCELLED' THEN 1 ELSE 0 END     AS is_cancelled
FROM raw.txn_log t
WHERE t.created_at >= '2023-01-01'
  AND t.source_system = 'ECOMM';
