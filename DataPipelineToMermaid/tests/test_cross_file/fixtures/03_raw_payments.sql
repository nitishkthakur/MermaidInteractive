-- 03_raw_payments.sql
-- Reads from payment gateway logs and produces a reconciled payments staging table.
-- Handles multi-currency, chargebacks, and partial refunds.
-- Writes: dbo.stg_payments

INSERT INTO dbo.stg_payments (
    payment_id,
    order_id,
    payment_date,
    payment_method,
    gross_amount_usd,
    refund_amount_usd,
    chargeback_amount_usd,
    net_payment_usd,
    payment_status
)
WITH fx AS (
    SELECT currency_code, usd_rate
    FROM ref.fx_rates
    WHERE rate_date = CURRENT_DATE - 1
),
raw_pay AS (
    SELECT
        p.pay_ref                                               AS payment_id,
        p.order_ref                                             AS order_id,
        CAST(p.pay_timestamp AS DATE)                           AS payment_date,
        LOWER(p.method)                                         AS payment_method,
        p.amount * COALESCE(fx.usd_rate, 1.0)                  AS gross_amount_usd,
        COALESCE(p.refund_total, 0) * COALESCE(fx.usd_rate,1.0) AS refund_amount_usd,
        COALESCE(p.chargeback_total,0)*COALESCE(fx.usd_rate,1.0) AS chargeback_amount_usd
    FROM payment_gw.transactions p
    LEFT JOIN fx ON fx.currency_code = p.currency
    WHERE p.pay_timestamp >= '2023-01-01'
)
SELECT
    payment_id,
    order_id,
    payment_date,
    payment_method,
    gross_amount_usd,
    refund_amount_usd,
    chargeback_amount_usd,
    gross_amount_usd - refund_amount_usd - chargeback_amount_usd AS net_payment_usd,
    CASE
        WHEN chargeback_amount_usd > 0 THEN 'CHARGEBACK'
        WHEN refund_amount_usd >= gross_amount_usd THEN 'FULLY_REFUNDED'
        WHEN refund_amount_usd > 0 THEN 'PARTIALLY_REFUNDED'
        ELSE 'SETTLED'
    END                                                         AS payment_status
FROM raw_pay;
