-- 02_raw_customers.sql
-- Reads from CRM source and produces a deduplicated customer master.
-- Writes: dbo.stg_customers

INSERT INTO dbo.stg_customers (
    customer_id,
    full_name,
    email,
    country_code,
    customer_segment,
    credit_limit,
    acquisition_date,
    is_active
)
WITH deduped AS (
    SELECT
        c.customer_ref                                          AS customer_id,
        TRIM(c.first_name || ' ' || c.last_name)               AS full_name,
        LOWER(TRIM(c.email_address))                            AS email,
        UPPER(c.country)                                        AS country_code,
        CASE
            WHEN c.annual_spend > 50000 THEN 'PREMIUM'
            WHEN c.annual_spend > 10000 THEN 'STANDARD'
            ELSE 'BASIC'
        END                                                     AS customer_segment,
        COALESCE(c.credit_limit_usd, 0)                        AS credit_limit,
        CAST(c.signup_date AS DATE)                             AS acquisition_date,
        CASE WHEN c.status = 'ACTIVE' THEN 1 ELSE 0 END        AS is_active,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_ref ORDER BY c.updated_at DESC
        )                                                       AS rn
    FROM crm.customers c
    WHERE c.source = 'SALESFORCE'
)
SELECT
    customer_id, full_name, email, country_code,
    customer_segment, credit_limit, acquisition_date, is_active
FROM deduped
WHERE rn = 1;
