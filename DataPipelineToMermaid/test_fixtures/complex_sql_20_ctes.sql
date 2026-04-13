-- ============================================================================
-- Complex Analytics ETL Pipeline
-- 20+ CTEs with window functions, complex joins, CASE logic, date manipulations,
-- aggregations, string operations, lateral joins, and recursive patterns.
-- 
-- Source tables:
--   raw.web_events         – clickstream data
--   raw.transactions       – financial transactions
--   raw.customers          – customer master data
--   raw.products           – product catalog
--   raw.product_categories – category hierarchy
--   raw.exchange_rates     – currency exchange rates
--   raw.promotions         – promotion campaigns
--   raw.customer_segments  – segmentation rules
--   raw.shipping           – shipping/logistics data
--   raw.returns            – product returns
--
-- Target table:
--   analytics.customer_360_summary
--
-- This SQL demonstrates real-world complexity including:
--   • 22 CTEs chained together
--   • Window functions (ROW_NUMBER, RANK, LAG, LEAD, NTILE, SUM OVER, AVG OVER)
--   • Complex CASE WHEN logic with nested conditions
--   • Multi-table JOINs (INNER, LEFT, CROSS APPLY equivalent, self-joins)
--   • Date arithmetic and formatting
--   • String concatenation and manipulation
--   • COALESCE / NULLIF / ISNULL patterns
--   • Aggregate functions with HAVING
--   • Correlated subqueries embedded in CTEs
--   • UNION ALL combinations
--   • Recursive CTE for category hierarchy
-- ============================================================================

WITH

-- ─── CTE 1: active_customers ──────────────────────────────────────────────
-- Filter raw customers to active/trial, normalize names
active_customers AS (
    SELECT
        c.customer_id,
        UPPER(TRIM(c.first_name))                              AS first_name,
        UPPER(TRIM(c.last_name))                               AS last_name,
        LOWER(TRIM(c.email))                                   AS email,
        c.phone,
        c.country,
        c.state,
        c.city,
        c.signup_date,
        c.date_of_birth,
        DATEDIFF(DAY, c.signup_date, GETDATE())                AS days_since_signup,
        DATEDIFF(YEAR, c.date_of_birth, GETDATE())             AS age,
        CASE
            WHEN c.status = 'active'  THEN 'Active'
            WHEN c.status = 'trial'   THEN 'Trial'
        END                                                     AS customer_status,
        c.preferred_currency
    FROM raw.customers c
    WHERE c.status IN ('active', 'trial')
      AND c.email IS NOT NULL
      AND c.signup_date >= '2018-01-01'
),

-- ─── CTE 2: category_hierarchy (recursive) ───────────────────────────────
-- Build full category path using recursive CTE
category_hierarchy AS (
    SELECT
        cat.category_id,
        cat.category_name,
        cat.parent_category_id,
        CAST(cat.category_name AS VARCHAR(500)) AS full_path,
        1                                        AS depth
    FROM raw.product_categories cat
    WHERE cat.parent_category_id IS NULL

    UNION ALL

    SELECT
        child.category_id,
        child.category_name,
        child.parent_category_id,
        CAST(ch.full_path + ' > ' + child.category_name AS VARCHAR(500)) AS full_path,
        ch.depth + 1                                                      AS depth
    FROM raw.product_categories child
    INNER JOIN category_hierarchy ch
        ON child.parent_category_id = ch.category_id
    WHERE ch.depth < 5
),

-- ─── CTE 3: enriched_products ─────────────────────────────────────────────
-- Join products with full category path
enriched_products AS (
    SELECT
        p.product_id,
        p.product_name,
        p.sku,
        p.unit_price,
        p.cost_price,
        (p.unit_price - p.cost_price)                              AS margin,
        CASE
            WHEN (p.unit_price - p.cost_price) / NULLIF(p.unit_price, 0) > 0.5
                THEN 'High Margin'
            WHEN (p.unit_price - p.cost_price) / NULLIF(p.unit_price, 0) > 0.25
                THEN 'Medium Margin'
            ELSE 'Low Margin'
        END                                                         AS margin_category,
        p.category_id,
        ch.category_name                                            AS leaf_category,
        ch.full_path                                                AS category_path,
        ch.depth                                                    AS category_depth,
        p.launch_date,
        DATEDIFF(MONTH, p.launch_date, GETDATE())                  AS months_since_launch,
        p.weight_kg,
        p.is_digital
    FROM raw.products p
    LEFT JOIN category_hierarchy ch ON p.category_id = ch.category_id
),

-- ─── CTE 4: exchange_rates_latest ─────────────────────────────────────────
-- Get latest exchange rate per currency pair
exchange_rates_latest AS (
    SELECT
        er.from_currency,
        er.to_currency,
        er.rate,
        er.rate_date
    FROM (
        SELECT
            from_currency,
            to_currency,
            rate,
            rate_date,
            ROW_NUMBER() OVER (
                PARTITION BY from_currency, to_currency
                ORDER BY rate_date DESC
            ) AS rn
        FROM raw.exchange_rates
    ) er
    WHERE er.rn = 1
),

-- ─── CTE 5: transactions_normalized ──────────────────────────────────────
-- Normalize all transactions to USD using exchange rates
transactions_normalized AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        t.product_id,
        t.order_date,
        t.quantity,
        t.unit_price_local,
        t.currency,
        t.discount_amount,
        t.tax_amount,
        COALESCE(er.rate, 1.0)                                      AS exchange_rate,
        t.unit_price_local * t.quantity * COALESCE(er.rate, 1.0)    AS gross_amount_usd,
        (t.unit_price_local * t.quantity - t.discount_amount)
            * COALESCE(er.rate, 1.0)                                AS net_amount_usd,
        t.tax_amount * COALESCE(er.rate, 1.0)                       AS tax_amount_usd,
        t.payment_method,
        t.channel,
        CASE
            WHEN t.channel = 'web'     THEN 'Online'
            WHEN t.channel = 'mobile'  THEN 'Online'
            WHEN t.channel = 'store'   THEN 'Offline'
            WHEN t.channel = 'phone'   THEN 'Offline'
            ELSE 'Other'
        END                                                          AS channel_group,
        t.store_id,
        t.status AS transaction_status
    FROM raw.transactions t
    LEFT JOIN exchange_rates_latest er
        ON t.currency = er.from_currency
        AND er.to_currency = 'USD'
    WHERE t.status NOT IN ('cancelled', 'fraudulent')
      AND t.order_date >= '2018-01-01'
),

-- ─── CTE 6: returns_summary ──────────────────────────────────────────────
-- Aggregate returns per customer-product
returns_summary AS (
    SELECT
        r.customer_id,
        r.product_id,
        COUNT(*)                                  AS return_count,
        SUM(r.refund_amount)                      AS total_refund,
        MAX(r.return_date)                        AS last_return_date,
        STRING_AGG(r.return_reason, '; ')         AS return_reasons
    FROM raw.returns r
    GROUP BY r.customer_id, r.product_id
),

-- ─── CTE 7: customer_transactions ─────────────────────────────────────────
-- Aggregate transaction metrics per customer
customer_transactions AS (
    SELECT
        tn.customer_id,
        COUNT(DISTINCT tn.transaction_id)                        AS total_orders,
        COUNT(DISTINCT tn.product_id)                            AS distinct_products,
        SUM(tn.net_amount_usd)                                   AS lifetime_revenue,
        SUM(tn.gross_amount_usd)                                 AS lifetime_gross,
        SUM(tn.tax_amount_usd)                                   AS lifetime_tax,
        AVG(tn.net_amount_usd)                                   AS avg_order_value,
        MIN(tn.order_date)                                       AS first_order_date,
        MAX(tn.order_date)                                       AS last_order_date,
        DATEDIFF(DAY, MAX(tn.order_date), GETDATE())             AS days_since_last_order,
        SUM(tn.quantity)                                         AS total_items_purchased,
        SUM(CASE WHEN tn.channel_group = 'Online' THEN 1 ELSE 0 END) AS online_orders,
        SUM(CASE WHEN tn.channel_group = 'Offline' THEN 1 ELSE 0 END) AS offline_orders,
        COUNT(DISTINCT tn.payment_method)                        AS payment_methods_used,
        -- Median approximation using PERCENTILE_CONT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tn.net_amount_usd)
            OVER (PARTITION BY tn.customer_id)                   AS median_order_value
    FROM transactions_normalized tn
    GROUP BY tn.customer_id
),

-- ─── CTE 8: customer_returns ──────────────────────────────────────────────
-- Aggregate return metrics per customer
customer_returns AS (
    SELECT
        rs.customer_id,
        SUM(rs.return_count)                     AS total_returns,
        SUM(rs.total_refund)                     AS total_refund_amount,
        MAX(rs.last_return_date)                 AS last_return_date,
        COUNT(DISTINCT rs.product_id)            AS products_returned
    FROM returns_summary rs
    GROUP BY rs.customer_id
),

-- ─── CTE 9: customer_web_activity ─────────────────────────────────────────
-- Aggregate web engagement metrics
customer_web_activity AS (
    SELECT
        we.customer_id,
        COUNT(*)                                                  AS total_events,
        COUNT(DISTINCT we.session_id)                             AS total_sessions,
        SUM(CASE WHEN we.event_type = 'page_view' THEN 1 ELSE 0 END)   AS page_views,
        SUM(CASE WHEN we.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
        SUM(CASE WHEN we.event_type = 'search' THEN 1 ELSE 0 END)      AS search_events,
        SUM(CASE WHEN we.event_type = 'wishlist' THEN 1 ELSE 0 END)    AS wishlist_events,
        AVG(we.time_on_page_seconds)                              AS avg_time_on_page,
        MAX(we.event_timestamp)                                   AS last_activity,
        DATEDIFF(DAY, MAX(we.event_timestamp), GETDATE())        AS days_since_last_activity,
        -- Bounce rate: sessions with only 1 event / total sessions
        CAST(
            SUM(CASE WHEN we.session_events = 1 THEN 1 ELSE 0 END) AS FLOAT
        ) / NULLIF(COUNT(DISTINCT we.session_id), 0)             AS bounce_rate
    FROM (
        SELECT
            we_inner.*,
            COUNT(*) OVER (PARTITION BY we_inner.session_id) AS session_events
        FROM raw.web_events we_inner
    ) we
    WHERE we.customer_id IS NOT NULL
    GROUP BY we.customer_id
),

-- ─── CTE 10: top_products_per_customer ─────────────────────────────────────
-- Rank products by revenue per customer, keep top 3
top_products_per_customer AS (
    SELECT
        tn.customer_id,
        tn.product_id,
        ep.product_name,
        ep.leaf_category,
        SUM(tn.net_amount_usd)                                   AS product_revenue,
        SUM(tn.quantity)                                         AS product_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY tn.customer_id
            ORDER BY SUM(tn.net_amount_usd) DESC
        )                                                        AS product_rank
    FROM transactions_normalized tn
    INNER JOIN enriched_products ep ON tn.product_id = ep.product_id
    GROUP BY tn.customer_id, tn.product_id, ep.product_name, ep.leaf_category
),

-- ─── CTE 11: top_categories_per_customer ───────────────────────────────────
-- Rank categories by spend per customer
top_categories_per_customer AS (
    SELECT
        tn.customer_id,
        ep.leaf_category,
        ep.category_path,
        SUM(tn.net_amount_usd)                                   AS category_spend,
        COUNT(DISTINCT tn.transaction_id)                        AS category_orders,
        ROW_NUMBER() OVER (
            PARTITION BY tn.customer_id
            ORDER BY SUM(tn.net_amount_usd) DESC
        )                                                        AS category_rank
    FROM transactions_normalized tn
    INNER JOIN enriched_products ep ON tn.product_id = ep.product_id
    WHERE ep.leaf_category IS NOT NULL
    GROUP BY tn.customer_id, ep.leaf_category, ep.category_path
),

-- ─── CTE 12: promotion_usage ──────────────────────────────────────────────
-- Which promotions each customer used
promotion_usage AS (
    SELECT
        tn.customer_id,
        p.promo_id,
        p.promo_name,
        p.promo_type,
        COUNT(DISTINCT tn.transaction_id)                        AS times_used,
        SUM(tn.discount_amount * COALESCE(tn.exchange_rate, 1))  AS total_discount_usd
    FROM transactions_normalized tn
    INNER JOIN raw.promotions p
        ON tn.order_date BETWEEN p.start_date AND p.end_date
        AND (p.product_id IS NULL OR p.product_id = tn.product_id)
    WHERE tn.discount_amount > 0
    GROUP BY tn.customer_id, p.promo_id, p.promo_name, p.promo_type
),

-- ─── CTE 13: customer_promo_summary ───────────────────────────────────────
-- Summarize promotion behaviour per customer
customer_promo_summary AS (
    SELECT
        pu.customer_id,
        COUNT(DISTINCT pu.promo_id)                              AS promotions_used,
        SUM(pu.total_discount_usd)                               AS total_promo_savings,
        MAX(pu.promo_type)                                       AS last_promo_type,
        STRING_AGG(DISTINCT pu.promo_name, ', ')                 AS promo_names
    FROM promotion_usage pu
    GROUP BY pu.customer_id
),

-- ─── CTE 14: shipping_metrics ─────────────────────────────────────────────
-- Shipping performance per customer
shipping_metrics AS (
    SELECT
        s.customer_id,
        COUNT(*)                                                 AS total_shipments,
        AVG(DATEDIFF(DAY, s.ship_date, s.delivery_date))        AS avg_delivery_days,
        MIN(DATEDIFF(DAY, s.ship_date, s.delivery_date))        AS fastest_delivery,
        MAX(DATEDIFF(DAY, s.ship_date, s.delivery_date))        AS slowest_delivery,
        SUM(CASE WHEN s.delivery_status = 'late' THEN 1 ELSE 0 END) AS late_deliveries,
        SUM(s.shipping_cost)                                     AS total_shipping_cost,
        -- Preferred shipping method (mode)
        (SELECT TOP 1 s2.shipping_method
         FROM raw.shipping s2
         WHERE s2.customer_id = s.customer_id
         GROUP BY s2.shipping_method
         ORDER BY COUNT(*) DESC)                                 AS preferred_shipping_method
    FROM raw.shipping s
    GROUP BY s.customer_id
),

-- ─── CTE 15: monthly_spend ────────────────────────────────────────────────
-- Monthly time series of customer spend
monthly_spend AS (
    SELECT
        tn.customer_id,
        DATEFROMPARTS(YEAR(tn.order_date), MONTH(tn.order_date), 1) AS month_start,
        SUM(tn.net_amount_usd)                                      AS monthly_revenue,
        COUNT(DISTINCT tn.transaction_id)                           AS monthly_orders
    FROM transactions_normalized tn
    GROUP BY tn.customer_id,
             DATEFROMPARTS(YEAR(tn.order_date), MONTH(tn.order_date), 1)
),

-- ─── CTE 16: spend_trend ─────────────────────────────────────────────────
-- Compute MoM growth and moving averages
spend_trend AS (
    SELECT
        ms.customer_id,
        ms.month_start,
        ms.monthly_revenue,
        ms.monthly_orders,
        LAG(ms.monthly_revenue, 1) OVER (
            PARTITION BY ms.customer_id ORDER BY ms.month_start
        )                                                         AS prev_month_revenue,
        LEAD(ms.monthly_revenue, 1) OVER (
            PARTITION BY ms.customer_id ORDER BY ms.month_start
        )                                                         AS next_month_revenue,
        AVG(ms.monthly_revenue) OVER (
            PARTITION BY ms.customer_id
            ORDER BY ms.month_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                                         AS rolling_3m_avg,
        SUM(ms.monthly_revenue) OVER (
            PARTITION BY ms.customer_id
            ORDER BY ms.month_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        )                                                         AS rolling_6m_total,
        NTILE(4) OVER (
            PARTITION BY ms.customer_id
            ORDER BY ms.monthly_revenue
        )                                                         AS spend_quartile
    FROM monthly_spend ms
),

-- ─── CTE 17: latest_spend_trend ───────────────────────────────────────────
-- Get the most recent trend metrics per customer
latest_spend_trend AS (
    SELECT
        st.customer_id,
        st.monthly_revenue                                        AS latest_month_revenue,
        st.rolling_3m_avg                                         AS latest_3m_avg,
        st.rolling_6m_total                                       AS latest_6m_total,
        CASE
            WHEN st.prev_month_revenue IS NULL THEN NULL
            WHEN st.prev_month_revenue = 0     THEN NULL
            ELSE (st.monthly_revenue - st.prev_month_revenue)
                 / st.prev_month_revenue * 100.0
        END                                                       AS mom_growth_pct,
        st.spend_quartile
    FROM spend_trend st
    WHERE st.month_start = (
        SELECT MAX(st2.month_start)
        FROM spend_trend st2
        WHERE st2.customer_id = st.customer_id
    )
),

-- ─── CTE 18: rfm_scores ──────────────────────────────────────────────────
-- Recency, Frequency, Monetary scoring
rfm_scores AS (
    SELECT
        ct.customer_id,
        ct.days_since_last_order,
        ct.total_orders,
        ct.lifetime_revenue,
        NTILE(5) OVER (ORDER BY ct.days_since_last_order ASC)   AS recency_score,
        NTILE(5) OVER (ORDER BY ct.total_orders DESC)           AS frequency_score,
        NTILE(5) OVER (ORDER BY ct.lifetime_revenue DESC)       AS monetary_score
    FROM customer_transactions ct
),

-- ─── CTE 19: rfm_segment ─────────────────────────────────────────────────
-- Map RFM scores to named segments
rfm_segment AS (
    SELECT
        rfm.customer_id,
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        rfm.recency_score + rfm.frequency_score + rfm.monetary_score AS rfm_total,
        CASE
            WHEN rfm.recency_score >= 4 AND rfm.frequency_score >= 4 AND rfm.monetary_score >= 4
                THEN 'Champion'
            WHEN rfm.recency_score >= 3 AND rfm.frequency_score >= 3 AND rfm.monetary_score >= 3
                THEN 'Loyal'
            WHEN rfm.recency_score >= 4 AND rfm.frequency_score <= 2
                THEN 'New Customer'
            WHEN rfm.recency_score <= 2 AND rfm.frequency_score >= 3 AND rfm.monetary_score >= 3
                THEN 'At Risk'
            WHEN rfm.recency_score <= 2 AND rfm.frequency_score <= 2 AND rfm.monetary_score <= 2
                THEN 'Lost'
            WHEN rfm.recency_score >= 3 AND rfm.monetary_score >= 4
                THEN 'Big Spender'
            ELSE 'Regular'
        END                                                       AS rfm_segment_name
    FROM rfm_scores rfm
),

-- ─── CTE 20: customer_tier_calc ───────────────────────────────────────────
-- Business-rule tier assignment with multiple criteria
customer_tier_calc AS (
    SELECT
        ct.customer_id,
        ct.lifetime_revenue,
        ct.total_orders,
        ct.avg_order_value,
        CASE
            WHEN ct.lifetime_revenue >= 50000 AND ct.total_orders >= 50
                THEN 'Platinum'
            WHEN ct.lifetime_revenue >= 20000 AND ct.total_orders >= 20
                THEN 'Gold'
            WHEN ct.lifetime_revenue >= 5000 AND ct.total_orders >= 5
                THEN 'Silver'
            ELSE 'Bronze'
        END                                                       AS customer_tier,
        RANK() OVER (ORDER BY ct.lifetime_revenue DESC)          AS revenue_rank,
        PERCENT_RANK() OVER (ORDER BY ct.lifetime_revenue DESC)  AS revenue_percentile,
        CASE
            WHEN ct.days_since_last_order <= 30  THEN 'Active'
            WHEN ct.days_since_last_order <= 90  THEN 'Warm'
            WHEN ct.days_since_last_order <= 180 THEN 'Cooling'
            ELSE 'Dormant'
        END                                                       AS engagement_status
    FROM customer_transactions ct
),

-- ─── CTE 21: cross_sell_candidates ────────────────────────────────────────
-- Find frequently co-purchased categories a customer hasn't tried
cross_sell_candidates AS (
    SELECT
        ac.customer_id,
        tc.leaf_category                                          AS purchased_category,
        cp.co_category,
        cp.co_purchase_count
    FROM active_customers ac
    INNER JOIN top_categories_per_customer tc
        ON ac.customer_id = tc.customer_id AND tc.category_rank = 1
    INNER JOIN (
        SELECT
            ep1.leaf_category                                     AS base_category,
            ep2.leaf_category                                     AS co_category,
            COUNT(DISTINCT tn1.customer_id)                       AS co_purchase_count
        FROM transactions_normalized tn1
        INNER JOIN enriched_products ep1 ON tn1.product_id = ep1.product_id
        INNER JOIN transactions_normalized tn2
            ON tn1.customer_id = tn2.customer_id
            AND tn1.product_id <> tn2.product_id
        INNER JOIN enriched_products ep2 ON tn2.product_id = ep2.product_id
        WHERE ep1.leaf_category <> ep2.leaf_category
        GROUP BY ep1.leaf_category, ep2.leaf_category
        HAVING COUNT(DISTINCT tn1.customer_id) >= 10
    ) cp ON tc.leaf_category = cp.base_category
    WHERE NOT EXISTS (
        SELECT 1
        FROM transactions_normalized tn3
        INNER JOIN enriched_products ep3 ON tn3.product_id = ep3.product_id
        WHERE tn3.customer_id = ac.customer_id
          AND ep3.leaf_category = cp.co_category
    )
),

-- ─── CTE 22: cross_sell_agg ──────────────────────────────────────────────
-- Aggregate cross-sell recommendations
cross_sell_agg AS (
    SELECT
        csc.customer_id,
        STRING_AGG(csc.co_category, ', ')                        AS recommended_categories,
        COUNT(DISTINCT csc.co_category)                          AS num_recommendations
    FROM (
        SELECT
            customer_id,
            co_category,
            co_purchase_count,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY co_purchase_count DESC
            ) AS rec_rank
        FROM cross_sell_candidates
    ) csc
    WHERE csc.rec_rank <= 5
    GROUP BY csc.customer_id
),

-- ─── CTE 23: predicted_ltv ───────────────────────────────────────────────
-- Simple LTV prediction based on historical spend pattern
predicted_ltv AS (
    SELECT
        ct.customer_id,
        ct.lifetime_revenue,
        ct.total_orders,
        ct.avg_order_value,
        CASE
            WHEN ct.days_since_last_order > 365 THEN 0
            ELSE
                ct.avg_order_value
                * (ct.total_orders
                   / NULLIF(
                       DATEDIFF(MONTH, ct.first_order_date, ct.last_order_date), 0
                   ))
                * 12  -- annualize
                * 3   -- 3-year horizon
        END                                                       AS predicted_3yr_ltv,
        CASE
            WHEN ct.days_since_last_order > 365 THEN 'Churned'
            WHEN ct.avg_order_value * ct.total_orders > 20000     THEN 'High Value'
            WHEN ct.avg_order_value * ct.total_orders > 5000      THEN 'Medium Value'
            ELSE 'Low Value'
        END                                                       AS ltv_segment
    FROM customer_transactions ct
)

-- ═══════════════════════════════════════════════════════════════════════════
-- FINAL SELECT: Join all CTEs into the target analytics table
-- ═══════════════════════════════════════════════════════════════════════════

SELECT
    -- Customer identity
    ac.customer_id,
    ac.first_name + ' ' + ac.last_name                            AS full_name,
    ac.email,
    ac.phone,
    ac.country,
    ac.state,
    ac.city,
    ac.signup_date,
    ac.days_since_signup,
    ac.age,
    ac.customer_status,

    -- Transaction summary
    COALESCE(ct.total_orders, 0)                                   AS total_orders,
    COALESCE(ct.distinct_products, 0)                              AS distinct_products,
    COALESCE(ct.lifetime_revenue, 0)                               AS lifetime_revenue,
    COALESCE(ct.lifetime_gross, 0)                                 AS lifetime_gross,
    COALESCE(ct.lifetime_tax, 0)                                   AS lifetime_tax,
    COALESCE(ct.avg_order_value, 0)                                AS avg_order_value,
    ct.median_order_value,
    ct.first_order_date,
    ct.last_order_date,
    ct.days_since_last_order,
    ct.total_items_purchased,
    ct.online_orders,
    ct.offline_orders,
    CASE
        WHEN COALESCE(ct.total_orders, 0) = 0 THEN 0
        ELSE CAST(ct.online_orders AS FLOAT) / ct.total_orders * 100
    END                                                            AS online_pct,
    ct.payment_methods_used,

    -- Returns
    COALESCE(cr.total_returns, 0)                                  AS total_returns,
    COALESCE(cr.total_refund_amount, 0)                            AS total_refund_amount,
    CASE
        WHEN COALESCE(ct.total_orders, 0) = 0 THEN 0
        ELSE CAST(COALESCE(cr.total_returns, 0) AS FLOAT)
             / ct.total_orders * 100
    END                                                            AS return_rate_pct,
    cr.products_returned,

    -- Web engagement
    COALESCE(cwa.total_sessions, 0)                                AS total_sessions,
    COALESCE(cwa.page_views, 0)                                    AS page_views,
    COALESCE(cwa.add_to_cart_events, 0)                            AS add_to_cart_events,
    COALESCE(cwa.search_events, 0)                                 AS search_events,
    cwa.avg_time_on_page,
    cwa.days_since_last_activity,
    cwa.bounce_rate,

    -- Top products & categories
    tp1.product_name                                               AS top_product_1,
    tp1.product_revenue                                            AS top_product_1_revenue,
    tp2.product_name                                               AS top_product_2,
    tp2.product_revenue                                            AS top_product_2_revenue,
    tp3.product_name                                               AS top_product_3,
    tc1.leaf_category                                              AS top_category,
    tc1.category_spend                                             AS top_category_spend,

    -- Promotions
    COALESCE(cps.promotions_used, 0)                               AS promotions_used,
    COALESCE(cps.total_promo_savings, 0)                           AS total_promo_savings,
    cps.promo_names,

    -- Shipping
    sm.avg_delivery_days,
    sm.fastest_delivery,
    sm.slowest_delivery,
    sm.late_deliveries,
    sm.total_shipping_cost,
    sm.preferred_shipping_method,

    -- Spend trend
    lst.latest_month_revenue,
    lst.latest_3m_avg,
    lst.latest_6m_total,
    lst.mom_growth_pct,

    -- RFM
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    rfm.rfm_total,
    rfm.rfm_segment_name,

    -- Tier & engagement
    tier.customer_tier,
    tier.revenue_rank,
    tier.revenue_percentile,
    tier.engagement_status,

    -- LTV
    ltv.predicted_3yr_ltv,
    ltv.ltv_segment,

    -- Cross-sell
    csa.recommended_categories,
    csa.num_recommendations,

    -- Computed: Net Promoter proxy
    CASE
        WHEN rfm.rfm_segment_name IN ('Champion', 'Loyal', 'Big Spender')
            THEN 'Promoter'
        WHEN rfm.rfm_segment_name IN ('At Risk', 'Lost')
            THEN 'Detractor'
        ELSE 'Passive'
    END                                                            AS nps_proxy,

    -- Metadata
    GETDATE()                                                      AS etl_timestamp,
    'v2.3'                                                         AS model_version

INTO analytics.customer_360_summary

FROM active_customers ac

LEFT JOIN customer_transactions ct     ON ac.customer_id = ct.customer_id
LEFT JOIN customer_returns cr          ON ac.customer_id = cr.customer_id
LEFT JOIN customer_web_activity cwa    ON ac.customer_id = cwa.customer_id
LEFT JOIN top_products_per_customer tp1
    ON ac.customer_id = tp1.customer_id AND tp1.product_rank = 1
LEFT JOIN top_products_per_customer tp2
    ON ac.customer_id = tp2.customer_id AND tp2.product_rank = 2
LEFT JOIN top_products_per_customer tp3
    ON ac.customer_id = tp3.customer_id AND tp3.product_rank = 3
LEFT JOIN top_categories_per_customer tc1
    ON ac.customer_id = tc1.customer_id AND tc1.category_rank = 1
LEFT JOIN customer_promo_summary cps   ON ac.customer_id = cps.customer_id
LEFT JOIN shipping_metrics sm          ON ac.customer_id = sm.customer_id
LEFT JOIN latest_spend_trend lst       ON ac.customer_id = lst.customer_id
LEFT JOIN rfm_segment rfm              ON ac.customer_id = rfm.customer_id
LEFT JOIN customer_tier_calc tier      ON ac.customer_id = tier.customer_id
LEFT JOIN predicted_ltv ltv            ON ac.customer_id = ltv.customer_id
LEFT JOIN cross_sell_agg csa           ON ac.customer_id = csa.customer_id

ORDER BY COALESCE(ct.lifetime_revenue, 0) DESC;
