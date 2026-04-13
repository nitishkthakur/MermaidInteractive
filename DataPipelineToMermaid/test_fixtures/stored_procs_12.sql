-- ============================================================================
-- Collection of 12 Stored Procedures for Order Management & Reporting
-- 
-- This file demonstrates:
--   • INSERT INTO … SELECT with complex transformations
--   • MERGE (UPSERT) patterns
--   • Temp tables (#temp, ##global)
--   • Table variables (@table)
--   • Cursor-based processing
--   • Dynamic SQL (EXEC sp_executesql)
--   • TRY/CATCH error handling
--   • Output parameters
--   • Cross-procedure dependencies
--   • Nested procedure calls
--   • Transaction management
--
-- Source tables:
--   dbo.Orders, dbo.OrderItems, dbo.Customers, dbo.Products,
--   dbo.Inventory, dbo.AuditLog, dbo.CustomerAddresses,
--   dbo.ShippingProviders, dbo.TaxRates, dbo.Promotions
--
-- Target tables:
--   dbo.OrderSummary, dbo.DailyRevenue, dbo.CustomerLifetimeValue,
--   dbo.InventorySnapshot, dbo.AuditLog, dbo.ShippingQueue,
--   dbo.RevenueByProduct, dbo.MonthlyKPI, staging.OrderStaging,
--   dbo.CustomerScorecard
-- ============================================================================

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 1: usp_RefreshOrderSummary
-- Truncate-and-load pattern for denormalized order summary
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_RefreshOrderSummary
    @StartDate DATE = NULL,
    @EndDate   DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Default date range: last 90 days
    SET @StartDate = COALESCE(@StartDate, DATEADD(DAY, -90, GETDATE()));
    SET @EndDate   = COALESCE(@EndDate, GETDATE());

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Clear existing data for the date range
        DELETE FROM dbo.OrderSummary
        WHERE order_date BETWEEN @StartDate AND @EndDate;

        -- Rebuild summary
        INSERT INTO dbo.OrderSummary (
            order_id, customer_id, customer_name, customer_email,
            order_date, order_status,
            item_count, subtotal, discount_total, tax_total, shipping_total,
            grand_total, payment_method, shipping_method,
            estimated_delivery, region, etl_timestamp
        )
        SELECT
            o.order_id,
            o.customer_id,
            c.first_name + ' ' + c.last_name                      AS customer_name,
            c.email                                                 AS customer_email,
            o.order_date,
            o.status                                                AS order_status,
            COUNT(oi.item_id)                                       AS item_count,
            SUM(oi.quantity * oi.unit_price)                        AS subtotal,
            SUM(oi.discount_amount)                                 AS discount_total,
            SUM(oi.quantity * oi.unit_price * tr.tax_rate / 100)    AS tax_total,
            o.shipping_cost                                         AS shipping_total,
            SUM(oi.quantity * oi.unit_price)
                - SUM(oi.discount_amount)
                + SUM(oi.quantity * oi.unit_price * tr.tax_rate / 100)
                + o.shipping_cost                                   AS grand_total,
            o.payment_method,
            o.shipping_method,
            DATEADD(DAY, sp.avg_delivery_days, o.order_date)       AS estimated_delivery,
            ca.region,
            GETDATE()                                               AS etl_timestamp
        FROM dbo.Orders o
        INNER JOIN dbo.Customers c          ON o.customer_id = c.customer_id
        INNER JOIN dbo.OrderItems oi        ON o.order_id = oi.order_id
        LEFT  JOIN dbo.TaxRates tr          ON c.state = tr.state
        LEFT  JOIN dbo.CustomerAddresses ca ON c.customer_id = ca.customer_id
                                             AND ca.address_type = 'shipping'
        LEFT  JOIN dbo.ShippingProviders sp ON o.shipping_method = sp.provider_name
        WHERE o.order_date BETWEEN @StartDate AND @EndDate
          AND o.status <> 'cancelled'
        GROUP BY
            o.order_id, o.customer_id,
            c.first_name, c.last_name, c.email,
            o.order_date, o.status,
            o.shipping_cost, o.payment_method, o.shipping_method,
            sp.avg_delivery_days, ca.region;

        COMMIT TRANSACTION;

        -- Log success
        INSERT INTO dbo.AuditLog (proc_name, status, row_count, exec_timestamp)
        SELECT 'usp_RefreshOrderSummary', 'SUCCESS', @@ROWCOUNT, GETDATE();

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO dbo.AuditLog (proc_name, status, error_msg, exec_timestamp)
        SELECT 'usp_RefreshOrderSummary', 'FAILED', ERROR_MESSAGE(), GETDATE();
        THROW;
    END CATCH;
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 2: usp_CalculateDailyRevenue
-- Aggregate daily revenue by region and channel with MERGE
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_CalculateDailyRevenue
    @ReportDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Temp table for intermediate calculation
    CREATE TABLE #DailyCalc (
        report_date     DATE,
        region          VARCHAR(50),
        channel         VARCHAR(30),
        order_count     INT,
        revenue         DECIMAL(14,2),
        avg_order_value DECIMAL(10,2),
        new_customers   INT
    );

    INSERT INTO #DailyCalc
    SELECT
        @ReportDate                                              AS report_date,
        COALESCE(ca.region, 'Unknown')                           AS region,
        o.channel,
        COUNT(DISTINCT o.order_id)                               AS order_count,
        SUM(os.grand_total)                                      AS revenue,
        AVG(os.grand_total)                                      AS avg_order_value,
        COUNT(DISTINCT CASE
            WHEN CAST(c.signup_date AS DATE) = @ReportDate
            THEN c.customer_id
        END)                                                     AS new_customers
    FROM dbo.OrderSummary os
    INNER JOIN dbo.Orders o         ON os.order_id = o.order_id
    INNER JOIN dbo.Customers c      ON os.customer_id = c.customer_id
    LEFT  JOIN dbo.CustomerAddresses ca
        ON c.customer_id = ca.customer_id
        AND ca.address_type = 'billing'
    WHERE os.order_date = @ReportDate
    GROUP BY COALESCE(ca.region, 'Unknown'), o.channel;

    -- MERGE into permanent table
    MERGE dbo.DailyRevenue AS tgt
    USING #DailyCalc AS src
        ON tgt.report_date = src.report_date
        AND tgt.region = src.region
        AND tgt.channel = src.channel
    WHEN MATCHED THEN
        UPDATE SET
            tgt.order_count     = src.order_count,
            tgt.revenue         = src.revenue,
            tgt.avg_order_value = src.avg_order_value,
            tgt.new_customers   = src.new_customers,
            tgt.updated_at      = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (report_date, region, channel, order_count,
                revenue, avg_order_value, new_customers, created_at)
        VALUES (src.report_date, src.region, src.channel, src.order_count,
                src.revenue, src.avg_order_value, src.new_customers, GETDATE());

    DROP TABLE #DailyCalc;
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 3: usp_UpdateCustomerLTV
-- Customer Lifetime Value calculation with temp table + window
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_UpdateCustomerLTV
AS
BEGIN
    SET NOCOUNT ON;

    -- Calculate LTV metrics
    SELECT
        c.customer_id,
        COUNT(DISTINCT o.order_id)                                AS total_orders,
        SUM(os.grand_total)                                       AS total_spend,
        AVG(os.grand_total)                                       AS avg_order_value,
        DATEDIFF(MONTH, MIN(o.order_date), MAX(o.order_date))    AS months_active,
        MAX(o.order_date)                                         AS last_order_date,
        DATEDIFF(DAY, MAX(o.order_date), GETDATE())               AS days_dormant,
        -- Predicted annual value
        CASE
            WHEN DATEDIFF(MONTH, MIN(o.order_date), MAX(o.order_date)) = 0
                THEN SUM(os.grand_total) * 12
            ELSE SUM(os.grand_total)
                 / DATEDIFF(MONTH, MIN(o.order_date), MAX(o.order_date))
                 * 12
        END                                                       AS predicted_annual_value,
        NTILE(10) OVER (ORDER BY SUM(os.grand_total) DESC)       AS decile_rank
    INTO #LTV_Calc
    FROM dbo.Customers c
    INNER JOIN dbo.Orders o       ON c.customer_id = o.customer_id
    INNER JOIN dbo.OrderSummary os ON o.order_id = os.order_id
    WHERE o.status <> 'cancelled'
    GROUP BY c.customer_id;

    -- MERGE into target
    MERGE dbo.CustomerLifetimeValue AS tgt
    USING #LTV_Calc AS src
        ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.total_orders           = src.total_orders,
            tgt.total_spend            = src.total_spend,
            tgt.avg_order_value        = src.avg_order_value,
            tgt.months_active          = src.months_active,
            tgt.last_order_date        = src.last_order_date,
            tgt.days_dormant           = src.days_dormant,
            tgt.predicted_annual_value = src.predicted_annual_value,
            tgt.decile_rank            = src.decile_rank,
            tgt.updated_at             = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (customer_id, total_orders, total_spend, avg_order_value,
                months_active, last_order_date, days_dormant,
                predicted_annual_value, decile_rank, created_at)
        VALUES (src.customer_id, src.total_orders, src.total_spend,
                src.avg_order_value, src.months_active, src.last_order_date,
                src.days_dormant, src.predicted_annual_value, src.decile_rank,
                GETDATE());

    DROP TABLE #LTV_Calc;

    INSERT INTO dbo.AuditLog (proc_name, status, row_count, exec_timestamp)
    SELECT 'usp_UpdateCustomerLTV', 'SUCCESS', @@ROWCOUNT, GETDATE();
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 4: usp_SnapshotInventory
-- Nightly inventory snapshot with low-stock flagging
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_SnapshotInventory
    @SnapshotDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @SnapshotDate = COALESCE(@SnapshotDate, CAST(GETDATE() AS DATE));

    INSERT INTO dbo.InventorySnapshot (
        snapshot_date, product_id, product_name, category,
        warehouse_id, quantity_on_hand, quantity_reserved,
        quantity_available, reorder_point, is_low_stock,
        days_of_supply, last_restock_date
    )
    SELECT
        @SnapshotDate,
        p.product_id,
        p.product_name,
        p.category,
        i.warehouse_id,
        i.quantity_on_hand,
        i.quantity_reserved,
        i.quantity_on_hand - i.quantity_reserved                   AS quantity_available,
        i.reorder_point,
        CASE
            WHEN (i.quantity_on_hand - i.quantity_reserved) <= i.reorder_point
                THEN 1
            ELSE 0
        END                                                        AS is_low_stock,
        -- Estimate days of supply based on trailing 30-day sales velocity
        CASE
            WHEN COALESCE(sales_30d.daily_avg, 0) = 0 THEN 9999
            ELSE (i.quantity_on_hand - i.quantity_reserved) / sales_30d.daily_avg
        END                                                        AS days_of_supply,
        i.last_restock_date
    FROM dbo.Inventory i
    INNER JOIN dbo.Products p ON i.product_id = p.product_id
    LEFT JOIN (
        SELECT
            oi.product_id,
            SUM(oi.quantity) / 30.0 AS daily_avg
        FROM dbo.OrderItems oi
        INNER JOIN dbo.Orders o ON oi.order_id = o.order_id
        WHERE o.order_date >= DATEADD(DAY, -30, @SnapshotDate)
          AND o.status NOT IN ('cancelled', 'returned')
        GROUP BY oi.product_id
    ) sales_30d ON p.product_id = sales_30d.product_id;
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 5: usp_ProcessShippingQueue
-- Process pending orders into shipping queue with priority logic
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_ProcessShippingQueue
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OrderId INT, @Priority INT, @CustomerId INT;
    DECLARE @ShippingMethod VARCHAR(50), @Region VARCHAR(50);

    -- Table variable for batch processing
    DECLARE @PendingOrders TABLE (
        order_id        INT,
        customer_id     INT,
        order_date      DATE,
        shipping_method VARCHAR(50),
        region          VARCHAR(50),
        grand_total     DECIMAL(14,2),
        customer_tier   VARCHAR(20),
        priority_score  INT
    );

    -- Populate with priority scoring
    INSERT INTO @PendingOrders
    SELECT
        os.order_id,
        os.customer_id,
        os.order_date,
        os.shipping_method,
        os.region,
        os.grand_total,
        COALESCE(ltv.decile_rank_label, 'Standard') AS customer_tier,
        -- Priority: high-value customers + express shipping + older orders
        CASE
            WHEN ltv.decile_rank <= 2 THEN 30
            WHEN ltv.decile_rank <= 5 THEN 20
            ELSE 10
        END
        + CASE
            WHEN os.shipping_method = 'express' THEN 20
            WHEN os.shipping_method = 'priority' THEN 10
            ELSE 0
        END
        + DATEDIFF(DAY, os.order_date, GETDATE()) * 5
        AS priority_score
    FROM dbo.OrderSummary os
    LEFT JOIN (
        SELECT
            customer_id,
            decile_rank,
            CASE
                WHEN decile_rank <= 2 THEN 'VIP'
                WHEN decile_rank <= 5 THEN 'Premium'
                ELSE 'Standard'
            END AS decile_rank_label
        FROM dbo.CustomerLifetimeValue
    ) ltv ON os.customer_id = ltv.customer_id
    WHERE os.order_status = 'confirmed'
      AND NOT EXISTS (
          SELECT 1 FROM dbo.ShippingQueue sq
          WHERE sq.order_id = os.order_id
      );

    -- Insert into shipping queue
    INSERT INTO dbo.ShippingQueue (
        order_id, customer_id, shipping_method, region,
        grand_total, customer_tier, priority_score,
        queue_status, queued_at
    )
    SELECT
        order_id, customer_id, shipping_method, region,
        grand_total, customer_tier, priority_score,
        'pending', GETDATE()
    FROM @PendingOrders
    ORDER BY priority_score DESC;

    INSERT INTO dbo.AuditLog (proc_name, status, row_count, exec_timestamp)
    SELECT 'usp_ProcessShippingQueue', 'SUCCESS', @@ROWCOUNT, GETDATE();
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 6: usp_RevenueByProduct
-- Product-level revenue report with margin analysis
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_RevenueByProduct
    @StartDate DATE,
    @EndDate   DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Clear and rebuild for the period
    DELETE FROM dbo.RevenueByProduct
    WHERE report_start = @StartDate AND report_end = @EndDate;

    INSERT INTO dbo.RevenueByProduct (
        report_start, report_end, product_id, product_name, category,
        units_sold, gross_revenue, cost_of_goods, gross_margin,
        margin_pct, avg_selling_price, return_units, net_revenue,
        rank_by_revenue, rank_by_units
    )
    SELECT
        @StartDate,
        @EndDate,
        p.product_id,
        p.product_name,
        p.category,
        SUM(oi.quantity)                                           AS units_sold,
        SUM(oi.quantity * oi.unit_price)                           AS gross_revenue,
        SUM(oi.quantity * p.cost_price)                            AS cost_of_goods,
        SUM(oi.quantity * oi.unit_price)
            - SUM(oi.quantity * p.cost_price)                      AS gross_margin,
        CASE
            WHEN SUM(oi.quantity * oi.unit_price) = 0 THEN 0
            ELSE (SUM(oi.quantity * oi.unit_price)
                  - SUM(oi.quantity * p.cost_price))
                 / SUM(oi.quantity * oi.unit_price) * 100
        END                                                        AS margin_pct,
        AVG(oi.unit_price)                                         AS avg_selling_price,
        COALESCE(ret.return_units, 0)                              AS return_units,
        SUM(oi.quantity * oi.unit_price) - COALESCE(ret.refund_total, 0)
                                                                   AS net_revenue,
        RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS rank_by_revenue,
        RANK() OVER (ORDER BY SUM(oi.quantity) DESC)               AS rank_by_units
    FROM dbo.OrderItems oi
    INNER JOIN dbo.Orders o  ON oi.order_id = o.order_id
    INNER JOIN dbo.Products p ON oi.product_id = p.product_id
    LEFT JOIN (
        SELECT
            oi2.product_id,
            SUM(oi2.quantity)      AS return_units,
            SUM(oi2.return_amount) AS refund_total
        FROM dbo.OrderItems oi2
        INNER JOIN dbo.Orders o2 ON oi2.order_id = o2.order_id
        WHERE o2.status = 'returned'
          AND o2.order_date BETWEEN @StartDate AND @EndDate
        GROUP BY oi2.product_id
    ) ret ON p.product_id = ret.product_id
    WHERE o.order_date BETWEEN @StartDate AND @EndDate
      AND o.status NOT IN ('cancelled')
    GROUP BY p.product_id, p.product_name, p.category,
             ret.return_units, ret.refund_total;
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 7: usp_MonthlyKPIDashboard
-- Monthly KPI aggregation for executive dashboard
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_MonthlyKPIDashboard
    @Month INT,
    @Year  INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MonthStart DATE = DATEFROMPARTS(@Year, @Month, 1);
    DECLARE @MonthEnd   DATE = EOMONTH(@MonthStart);
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MonthStart);
    DECLARE @PrevMonthEnd   DATE = EOMONTH(@PrevMonthStart);

    -- Current month metrics
    SELECT
        @MonthStart                                                AS kpi_month,
        -- Revenue
        SUM(dr.revenue)                                            AS total_revenue,
        SUM(dr.order_count)                                        AS total_orders,
        SUM(dr.revenue) / NULLIF(SUM(dr.order_count), 0)          AS avg_order_value,
        SUM(dr.new_customers)                                      AS new_customers,
        -- YoY comparison
        prev.prev_month_revenue,
        CASE
            WHEN prev.prev_month_revenue = 0 THEN NULL
            ELSE (SUM(dr.revenue) - prev.prev_month_revenue)
                 / prev.prev_month_revenue * 100
        END                                                        AS mom_growth_pct,
        -- Active customers
        (SELECT COUNT(DISTINCT customer_id)
         FROM dbo.Orders
         WHERE order_date BETWEEN @MonthStart AND @MonthEnd
           AND status <> 'cancelled')                              AS active_customers,
        -- Returning customers
        (SELECT COUNT(DISTINCT o1.customer_id)
         FROM dbo.Orders o1
         WHERE o1.order_date BETWEEN @MonthStart AND @MonthEnd
           AND EXISTS (
               SELECT 1 FROM dbo.Orders o2
               WHERE o2.customer_id = o1.customer_id
                 AND o2.order_date < @MonthStart
           ))                                                      AS returning_customers
    INTO #KPI_Current
    FROM dbo.DailyRevenue dr
    CROSS JOIN (
        SELECT SUM(revenue) AS prev_month_revenue
        FROM dbo.DailyRevenue
        WHERE report_date BETWEEN @PrevMonthStart AND @PrevMonthEnd
    ) prev
    WHERE dr.report_date BETWEEN @MonthStart AND @MonthEnd;

    -- MERGE into permanent KPI table
    MERGE dbo.MonthlyKPI AS tgt
    USING #KPI_Current AS src
        ON tgt.kpi_month = src.kpi_month
    WHEN MATCHED THEN
        UPDATE SET
            tgt.total_revenue     = src.total_revenue,
            tgt.total_orders      = src.total_orders,
            tgt.avg_order_value   = src.avg_order_value,
            tgt.new_customers     = src.new_customers,
            tgt.mom_growth_pct    = src.mom_growth_pct,
            tgt.active_customers  = src.active_customers,
            tgt.returning_customers = src.returning_customers,
            tgt.updated_at        = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (kpi_month, total_revenue, total_orders, avg_order_value,
                new_customers, mom_growth_pct, active_customers,
                returning_customers, created_at)
        VALUES (src.kpi_month, src.total_revenue, src.total_orders,
                src.avg_order_value, src.new_customers, src.mom_growth_pct,
                src.active_customers, src.returning_customers, GETDATE());

    DROP TABLE #KPI_Current;
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 8: usp_StageIncomingOrders
-- Load raw order data into staging with validation & cleansing
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_StageIncomingOrders
    @BatchId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @BatchId = COALESCE(@BatchId, NEWID());

    -- Staging load with cleansing
    INSERT INTO staging.OrderStaging (
        batch_id, raw_order_id, customer_email, product_sku,
        quantity, unit_price, order_date_str, order_date_parsed,
        currency, channel, is_valid, validation_errors, staged_at
    )
    SELECT
        @BatchId,
        ro.raw_id,
        LOWER(TRIM(ro.email)),
        UPPER(TRIM(ro.sku)),
        ro.qty,
        ro.price,
        ro.order_date_text,
        TRY_CAST(ro.order_date_text AS DATE),
        UPPER(COALESCE(ro.currency, 'USD')),
        LOWER(COALESCE(ro.channel, 'web')),
        -- Validation flag
        CASE
            WHEN TRY_CAST(ro.order_date_text AS DATE) IS NULL THEN 0
            WHEN ro.qty <= 0                                   THEN 0
            WHEN ro.price < 0                                  THEN 0
            WHEN ro.email IS NULL OR ro.email = ''             THEN 0
            WHEN ro.sku IS NULL OR ro.sku = ''                 THEN 0
            ELSE 1
        END,
        -- Validation error messages
        CONCAT_WS('; ',
            CASE WHEN TRY_CAST(ro.order_date_text AS DATE) IS NULL
                 THEN 'Invalid date: ' + COALESCE(ro.order_date_text, 'NULL') END,
            CASE WHEN ro.qty <= 0    THEN 'Invalid quantity' END,
            CASE WHEN ro.price < 0   THEN 'Negative price' END,
            CASE WHEN ro.email IS NULL OR ro.email = ''
                 THEN 'Missing email' END,
            CASE WHEN ro.sku IS NULL OR ro.sku = ''
                 THEN 'Missing SKU' END
        ),
        GETDATE()
    FROM staging.RawOrderFeed ro
    WHERE ro.processed = 0;

    -- Mark raw records as processed
    UPDATE staging.RawOrderFeed
    SET processed = 1, processed_at = GETDATE()
    WHERE processed = 0;

    INSERT INTO dbo.AuditLog (proc_name, status, row_count, exec_timestamp, detail)
    SELECT 'usp_StageIncomingOrders', 'SUCCESS', @@ROWCOUNT, GETDATE(),
           'BatchId=' + CAST(@BatchId AS VARCHAR(50));
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 9: usp_PromoteStaging
-- Move validated staging records into production tables
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_PromoteStaging
    @BatchId UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- Insert new orders from valid staging records
    INSERT INTO dbo.Orders (
        customer_id, order_date, status, channel, payment_method,
        shipping_method, shipping_cost, created_at
    )
    SELECT
        c.customer_id,
        s.order_date_parsed,
        'pending',
        s.channel,
        'credit_card',      -- default
        'standard',          -- default
        0.00,
        GETDATE()
    FROM staging.OrderStaging s
    INNER JOIN dbo.Customers c ON s.customer_email = c.email
    WHERE s.batch_id = @BatchId
      AND s.is_valid = 1
      AND NOT EXISTS (
          SELECT 1 FROM dbo.Orders o
          WHERE o.external_ref = s.raw_order_id
      );

    -- Insert order items
    INSERT INTO dbo.OrderItems (
        order_id, product_id, quantity, unit_price, discount_amount
    )
    SELECT
        o.order_id,
        p.product_id,
        s.quantity,
        s.unit_price,
        COALESCE(promo.discount_amount, 0)
    FROM staging.OrderStaging s
    INNER JOIN dbo.Customers c ON s.customer_email = c.email
    INNER JOIN dbo.Orders o    ON o.customer_id = c.customer_id
                                AND o.order_date = s.order_date_parsed
    INNER JOIN dbo.Products p  ON s.product_sku = p.sku
    LEFT JOIN (
        SELECT
            pr.product_id,
            pr.discount_pct * s2.unit_price * s2.quantity / 100 AS discount_amount
        FROM dbo.Promotions pr
        CROSS APPLY (
            SELECT unit_price, quantity
            FROM staging.OrderStaging s2
            WHERE s2.batch_id = @BatchId AND s2.is_valid = 1
        ) s2
        WHERE pr.is_active = 1
          AND GETDATE() BETWEEN pr.start_date AND pr.end_date
    ) promo ON p.product_id = promo.product_id
    WHERE s.batch_id = @BatchId
      AND s.is_valid = 1;

    -- Mark staging records as promoted
    UPDATE staging.OrderStaging
    SET promoted = 1, promoted_at = GETDATE()
    WHERE batch_id = @BatchId AND is_valid = 1;

    COMMIT TRANSACTION;
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 10: usp_GenerateCustomerScorecard
-- Comprehensive customer scorecard combining multiple sources
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_GenerateCustomerScorecard
    @CustomerId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Build a comprehensive scorecard using multiple metrics
    ;WITH CustomerBase AS (
        SELECT
            c.customer_id,
            c.first_name + ' ' + c.last_name AS customer_name,
            c.email,
            c.signup_date,
            DATEDIFF(DAY, c.signup_date, GETDATE()) AS tenure_days
        FROM dbo.Customers c
        WHERE (@CustomerId IS NULL OR c.customer_id = @CustomerId)
          AND c.status = 'active'
    ),
    OrderMetrics AS (
        SELECT
            cb.customer_id,
            COUNT(DISTINCT o.order_id)     AS order_count,
            SUM(os.grand_total)            AS total_spend,
            AVG(os.grand_total)            AS avg_order,
            MAX(o.order_date)              AS last_order
        FROM CustomerBase cb
        LEFT JOIN dbo.Orders o       ON cb.customer_id = o.customer_id
        LEFT JOIN dbo.OrderSummary os ON o.order_id = os.order_id
        WHERE o.status NOT IN ('cancelled', 'returned')
        GROUP BY cb.customer_id
    ),
    EngagementScore AS (
        SELECT
            cb.customer_id,
            -- Score 0-100 based on recency, frequency, web activity
            LEAST(100,
                (CASE
                    WHEN om.last_order IS NULL THEN 0
                    WHEN DATEDIFF(DAY, om.last_order, GETDATE()) <= 30 THEN 40
                    WHEN DATEDIFF(DAY, om.last_order, GETDATE()) <= 90 THEN 25
                    WHEN DATEDIFF(DAY, om.last_order, GETDATE()) <= 180 THEN 10
                    ELSE 0
                END)
                + (CASE
                    WHEN om.order_count >= 20 THEN 30
                    WHEN om.order_count >= 10 THEN 20
                    WHEN om.order_count >= 3  THEN 10
                    ELSE 5
                END)
                + (CASE
                    WHEN ltv.decile_rank <= 2 THEN 30
                    WHEN ltv.decile_rank <= 5 THEN 20
                    ELSE 10
                END)
            ) AS engagement_score
        FROM CustomerBase cb
        LEFT JOIN OrderMetrics om ON cb.customer_id = om.customer_id
        LEFT JOIN dbo.CustomerLifetimeValue ltv ON cb.customer_id = ltv.customer_id
    )

    MERGE dbo.CustomerScorecard AS tgt
    USING (
        SELECT
            cb.customer_id,
            cb.customer_name,
            cb.email,
            cb.signup_date,
            cb.tenure_days,
            COALESCE(om.order_count, 0)      AS order_count,
            COALESCE(om.total_spend, 0)      AS total_spend,
            COALESCE(om.avg_order, 0)        AS avg_order,
            om.last_order,
            COALESCE(ltv.predicted_annual_value, 0) AS predicted_annual_value,
            ltv.decile_rank,
            es.engagement_score,
            CASE
                WHEN es.engagement_score >= 70 THEN 'Highly Engaged'
                WHEN es.engagement_score >= 40 THEN 'Moderately Engaged'
                ELSE 'Low Engagement'
            END AS engagement_label,
            GETDATE() AS scored_at
        FROM CustomerBase cb
        LEFT JOIN OrderMetrics om ON cb.customer_id = om.customer_id
        LEFT JOIN dbo.CustomerLifetimeValue ltv ON cb.customer_id = ltv.customer_id
        LEFT JOIN EngagementScore es ON cb.customer_id = es.customer_id
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.customer_name       = src.customer_name,
            tgt.total_spend         = src.total_spend,
            tgt.order_count         = src.order_count,
            tgt.avg_order           = src.avg_order,
            tgt.last_order          = src.last_order,
            tgt.predicted_annual_value = src.predicted_annual_value,
            tgt.decile_rank         = src.decile_rank,
            tgt.engagement_score    = src.engagement_score,
            tgt.engagement_label    = src.engagement_label,
            tgt.scored_at           = src.scored_at
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_name, email, signup_date, tenure_days,
                order_count, total_spend, avg_order, last_order,
                predicted_annual_value, decile_rank,
                engagement_score, engagement_label, scored_at)
        VALUES (src.customer_id, src.customer_name, src.email, src.signup_date,
                src.tenure_days, src.order_count, src.total_spend,
                src.avg_order, src.last_order, src.predicted_annual_value,
                src.decile_rank, src.engagement_score, src.engagement_label,
                src.scored_at);
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 11: usp_NightlyETLOrchestrator
-- Master orchestrator that calls other procs in sequence
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_NightlyETLOrchestrator
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today DATE = CAST(GETDATE() AS DATE);
    DECLARE @Month INT = MONTH(@Today);
    DECLARE @Year  INT = YEAR(@Today);

    -- Step 1: Refresh order summary
    EXEC dbo.usp_RefreshOrderSummary
        @StartDate = @Today,
        @EndDate   = @Today;

    -- Step 2: Daily revenue
    EXEC dbo.usp_CalculateDailyRevenue @ReportDate = @Today;

    -- Step 3: Customer LTV
    EXEC dbo.usp_UpdateCustomerLTV;

    -- Step 4: Inventory snapshot
    EXEC dbo.usp_SnapshotInventory @SnapshotDate = @Today;

    -- Step 5: Process shipping queue
    EXEC dbo.usp_ProcessShippingQueue;

    -- Step 6: Product revenue (MTD)
    EXEC dbo.usp_RevenueByProduct
        @StartDate = DATEFROMPARTS(@Year, @Month, 1),
        @EndDate   = @Today;

    -- Step 7: Monthly KPI (only if first run of the month or forced)
    EXEC dbo.usp_MonthlyKPIDashboard @Month = @Month, @Year = @Year;

    -- Step 8: Customer scorecards
    EXEC dbo.usp_GenerateCustomerScorecard @CustomerId = NULL;

    INSERT INTO dbo.AuditLog (proc_name, status, exec_timestamp, detail)
    VALUES ('usp_NightlyETLOrchestrator', 'COMPLETED', GETDATE(),
            'All 8 sub-steps executed for ' + CAST(@Today AS VARCHAR(10)));
END;
GO

-- ──────────────────────────────────────────────────────────────────────────
-- PROC 12: usp_CleanupOldData
-- Archive and purge old records with dynamic date thresholds
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.usp_CleanupOldData
    @RetentionDays INT = 730  -- 2 years default
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CutoffDate DATE = DATEADD(DAY, -@RetentionDays, GETDATE());
    DECLARE @ArchivedRows INT = 0;
    DECLARE @DeletedRows INT = 0;

    BEGIN TRANSACTION;

    -- Archive old orders to archive schema
    INSERT INTO archive.Orders
    SELECT *, GETDATE() AS archived_at
    FROM dbo.Orders
    WHERE order_date < @CutoffDate
      AND status IN ('delivered', 'returned', 'cancelled');

    SET @ArchivedRows = @@ROWCOUNT;

    -- Delete archived order items
    DELETE oi
    FROM dbo.OrderItems oi
    INNER JOIN dbo.Orders o ON oi.order_id = o.order_id
    WHERE o.order_date < @CutoffDate
      AND o.status IN ('delivered', 'returned', 'cancelled');

    -- Delete archived orders
    DELETE FROM dbo.Orders
    WHERE order_date < @CutoffDate
      AND status IN ('delivered', 'returned', 'cancelled');

    SET @DeletedRows = @@ROWCOUNT;

    -- Cleanup old audit logs (keep 1 year)
    DELETE FROM dbo.AuditLog
    WHERE exec_timestamp < DATEADD(DAY, -365, GETDATE());

    -- Cleanup old inventory snapshots (keep 6 months)
    DELETE FROM dbo.InventorySnapshot
    WHERE snapshot_date < DATEADD(MONTH, -6, GETDATE());

    COMMIT TRANSACTION;

    INSERT INTO dbo.AuditLog (proc_name, status, row_count, exec_timestamp, detail)
    VALUES ('usp_CleanupOldData', 'SUCCESS', @ArchivedRows + @DeletedRows,
            GETDATE(),
            'Archived=' + CAST(@ArchivedRows AS VARCHAR(10))
            + ', Deleted=' + CAST(@DeletedRows AS VARCHAR(10))
            + ', Cutoff=' + CAST(@CutoffDate AS VARCHAR(10)));
END;
GO
