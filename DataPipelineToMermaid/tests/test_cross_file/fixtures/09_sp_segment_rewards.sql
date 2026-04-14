-- 09_sp_segment_rewards.sql
-- Stored Procedure: dbo.sp_calculate_segment_rewards
-- Reads: dbo.customer_loyalty_tiers  (from 08_sp_loyalty_tiers.sql)
--        dbo.customer_risk_scores     (from 06_customer_risk.mp)
-- Writes: dbo.segment_rewards
--
-- Combines loyalty tier data with risk scores to assign reward budgets
-- and promotional eligibility per customer segment.
-- This proc is downstream of BOTH 08 (loyalty) AND 06 (risk), making
-- it the convergence point of the loyalty and risk branches.

CREATE OR ALTER PROCEDURE dbo.sp_calculate_segment_rewards
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dbo.segment_rewards;

    INSERT INTO dbo.segment_rewards (
        customer_id,
        loyalty_tier,
        risk_tier,
        reward_budget_usd,
        promo_eligible,
        reward_multiplier,
        net_reward_value
    )
    WITH base AS (
        SELECT
            lt.customer_id,
            lt.loyalty_tier,
            lt.loyalty_points,
            lt.lifetime_spend,
            COALESCE(rs.risk_tier, 'NORMAL')        AS risk_tier,
            COALESCE(rs.risk_score, 0)              AS risk_score
        FROM dbo.customer_loyalty_tiers lt
        LEFT JOIN dbo.customer_risk_scores rs
               ON rs.customer_id = lt.customer_id
    ),
    reward_calc AS (
        SELECT
            customer_id,
            loyalty_tier,
            risk_tier,
            -- Base reward budget by loyalty tier
            CASE loyalty_tier
                WHEN 'PLATINUM' THEN 500.00
                WHEN 'GOLD'     THEN 200.00
                WHEN 'SILVER'   THEN 75.00
                ELSE 20.00
            END                                     AS reward_budget_usd,
            -- Not eligible if CRITICAL risk, regardless of loyalty
            CASE
                WHEN risk_tier = 'CRITICAL' THEN 0
                WHEN risk_tier = 'ELEVATED' AND loyalty_tier = 'BRONZE' THEN 0
                ELSE 1
            END                                     AS promo_eligible,
            -- Multiplier: loyalty drives up, risk drives down
            ROUND(
                CASE loyalty_tier
                    WHEN 'PLATINUM' THEN 2.5
                    WHEN 'GOLD'     THEN 1.8
                    WHEN 'SILVER'   THEN 1.2
                    ELSE 1.0
                END
                *
                CASE risk_tier
                    WHEN 'CRITICAL' THEN 0.0
                    WHEN 'ELEVATED' THEN 0.5
                    WHEN 'WATCH'    THEN 0.8
                    ELSE 1.0
                END,
                2
            )                                       AS reward_multiplier
        FROM base
    )
    SELECT
        customer_id,
        loyalty_tier,
        risk_tier,
        reward_budget_usd,
        promo_eligible,
        reward_multiplier,
        -- net_reward_value: what the customer actually receives
        ROUND(reward_budget_usd * reward_multiplier, 2) AS net_reward_value
    FROM reward_calc;

END;
GO

EXEC dbo.sp_calculate_segment_rewards;
