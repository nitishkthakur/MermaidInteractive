|** Ab-Initio graph: 06_customer_risk.mp
|** Reads dbo.reconciled_orders (output of 05_reconcile_payments.sql).
|** Aggregates order-level risk into a customer-level risk score.
|** The "risk_score" column is computed here for the first time.
|** Writes: dbo.customer_risk_scores
**|

begin components

  component "Read_Reconciled" : "input-table"
    table  : "dbo.reconciled_orders"
    fields : order_id, customer_id, order_date, customer_segment,
             region_code, net_order_revenue, payment_gap,
             risk_flag, payment_status
  end component

  component "Rollup_By_Customer" : "rollup"
    input      : Read_Reconciled
    group_by   : customer_id, customer_segment, region_code
    aggregates :
      COUNT(order_id)                                    AS total_orders,
      SUM(net_order_revenue)                             AS lifetime_revenue,
      SUM(CASE WHEN risk_flag = 'HIGH'   THEN 1 ELSE 0 END) AS high_risk_count,
      SUM(CASE WHEN risk_flag = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_count,
      AVG(payment_gap)                                   AS avg_payment_gap,
      MAX(order_date)                                    AS last_order_date
  end component

  component "Compute_Risk_Score" : "transform"
    input  : Rollup_By_Customer
    output :
      customer_id,
      customer_segment,
      region_code,
      total_orders,
      lifetime_revenue,
      last_order_date,
      avg_payment_gap,
      high_risk_count,
      medium_risk_count,
      -- risk_score: weighted composite, 0-100 scale
      LEAST(100,
        (high_risk_count   * 30) +
        (medium_risk_count * 10) +
        CASE WHEN avg_payment_gap > 1000 THEN 20
             WHEN avg_payment_gap > 200  THEN 10
             ELSE 0 END
      )                                                  AS risk_score,
      -- risk_tier derived from risk_score
      CASE
        WHEN risk_score >= 60 THEN 'CRITICAL'
        WHEN risk_score >= 30 THEN 'ELEVATED'
        WHEN risk_score >= 10 THEN 'WATCH'
        ELSE 'NORMAL'
      END                                                AS risk_tier
  end component

  component "Write_Risk_Scores" : "output-table"
    input  : Compute_Risk_Score
    table  : "dbo.customer_risk_scores"
    fields : customer_id, customer_segment, region_code,
             total_orders, lifetime_revenue, last_order_date,
             avg_payment_gap, high_risk_count, medium_risk_count,
             risk_score, risk_tier
  end component

end components

begin flow
  Read_Reconciled   --> Rollup_By_Customer
  Rollup_By_Customer --> Compute_Risk_Score
  Compute_Risk_Score --> Write_Risk_Scores
end flow
