|** Ab-Initio graph: 04_enrich_orders.mp
|** Reads stg_orders and stg_customers (both written by upstream SQL files).
|** Enriches orders with customer segment and credit information.
|** Writes: dbo.enr_orders
**|

begin components

  component "Read_Stg_Orders" : "input-table"
    table    : "dbo.stg_orders"
    fields   : order_id, customer_id, order_date, raw_amount,
               currency_code, discount_pct, region_code, is_cancelled
  end component

  component "Read_Stg_Customers" : "input-table"
    table    : "dbo.stg_customers"
    fields   : customer_id, full_name, email, country_code,
               customer_segment, credit_limit, is_active
  end component

  component "Join_Orders_Customers" : "join"
    left     : Read_Stg_Orders
    right    : Read_Stg_Customers
    key      : customer_id = customer_id
    type     : LEFT OUTER
    output   :
      order_id,
      Read_Stg_Orders.customer_id,
      order_date,
      raw_amount,
      currency_code,
      discount_pct,
      region_code,
      is_cancelled,
      full_name          AS customer_name,
      country_code       AS customer_country,
      customer_segment,
      credit_limit,
      is_active          AS customer_is_active
  end component

  component "Transform_Net_Amount" : "transform"
    input    : Join_Orders_Customers
    output   :
      order_id,
      customer_id,
      order_date,
      raw_amount * (1 - discount_pct / 100.0)   AS discounted_amount,
      discount_pct,
      currency_code,
      region_code,
      is_cancelled,
      customer_name,
      customer_country,
      customer_segment,
      credit_limit,
      customer_is_active,
      CASE
        WHEN customer_segment = 'PREMIUM'  THEN raw_amount * 0.02
        WHEN customer_segment = 'STANDARD' THEN raw_amount * 0.01
        ELSE 0.0
      END                                        AS loyalty_bonus
  end component

  component "Filter_Active" : "filter"
    input    : Transform_Net_Amount
    condition: is_cancelled = 0 AND customer_is_active = 1
  end component

  component "Write_Enr_Orders" : "output-table"
    input    : Filter_Active
    table    : "dbo.enr_orders"
    fields   : order_id, customer_id, order_date, raw_amount,
               discounted_amount, discount_pct, currency_code,
               region_code, customer_name, customer_country,
               customer_segment, credit_limit, loyalty_bonus
  end component

end components

begin flow
  Read_Stg_Orders      --> Join_Orders_Customers
  Read_Stg_Customers   --> Join_Orders_Customers
  Join_Orders_Customers --> Transform_Net_Amount
  Transform_Net_Amount --> Filter_Active
  Filter_Active        --> Write_Enr_Orders
end flow
