!*sym*! customer_order_analytics
!*sym*! Author: etl_team
!*sym*! Description: Customer Order Analytics Pipeline
!*sym*!   Reads from two Oracle source tables and a CSV flat file,
!*sym*!   performs currency conversion lookups, product hierarchy enrichment,
!*sym*!   filtering, joining, aggregation, tier assignment, and dedup,
!*sym*!   then writes to a PostgreSQL analytics table and summary CSV.
!*sym*!
!*sym*! Parameters:
!*sym*!   $DATA_DATE      = @DIFFDATE(@TODAY(), 1, 'YYYYMMDD')
!*sym*!   $SRC_DB_CONN    = oracle_prod
!*sym*!   $TGT_DB_CONN    = postgres_analytics
!*sym*!   $LOOKUP_DIR     = /data/lookups
!*sym*!   $OUTPUT_DIR     = /data/output/${DATA_DATE}
!*sym*!   $PARALLELISM    = 4

type = graph
begin

  phase 0

  !*--- SOURCE COMPONENTS ---*!

  component read_orders
    type = input_table
    database = $SRC_DB_CONN
    table = sales.orders
    query = <<END_SQL
      SELECT
        order_id,
        customer_id,
        order_date,
        total_amount,
        discount_amount,
        tax_amount,
        shipping_cost,
        payment_method,
        order_status,
        channel,
        currency,
        store_id
      FROM sales.orders
      WHERE order_date = TO_DATE('${DATA_DATE}', 'YYYYMMDD')
        AND order_status NOT IN ('cancelled', 'fraudulent')
    END_SQL
    out :: out0
    layout = record
      field("order_id",        "integer")
      field("customer_id",     "integer")
      field("order_date",      "date(\"YYYY-MM-DD\")")
      field("total_amount",    "decimal(12,2)")
      field("discount_amount", "decimal(10,2)")
      field("tax_amount",      "decimal(10,2)")
      field("shipping_cost",   "decimal(8,2)")
      field("payment_method",  "string(30)")
      field("order_status",    "string(20)")
      field("channel",         "string(20)")
      field("currency",        "string(3)")
      field("store_id",        "integer" nullable)
    end
  end

  component read_customers
    type = input_table
    database = $SRC_DB_CONN
    table = crm.customers
    query = <<END_SQL
      SELECT
        customer_id,
        first_name,
        last_name,
        email,
        signup_date,
        country,
        region,
        segment,
        is_active
      FROM crm.customers
      WHERE is_active = 1
    END_SQL
    out :: out0
    layout = record
      field("customer_id", "integer")
      field("first_name",  "string(100)")
      field("last_name",   "string(100)")
      field("email",       "string(255)")
      field("signup_date", "date(\"YYYY-MM-DD\")")
      field("country",     "string(50)")
      field("region",      "string(50)")
      field("segment",     "string(30)")
      field("is_active",   "integer")
    end
  end

  component read_order_items
    type = input_file
    filename = "/data/feeds/${DATA_DATE}/order_items.csv"
    format = delimited
    delimiter = ","
    header = true
    reject_filename = "${OUTPUT_DIR}/order_items_rejects.csv"
    out :: out0
    layout = record
      field("item_id",      "integer")
      field("order_id",     "integer")
      field("product_id",   "integer")
      field("product_name", "string(200)")
      field("category",     "string(100)")
      field("quantity",     "integer")
      field("unit_price",   "decimal(10,2)")
      field("line_total",   "decimal(12,2)")
    end
  end

  !*--- LOOKUP COMPONENTS ---*!

  component lookup_exchange_rates
    type = lookup_file
    filename = "${LOOKUP_DIR}/exchange_rates.dat"
    key = currency_code
    in :: read_orders.out0
    out :: out0
    lookup_layout = record
      field("currency_code", "string(3)")  key
      field("usd_rate",      "decimal(10,6)")
      field("rate_date",     "date(\"YYYY-MM-DD\")")
    end
    transform = <<END_DML
      out.order_id        :: in.order_id;
      out.customer_id     :: in.customer_id;
      out.order_date      :: in.order_date;
      out.total_amount    :: in.total_amount;
      out.discount_amount :: in.discount_amount;
      out.tax_amount      :: in.tax_amount;
      out.shipping_cost   :: in.shipping_cost;
      out.payment_method  :: in.payment_method;
      out.order_status    :: in.order_status;
      out.channel         :: in.channel;
      out.currency        :: in.currency;
      out.store_id        :: in.store_id;
      out.usd_rate        :: lookup(in.currency, "currency_code").usd_rate;
      out.amount_usd      :: in.total_amount * lookup(in.currency, "currency_code").usd_rate;
      out.discount_usd    :: in.discount_amount * lookup(in.currency, "currency_code").usd_rate;
      out.tax_usd         :: in.tax_amount * lookup(in.currency, "currency_code").usd_rate;
      out.net_amount_usd  :: (in.total_amount - in.discount_amount + in.tax_amount)
                             * lookup(in.currency, "currency_code").usd_rate;
    END_DML
  end

  component lookup_product_hierarchy
    type = lookup_file
    filename = "${LOOKUP_DIR}/product_hierarchy.dat"
    key = product_id
    in :: read_order_items.out0
    out :: out0
    lookup_layout = record
      field("product_id",       "integer")  key
      field("department",       "string(100)")
      field("sub_category",     "string(100)")
      field("brand",            "string(100)")
      field("is_private_label", "integer")
      field("margin_pct",       "decimal(5,2)")
    end
    transform = <<END_DML
      out.item_id          :: in.item_id;
      out.order_id         :: in.order_id;
      out.product_id       :: in.product_id;
      out.product_name     :: in.product_name;
      out.category         :: in.category;
      out.quantity          :: in.quantity;
      out.unit_price        :: in.unit_price;
      out.line_total        :: in.line_total;
      out.department        :: lookup(in.product_id).department;
      out.sub_category      :: lookup(in.product_id).sub_category;
      out.brand             :: lookup(in.product_id).brand;
      out.is_private_label  :: lookup(in.product_id).is_private_label;
      out.margin_pct        :: lookup(in.product_id).margin_pct;
      out.estimated_margin  :: in.line_total * lookup(in.product_id).margin_pct / 100;
    END_DML
  end

  !*--- FILTER COMPONENTS ---*!

  component filter_valid_orders
    type = filter_by_expression
    in :: lookup_exchange_rates.out0
    accepted :: out_accepted
    rejected :: out_rejected
    expression = <<END_EXPR
      in.amount_usd > 0
      AND in.order_status IN ('confirmed', 'shipped', 'delivered')
      AND in.customer_id IS NOT NULL
    END_EXPR
  end

  component filter_high_value_items
    type = filter_by_expression
    in :: lookup_product_hierarchy.out0
    accepted :: out_accepted
    rejected :: out_rejected
    expression = <<END_EXPR
      in.quantity > 0 AND in.unit_price > 0
    END_EXPR
  end

  !*--- JOIN COMPONENTS ---*!

  component join_order_customer
    type = join
    join_type = inner
    join_key = customer_id
    left  :: filter_valid_orders.out_accepted
    right :: read_customers.out0
    out   :: out0
    transform = <<END_DML
      out.order_id        :: left.order_id;
      out.customer_id     :: left.customer_id;
      out.customer_name   :: string_concat(right.first_name, " ", right.last_name);
      out.email           :: right.email;
      out.country         :: right.country;
      out.region          :: right.region;
      out.segment         :: right.segment;
      out.order_date      :: left.order_date;
      out.amount_usd      :: left.amount_usd;
      out.discount_usd    :: left.discount_usd;
      out.tax_usd         :: left.tax_usd;
      out.net_amount_usd  :: left.net_amount_usd;
      out.payment_method  :: left.payment_method;
      out.channel         :: left.channel;
      out.store_id        :: left.store_id;
      out.days_since_signup :: date_difference(right.signup_date, left.order_date, "days");
    END_DML
  end

  component join_order_items
    type = join
    join_type = left_outer
    join_key = order_id
    left  :: join_order_customer.out0
    right :: filter_high_value_items.out_accepted
    out   :: out0
    transform = <<END_DML
      out.order_id        :: left.order_id;
      out.customer_id     :: left.customer_id;
      out.customer_name   :: left.customer_name;
      out.email           :: left.email;
      out.country         :: left.country;
      out.region          :: left.region;
      out.segment         :: left.segment;
      out.order_date      :: left.order_date;
      out.amount_usd      :: left.amount_usd;
      out.discount_usd    :: left.discount_usd;
      out.tax_usd         :: left.tax_usd;
      out.net_amount_usd  :: left.net_amount_usd;
      out.payment_method  :: left.payment_method;
      out.channel         :: left.channel;
      out.store_id        :: left.store_id;
      out.days_since_signup :: left.days_since_signup;
      out.product_id       :: right.product_id;
      out.product_name     :: right.product_name;
      out.category         :: right.category;
      out.department       :: right.department;
      out.sub_category     :: right.sub_category;
      out.brand            :: right.brand;
      out.quantity         :: right.quantity;
      out.unit_price       :: right.unit_price;
      out.line_total       :: right.line_total;
      out.estimated_margin :: right.estimated_margin;
    END_DML
  end

  !*--- AGGREGATION ---*!

  component rollup_by_customer
    type = rollup
    key = customer_id
    sort_key = customer_id
    in :: join_order_items.out0
    out :: out0
    transform = <<END_DML
      out.customer_id       :: in.customer_id;
      out.customer_name     :: first(in.customer_name);
      out.email             :: first(in.email);
      out.country           :: first(in.country);
      out.region            :: first(in.region);
      out.segment           :: first(in.segment);
      out.total_orders      :: count_distinct(in.order_id);
      out.total_items       :: sum(in.quantity);
      out.gross_revenue     :: sum(in.amount_usd);
      out.total_discount    :: sum(in.discount_usd);
      out.total_tax         :: sum(in.tax_usd);
      out.net_revenue       :: sum(in.net_amount_usd);
      out.avg_order_value   :: mean(in.amount_usd);
      out.max_order_value   :: max(in.amount_usd);
      out.distinct_products :: count_distinct(in.product_id);
      out.top_category      :: mode(in.category);
      out.top_brand         :: mode(in.brand);
      out.total_margin      :: sum(in.estimated_margin);
      out.margin_pct        :: sum(in.estimated_margin) / if_null(sum(in.amount_usd), 1) * 100;
      out.first_order_date  :: min(in.order_date);
      out.last_order_date   :: max(in.order_date);
      out.days_since_signup :: first(in.days_since_signup);
    END_DML
  end

  !*--- REFORMAT (Tier Assignment + Scoring) ---*!

  component assign_customer_tier
    type = reformat
    in :: rollup_by_customer.out0
    out :: out0
    transform = <<END_DML
      out.customer_id       :: in.customer_id;
      out.customer_name     :: in.customer_name;
      out.email             :: in.email;
      out.country           :: in.country;
      out.region            :: in.region;
      out.segment           :: in.segment;
      out.total_orders      :: in.total_orders;
      out.total_items       :: in.total_items;
      out.gross_revenue     :: in.gross_revenue;
      out.total_discount    :: in.total_discount;
      out.total_tax         :: in.total_tax;
      out.net_revenue       :: in.net_revenue;
      out.avg_order_value   :: in.avg_order_value;
      out.max_order_value   :: in.max_order_value;
      out.distinct_products :: in.distinct_products;
      out.top_category      :: in.top_category;
      out.top_brand         :: in.top_brand;
      out.total_margin      :: in.total_margin;
      out.margin_pct        :: in.margin_pct;
      out.first_order_date  :: in.first_order_date;
      out.last_order_date   :: in.last_order_date;
      out.days_since_signup :: in.days_since_signup;
      out.customer_tier     ::
        if in.net_revenue >= 50000 and in.total_orders >= 50 then "Platinum"
        else if in.net_revenue >= 20000 and in.total_orders >= 20 then "Gold"
        else if in.net_revenue >= 5000 and in.total_orders >= 5 then "Silver"
        else "Bronze";
      out.engagement_score  ::
        (if in.total_orders >= 20 then 30
         else if in.total_orders >= 10 then 20
         else 10)
        + (if in.distinct_products >= 15 then 20
           else if in.distinct_products >= 5 then 10
           else 5)
        + (if in.days_since_signup >= 365 then 25
           else if in.days_since_signup >= 180 then 15
           else 10)
        + (if in.margin_pct >= 30 then 25
           else if in.margin_pct >= 15 then 15
           else 5);
      out.etl_date :: today();
    END_DML
  end

  !*--- DEDUP & PARTITION ---*!

  component dedup_customers
    type = dedup_sorted
    key = customer_id
    keep = first
    in :: assign_customer_tier.out0
    unique :: out_unique
    duplicates :: out_dupes
  end

  component partition_output
    type = partition_by_roundrobin
    num_partitions = $PARALLELISM
    in :: dedup_customers.out_unique
    out :: out0
  end

  !*--- OUTPUT COMPONENTS ---*!

  component write_customer_analytics
    type = output_table
    database = $TGT_DB_CONN
    table = analytics.customer_daily_summary
    write_mode = merge
    merge_key = "customer_id, etl_date"
    in :: partition_output.out0
    layout = record
      field("customer_id",       "integer")
      field("customer_name",     "string(201)")
      field("email",             "string(255)")
      field("country",           "string(50)")
      field("region",            "string(50)")
      field("segment",           "string(30)")
      field("total_orders",      "integer")
      field("total_items",       "integer")
      field("gross_revenue",     "decimal(14,2)")
      field("total_discount",    "decimal(12,2)")
      field("total_tax",         "decimal(12,2)")
      field("net_revenue",       "decimal(14,2)")
      field("avg_order_value",   "decimal(10,2)")
      field("max_order_value",   "decimal(12,2)")
      field("distinct_products", "integer")
      field("top_category",      "string(100)")
      field("top_brand",         "string(100)")
      field("total_margin",      "decimal(12,2)")
      field("margin_pct",        "decimal(5,2)")
      field("customer_tier",     "string(20)")
      field("engagement_score",  "integer")
      field("first_order_date",  "date(\"YYYY-MM-DD\")")
      field("last_order_date",   "date(\"YYYY-MM-DD\")")
      field("etl_date",          "date(\"YYYY-MM-DD\")")
    end
  end

  component write_summary_file
    type = output_file
    filename = "${OUTPUT_DIR}/customer_summary_${DATA_DATE}.csv"
    format = delimited
    delimiter = ","
    header = true
    in :: dedup_customers.out_unique
    layout = record
      field("customer_id")
      field("customer_name")
      field("region")
      field("customer_tier")
      field("total_orders")
      field("net_revenue")
      field("engagement_score")
      field("etl_date")
    end
  end

  !*--- FLOW CONNECTIONS ---*!

  flow read_orders.out0                      -> lookup_exchange_rates.in
  flow lookup_exchange_rates.out0            -> filter_valid_orders.in
  flow read_order_items.out0                 -> lookup_product_hierarchy.in
  flow lookup_product_hierarchy.out0         -> filter_high_value_items.in
  flow filter_valid_orders.out_accepted      -> join_order_customer.left
  flow read_customers.out0                   -> join_order_customer.right
  flow join_order_customer.out0              -> join_order_items.left
  flow filter_high_value_items.out_accepted  -> join_order_items.right
  flow join_order_items.out0                 -> rollup_by_customer.in
  flow rollup_by_customer.out0               -> assign_customer_tier.in
  flow assign_customer_tier.out0             -> dedup_customers.in
  flow dedup_customers.out_unique            -> partition_output.in
  flow partition_output.out0                 -> write_customer_analytics.in
  flow dedup_customers.out_unique            -> write_summary_file.in

end
