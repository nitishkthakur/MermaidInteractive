"""
Pandas ETL Pipeline — E-Commerce Analytics
============================================

Reads from multiple CSV/Parquet sources, performs complex transformations
including joins, aggregations, window functions, pivot tables, and writes
to multiple targets (Parquet, CSV, PostgreSQL).

Source datasets:
  - data/orders.csv
  - data/order_items.csv
  - data/customers.parquet
  - data/products.parquet
  - data/exchange_rates.csv
  - data/web_sessions.csv

Target datasets:
  - output/customer_360.parquet
  - output/product_performance.parquet
  - output/daily_kpi.csv
  - postgresql://analytics_db/public.customer_summary
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────────

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
DB_URI = "postgresql://etl_user:password@localhost:5432/analytics_db"


# ═══════════════════════════════════════════════════════════════════
# Step 1: Extract — Read source data
# ═══════════════════════════════════════════════════════════════════


def extract_orders() -> pd.DataFrame:
    """Read orders CSV with type coercion and date parsing."""
    df = pd.read_csv(
        DATA_DIR / "orders.csv",
        parse_dates=["order_date", "ship_date"],
        dtype={
            "order_id": "int64",
            "customer_id": "int64",
            "store_id": "Int64",  # nullable int
            "total_amount": "float64",
            "discount_amount": "float64",
            "tax_amount": "float64",
            "shipping_cost": "float64",
            "payment_method": "string",
            "channel": "string",
            "currency": "string",
            "status": "string",
        },
    )
    logger.info("Extracted %d orders", len(df))
    return df


def extract_order_items() -> pd.DataFrame:
    """Read order items CSV."""
    df = pd.read_csv(
        DATA_DIR / "order_items.csv",
        dtype={
            "item_id": "int64",
            "order_id": "int64",
            "product_id": "int64",
            "quantity": "int64",
            "unit_price": "float64",
            "line_total": "float64",
        },
    )
    logger.info("Extracted %d order items", len(df))
    return df


def extract_customers() -> pd.DataFrame:
    """Read customers Parquet."""
    df = pd.read_parquet(DATA_DIR / "customers.parquet")
    logger.info("Extracted %d customers", len(df))
    return df


def extract_products() -> pd.DataFrame:
    """Read products Parquet."""
    df = pd.read_parquet(DATA_DIR / "products.parquet")
    logger.info("Extracted %d products", len(df))
    return df


def extract_exchange_rates() -> pd.DataFrame:
    """Read daily exchange rates."""
    df = pd.read_csv(
        DATA_DIR / "exchange_rates.csv",
        parse_dates=["rate_date"],
        dtype={
            "from_currency": "string",
            "to_currency": "string",
            "rate": "float64",
        },
    )
    logger.info("Extracted %d exchange rate records", len(df))
    return df


def extract_web_sessions() -> pd.DataFrame:
    """Read web session clickstream data."""
    df = pd.read_csv(
        DATA_DIR / "web_sessions.csv",
        parse_dates=["session_start", "session_end"],
        dtype={
            "session_id": "string",
            "customer_id": "Int64",
            "page_views": "int64",
            "events": "int64",
            "device_type": "string",
            "referrer": "string",
            "converted": "boolean",
        },
    )
    logger.info("Extracted %d web sessions", len(df))
    return df


# ═══════════════════════════════════════════════════════════════════
# Step 2: Transform — Clean, enrich, aggregate
# ═══════════════════════════════════════════════════════════════════


def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Filter cancelled/fraudulent orders, normalize channels."""
    df = orders.copy()

    # Filter out bad statuses
    df = df[~df["status"].isin(["cancelled", "fraudulent"])]

    # Normalize channel names
    channel_map = {
        "web": "Online",
        "mobile": "Online",
        "app": "Online",
        "store": "Offline",
        "phone": "Offline",
        "catalog": "Offline",
    }
    df["channel_group"] = df["channel"].str.lower().map(channel_map).fillna("Other")

    # Flag high-value orders
    df["is_high_value"] = df["total_amount"] > df["total_amount"].quantile(0.9)

    logger.info("Cleaned orders: %d rows (removed %d)", len(df), len(orders) - len(df))
    return df


def normalize_currency(
    orders: pd.DataFrame, fx_rates: pd.DataFrame
) -> pd.DataFrame:
    """Convert all monetary amounts to USD using latest exchange rates."""
    # Get latest rate per currency
    latest_fx = (
        fx_rates[fx_rates["to_currency"] == "USD"]
        .sort_values("rate_date")
        .drop_duplicates(subset=["from_currency"], keep="last")
        .set_index("from_currency")["rate"]
    )

    df = orders.copy()
    df["fx_rate"] = df["currency"].map(latest_fx).fillna(1.0)

    # Convert amounts
    for col in ["total_amount", "discount_amount", "tax_amount", "shipping_cost"]:
        df[f"{col}_usd"] = df[col] * df["fx_rate"]

    # Net amount
    df["net_amount_usd"] = (
        df["total_amount_usd"]
        - df["discount_amount_usd"]
        + df["tax_amount_usd"]
    )

    logger.info("Currency normalized: %d rows", len(df))
    return df


def enrich_order_items(
    items: pd.DataFrame, products: pd.DataFrame
) -> pd.DataFrame:
    """Join order items with product details for category & margin info."""
    df = items.merge(
        products[["product_id", "product_name", "category", "sub_category",
                  "brand", "cost_price", "is_digital"]],
        on="product_id",
        how="left",
    )

    # Calculate margin
    df["margin"] = df["line_total"] - (df["cost_price"] * df["quantity"])
    df["margin_pct"] = (df["margin"] / df["line_total"].replace(0, np.nan) * 100).fillna(0)

    # Margin band
    df["margin_band"] = pd.cut(
        df["margin_pct"],
        bins=[-np.inf, 0, 15, 30, 50, np.inf],
        labels=["Negative", "Low", "Medium", "High", "Premium"],
    )

    logger.info("Enriched order items: %d rows, %d columns", *df.shape)
    return df


def build_customer_360(
    orders: pd.DataFrame,
    items_enriched: pd.DataFrame,
    customers: pd.DataFrame,
    web_sessions: pd.DataFrame,
) -> pd.DataFrame:
    """Build comprehensive customer-level analytics dataset."""

    # ── Transaction aggregations ───────────────────────────────
    order_agg = (
        orders.groupby("customer_id")
        .agg(
            total_orders=("order_id", "nunique"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
            lifetime_revenue=("net_amount_usd", "sum"),
            avg_order_value=("net_amount_usd", "mean"),
            total_discount_usd=("discount_amount_usd", "sum"),
            total_tax_usd=("tax_amount_usd", "sum"),
            online_orders=("channel_group", lambda x: (x == "Online").sum()),
            offline_orders=("channel_group", lambda x: (x == "Offline").sum()),
            payment_methods=("payment_method", "nunique"),
            high_value_orders=("is_high_value", "sum"),
        )
        .reset_index()
    )

    # Days since last order
    today = pd.Timestamp.now().normalize()
    order_agg["days_since_last_order"] = (
        (today - order_agg["last_order_date"]).dt.days
    )
    order_agg["online_pct"] = (
        order_agg["online_orders"]
        / order_agg["total_orders"].replace(0, np.nan)
        * 100
    ).fillna(0)

    # ── Product preferences ─────────────────────────────────────
    # Top category per customer
    cust_items = items_enriched.merge(
        orders[["order_id", "customer_id"]], on="order_id"
    )

    top_category = (
        cust_items.groupby(["customer_id", "category"])
        .agg(cat_spend=("line_total", "sum"))
        .reset_index()
        .sort_values(["customer_id", "cat_spend"], ascending=[True, False])
        .drop_duplicates(subset=["customer_id"], keep="first")
        .rename(columns={"category": "top_category", "cat_spend": "top_category_spend"})
        [["customer_id", "top_category", "top_category_spend"]]
    )

    # Top brand per customer
    top_brand = (
        cust_items.groupby(["customer_id", "brand"])
        .agg(brand_spend=("line_total", "sum"))
        .reset_index()
        .sort_values(["customer_id", "brand_spend"], ascending=[True, False])
        .drop_duplicates(subset=["customer_id"], keep="first")
        .rename(columns={"brand": "top_brand", "brand_spend": "top_brand_spend"})
        [["customer_id", "top_brand", "top_brand_spend"]]
    )

    # Distinct products count
    distinct_products = (
        cust_items.groupby("customer_id")["product_id"]
        .nunique()
        .reset_index()
        .rename(columns={"product_id": "distinct_products"})
    )

    # ── Web engagement ──────────────────────────────────────────
    web_agg = (
        web_sessions[web_sessions["customer_id"].notna()]
        .groupby("customer_id")
        .agg(
            total_sessions=("session_id", "nunique"),
            total_page_views=("page_views", "sum"),
            avg_page_views=("page_views", "mean"),
            total_events=("events", "sum"),
            conversion_count=("converted", "sum"),
            last_session=("session_start", "max"),
            mobile_sessions=(
                "device_type",
                lambda x: (x == "mobile").sum(),
            ),
        )
        .reset_index()
    )
    web_agg["conversion_rate"] = (
        web_agg["conversion_count"]
        / web_agg["total_sessions"].replace(0, np.nan)
        * 100
    ).fillna(0)
    web_agg["days_since_last_session"] = (
        (today - web_agg["last_session"]).dt.days
    )

    # ── RFM scoring ─────────────────────────────────────────────
    rfm = order_agg[["customer_id", "days_since_last_order",
                      "total_orders", "lifetime_revenue"]].copy()
    rfm["recency_score"] = pd.qcut(
        rfm["days_since_last_order"], 5, labels=[5, 4, 3, 2, 1]
    ).astype(int)
    rfm["frequency_score"] = pd.qcut(
        rfm["total_orders"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
    ).astype(int)
    rfm["monetary_score"] = pd.qcut(
        rfm["lifetime_revenue"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
    ).astype(int)
    rfm["rfm_total"] = rfm["recency_score"] + rfm["frequency_score"] + rfm["monetary_score"]

    # Segment assignment
    def assign_rfm_segment(row):
        r, f, m = row["recency_score"], row["frequency_score"], row["monetary_score"]
        if r >= 4 and f >= 4 and m >= 4:
            return "Champion"
        if r >= 3 and f >= 3 and m >= 3:
            return "Loyal"
        if r >= 4 and f <= 2:
            return "New Customer"
        if r <= 2 and f >= 3 and m >= 3:
            return "At Risk"
        if r <= 2 and f <= 2 and m <= 2:
            return "Lost"
        if r >= 3 and m >= 4:
            return "Big Spender"
        return "Regular"

    rfm["rfm_segment"] = rfm.apply(assign_rfm_segment, axis=1)

    # ── Customer tier ───────────────────────────────────────────
    def assign_tier(row):
        if row["lifetime_revenue"] >= 50000 and row["total_orders"] >= 50:
            return "Platinum"
        if row["lifetime_revenue"] >= 20000 and row["total_orders"] >= 20:
            return "Gold"
        if row["lifetime_revenue"] >= 5000 and row["total_orders"] >= 5:
            return "Silver"
        return "Bronze"

    order_agg["customer_tier"] = order_agg.apply(assign_tier, axis=1)

    # ── Predicted LTV ───────────────────────────────────────────
    order_agg["months_active"] = (
        (order_agg["last_order_date"] - order_agg["first_order_date"]).dt.days / 30.44
    ).clip(lower=1)
    order_agg["monthly_spend_rate"] = (
        order_agg["lifetime_revenue"] / order_agg["months_active"]
    )
    order_agg["predicted_3yr_ltv"] = np.where(
        order_agg["days_since_last_order"] > 365,
        0,
        order_agg["monthly_spend_rate"] * 36,  # 3 years
    )

    # ── Final merge ─────────────────────────────────────────────
    c360 = (
        customers[["customer_id", "first_name", "last_name", "email",
                    "signup_date", "country", "region", "segment"]]
        .merge(order_agg, on="customer_id", how="left")
        .merge(top_category, on="customer_id", how="left")
        .merge(top_brand, on="customer_id", how="left")
        .merge(distinct_products, on="customer_id", how="left")
        .merge(web_agg, on="customer_id", how="left")
        .merge(
            rfm[["customer_id", "recency_score", "frequency_score",
                 "monetary_score", "rfm_total", "rfm_segment"]],
            on="customer_id",
            how="left",
        )
    )

    # Derived: full name
    c360["full_name"] = c360["first_name"] + " " + c360["last_name"]

    # Derived: NPS proxy
    c360["nps_proxy"] = c360["rfm_segment"].map({
        "Champion": "Promoter",
        "Loyal": "Promoter",
        "Big Spender": "Promoter",
        "At Risk": "Detractor",
        "Lost": "Detractor",
    }).fillna("Passive")

    # Metadata
    c360["etl_timestamp"] = pd.Timestamp.now()
    c360["model_version"] = "v2.3"

    logger.info("Customer 360 built: %d rows, %d columns", *c360.shape)
    return c360


def build_product_performance(
    orders: pd.DataFrame,
    items_enriched: pd.DataFrame,
) -> pd.DataFrame:
    """Build product-level performance metrics."""
    joined = items_enriched.merge(
        orders[["order_id", "customer_id", "order_date", "channel_group"]],
        on="order_id",
    )

    product_perf = (
        joined.groupby(["product_id", "product_name", "category",
                         "sub_category", "brand"])
        .agg(
            total_quantity=("quantity", "sum"),
            total_revenue=("line_total", "sum"),
            total_margin=("margin", "sum"),
            avg_unit_price=("unit_price", "mean"),
            unique_customers=("customer_id", "nunique"),
            order_count=("order_id", "nunique"),
            first_sale=("order_date", "min"),
            last_sale=("order_date", "max"),
            online_sales=(
                "channel_group", lambda x: (x == "Online").sum()
            ),
        )
        .reset_index()
    )

    product_perf["avg_margin_pct"] = (
        product_perf["total_margin"]
        / product_perf["total_revenue"].replace(0, np.nan)
        * 100
    ).fillna(0)

    # Revenue rank
    product_perf["revenue_rank"] = product_perf["total_revenue"].rank(
        ascending=False, method="dense"
    ).astype(int)

    # ABC classification
    cumulative = product_perf.sort_values("total_revenue", ascending=False)
    cumulative["cumrev_pct"] = (
        cumulative["total_revenue"].cumsum()
        / cumulative["total_revenue"].sum()
        * 100
    )
    cumulative["abc_class"] = pd.cut(
        cumulative["cumrev_pct"],
        bins=[0, 80, 95, 100.01],
        labels=["A", "B", "C"],
    )
    product_perf = product_perf.merge(
        cumulative[["product_id", "abc_class"]], on="product_id"
    )

    product_perf["etl_timestamp"] = pd.Timestamp.now()

    logger.info("Product performance: %d products", len(product_perf))
    return product_perf


def build_daily_kpi(orders: pd.DataFrame) -> pd.DataFrame:
    """Build daily KPI summary."""
    daily = (
        orders.groupby(orders["order_date"].dt.date)
        .agg(
            order_count=("order_id", "nunique"),
            unique_customers=("customer_id", "nunique"),
            total_revenue=("net_amount_usd", "sum"),
            avg_order_value=("net_amount_usd", "mean"),
            total_discount=("discount_amount_usd", "sum"),
            online_orders=("channel_group", lambda x: (x == "Online").sum()),
        )
        .reset_index()
        .rename(columns={"order_date": "report_date"})
    )

    # Rolling metrics
    daily = daily.sort_values("report_date")
    daily["rolling_7d_revenue"] = daily["total_revenue"].rolling(7, min_periods=1).sum()
    daily["rolling_7d_orders"] = daily["order_count"].rolling(7, min_periods=1).sum()
    daily["rolling_30d_revenue"] = daily["total_revenue"].rolling(30, min_periods=1).sum()

    # Day-over-day change
    daily["dod_revenue_change"] = daily["total_revenue"].pct_change() * 100

    daily["etl_timestamp"] = pd.Timestamp.now()

    logger.info("Daily KPI: %d days", len(daily))
    return daily


# ═══════════════════════════════════════════════════════════════════
# Step 3: Load — Write to targets
# ═══════════════════════════════════════════════════════════════════


def load_to_parquet(df: pd.DataFrame, name: str) -> Path:
    """Write DataFrame to Parquet file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("Wrote %d rows to %s", len(df), path)
    return path


def load_to_csv(df: pd.DataFrame, name: str) -> Path:
    """Write DataFrame to CSV file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info("Wrote %d rows to %s", len(df), path)
    return path


def load_to_postgres(df: pd.DataFrame, table_name: str) -> None:
    """Write DataFrame to PostgreSQL table."""
    engine = create_engine(DB_URI)
    df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)
    logger.info("Wrote %d rows to postgres public.%s", len(df), table_name)


# ═══════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════


def run_pipeline() -> dict[str, Path | str]:
    """Execute the full ETL pipeline and return output paths."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.info("═══ Starting E-Commerce Analytics ETL ═══")

    # ── Extract ─────────────────────────────────────────────────
    orders_raw = extract_orders()
    items_raw = extract_order_items()
    customers = extract_customers()
    products = extract_products()
    fx_rates = extract_exchange_rates()
    web_sessions = extract_web_sessions()

    # ── Transform ───────────────────────────────────────────────
    orders_clean = clean_orders(orders_raw)
    orders_usd = normalize_currency(orders_clean, fx_rates)
    items_enriched = enrich_order_items(items_raw, products)

    customer_360 = build_customer_360(
        orders_usd, items_enriched, customers, web_sessions
    )
    product_perf = build_product_performance(orders_usd, items_enriched)
    daily_kpi = build_daily_kpi(orders_usd)

    # ── Load ────────────────────────────────────────────────────
    outputs: dict[str, Path | str] = {}
    outputs["customer_360"] = load_to_parquet(customer_360, "customer_360")
    outputs["product_performance"] = load_to_parquet(product_perf, "product_performance")
    outputs["daily_kpi"] = load_to_csv(daily_kpi, "daily_kpi")

    # Optional: load to database
    try:
        load_to_postgres(customer_360, "customer_summary")
        outputs["pg_customer_summary"] = "public.customer_summary"
    except Exception as e:
        logger.warning("PostgreSQL load skipped: %s", e)

    logger.info("═══ ETL Pipeline Complete ═══")
    logger.info("Outputs: %s", outputs)
    return outputs


if __name__ == "__main__":
    run_pipeline()
