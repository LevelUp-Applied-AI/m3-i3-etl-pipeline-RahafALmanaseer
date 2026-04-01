"""
Challenges ETL Pipeline — Amman Digital Market
Includes:
- Tier 1: Outlier Detection + Quality Report
- Tier 2: Incremental ETL
- Tier 3: Config-driven ETL with Logging
"""

import pandas as pd
import os
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

# -----------------------------
# Setup Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/challenges_etl.log"),
        logging.StreamHandler()
    ]
)

# -----------------------------
# Load ETL Config
# -----------------------------
CONFIG_PATH = "etl_config.json"

def load_config(path=CONFIG_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    with open(path) as f:
        config = json.load(f)
    return config

# -----------------------------
# Extract
# -----------------------------
def extract(engine, incremental_date=None):
    """
    Extract tables from Postgres. Supports incremental extraction.
    """
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders_query = "SELECT * FROM orders"
    if incremental_date:
        orders_query += f" WHERE order_date > '{incremental_date}'"
    orders = pd.read_sql(orders_query, engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    logging.info(f"Extracted: customers={len(customers)}, products={len(products)}, "
                 f"orders={len(orders)}, order_items={len(order_items)}")
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}

# -----------------------------
# Transform
# -----------------------------
def transform(data_dict):
    """
    Join tables, compute line_total, filter cancelled/suspicious, aggregate per customer
    """
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    df = orders.merge(order_items, on="order_id") \
               .merge(products, on="product_id") \
               .merge(customers, on="customer_id")

    # Filters
    df = df[df["status"] != "cancelled"]
    df = df[df["quantity"] <= 100]

    # Line total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Aggregate
    summary = df.groupby(["customer_id", "customer_name", "city"]).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    ).reset_index()

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    # Top category
    top_cat = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = top_cat.sort_values(["customer_id", "line_total"], ascending=[True, False])
    top_cat = top_cat.drop_duplicates("customer_id")
    summary = summary.merge(top_cat[["customer_id", "category"]], on="customer_id")
    summary.rename(columns={"category": "top_category"}, inplace=True)

    return summary

# -----------------------------
# Tier 1 — Outliers + Quality Report
# -----------------------------
def tier1_outliers(df):
    """
    Add is_outlier column for total_revenue > 3 std deviations
    and generate quality_report.json
    """
    # -----------------------------
def tier1_outliers(df):
    """
    Add an 'is_outlier' column for customers whose total_revenue
    exceeds 3 standard deviations above the mean, and generate
    a quality_report.json file.
    """
    import os, json
    from datetime import datetime
    import logging

    # Calculate mean and standard deviation of total revenue
    mean_rev = df["total_revenue"].mean()
    std_rev = df["total_revenue"].std()

    # Identify outliers
    df["is_outlier"] = df["total_revenue"] > (mean_rev + 3 * std_rev)
    outliers = df[df["is_outlier"] == True]["customer_id"].tolist()

    # Current timestamp for the report
    timestamp = datetime.now().isoformat()

    # Prepare the report dictionary
    report = {
        "total_records": int(df.shape[0]),
        "outliers_count": int(len(outliers)),
        "outlier_ids": outliers,
        "timestamp": timestamp
    }

    # Make sure the output folder exists
    os.makedirs("output", exist_ok=True)

    # Write the report to JSON
    with open("output/quality_report.json", "w") as f:
        json.dump(report, f, indent=2)

    logging.info(f"Tier 1: Outliers detected: {report['outliers_count']}")

    return df


# -----------------------------
# Validate
# -----------------------------
def validate(df):
    results = {
        "no_null_customer_id": df["customer_id"].notnull().all(),
        "no_null_customer_name": df["customer_name"].notnull().all(),
        "total_revenue_positive": (df["total_revenue"] > 0).all(),
        "no_duplicate_customer_id": not df["customer_id"].duplicated().any(),
        "total_orders_positive": (df["total_orders"] > 0).all()
    }
    for check, passed in results.items():
        logging.info(f"{check}: {'PASS' if passed else 'FAIL'}")
    if not all(results.values()):
        raise ValueError("Validation failed")
    return results

# -----------------------------
# Load
# -----------------------------
def load(df, engine, csv_path, incremental=False):
    """
    Load summary to Postgres and CSV
    """
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    logging.info(f"Loaded {len(df)} rows to DB and {csv_path}")

    # Update ETL metadata for incremental
    if incremental:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_metadata (
                    run_id SERIAL PRIMARY KEY,
                    run_time TIMESTAMP,
                    rows_processed INT
                )
            """))
            conn.execute(text("""
                INSERT INTO etl_metadata (run_time, rows_processed)
                VALUES (:time, :rows)
            """), {"time": datetime.now(), "rows": len(df)})

# -----------------------------
# Main
# -----------------------------
def main():
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/amman_market"
    )
    engine = create_engine(DATABASE_URL)
    
    # Incremental: get last run time
    incremental_date = None
    with engine.connect() as conn:
        try:
            last_run = conn.execute(text("SELECT MAX(run_time) FROM etl_metadata")).scalar()
            if last_run:
                incremental_date = last_run
        except:
            pass

    logging.info("Extracting...")
    data = extract(engine, incremental_date)

    logging.info("Transforming...")
    df = transform(data)

    logging.info("Tier 1: Detecting outliers and generating quality report...")
    df = tier1_outliers(df)

    logging.info("Validating...")
    validate(df)

    logging.info("Loading...")
    load(df, engine, "output/customer_analytics.csv", incremental=True)

    logging.info("ETL Challenges pipeline completed successfully")

if __name__ == "__main__":
    main()
