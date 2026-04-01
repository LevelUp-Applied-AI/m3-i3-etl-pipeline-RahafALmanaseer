"""
ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""

import os
import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# 1. Extract
# -----------------------------
def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames."""
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    print("Extracted:")
    print({
        "customers": len(customers),
        "products": len(products),
        "orders": len(orders),
        "order_items": len(order_items)
    })

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }

# -----------------------------
# 2. Transform
# -----------------------------
def transform(data_dict):
    """Transform raw data into customer-level analytics summary."""
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # Merge tables
    df = orders.merge(order_items, on="order_id") \
               .merge(products, on="product_id") \
               .merge(customers, on="customer_id")

    # Filter cancelled orders and suspicious quantities
    df = df[df["status"] != "cancelled"]
    df = df[df["quantity"] <= 100]

    # Calculate line total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Aggregate per customer
    summary = df.groupby(["customer_id", "customer_name", "city"]).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    ).reset_index()

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    # Determine top category per customer
    top_cat = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = top_cat.sort_values(["customer_id", "line_total"], ascending=[True, False])
    top_cat = top_cat.drop_duplicates("customer_id")

    summary = summary.merge(top_cat[["customer_id", "category"]], on="customer_id")
    summary.rename(columns={"category": "top_category"}, inplace=True)

    print("Transform completed. Rows:", len(summary))
    return summary

# -----------------------------
# 3. Validate
# -----------------------------
def validate(df):
    """Run data quality checks on the transformed DataFrame."""
    results = {}
    results["no_null_customer_id"] = df["customer_id"].notnull().all()
    results["no_null_customer_name"] = df["customer_name"].notnull().all()
    results["total_revenue_positive"] = (df["total_revenue"] > 0).all()
    results["no_duplicate_customer_id"] = df["customer_id"].duplicated().sum() == 0
    results["total_orders_positive"] = (df["total_orders"] > 0).all()

    for check, passed in results.items():
        print(f"{check}: {'PASS' if passed else 'FAIL'}")

    if not all(results.values()):
        raise ValueError("Validation failed!")

    return results

# -----------------------------
# 4. Load
# -----------------------------
def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)
    print(f"Loaded {len(df)} rows to DB and {csv_path}")

# -----------------------------
# Main ETL orchestration
# -----------------------------
def main():
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/amman_market"
    )
    engine = create_engine(DATABASE_URL)

    print("Extracting...")
    data = extract(engine)

    print("Transforming...")
    df = transform(data)

    print("Validating...")
    validate(df)

    print("Loading...")
    load(df, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully!")

if __name__ == "__main__":
    main()
