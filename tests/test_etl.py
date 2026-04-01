"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # TODO: Implement

    customers = pd.DataFrame({
        "customer_id": [1],
        "customer_name": ["Alice"],
        "city": ["Amman"]
    })
    orders = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [1, 1],
        "status": ["completed", "cancelled"]
    })
    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [1, 2],
        "product_id": [1, 1],
        "quantity": [1, 1]
    })
    products = pd.DataFrame({
        "product_id": [1],
        "name": ["Widget"],
        "category": ["Gadgets"],
        "unit_price": [10]
    })

    data_dict = {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "products": products
    }

    df = transform(data_dict)
    assert df["total_orders"].iloc[0] == 1  # cancelled order should be excluded


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    # TODO: Implement
    
    customers = pd.DataFrame({
        "customer_id": [1],
        "customer_name": ["Alice"],
        "city": ["Amman"]
    })
    orders = pd.DataFrame({
        "order_id": [1],
        "customer_id": [1],
        "status": ["completed"]
    })
    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [1, 1],
        "product_id": [1, 1],
        "quantity": [1, 101]  # 101 is suspicious
    })
    products = pd.DataFrame({
        "product_id": [1],
        "name": ["Widget"],
        "category": ["Gadgets"],
        "unit_price": [10]
    })

    data_dict = {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "products": products
    }

    df = transform(data_dict)
    assert df["total_revenue"].iloc[0] == 10  # only quantity=1 counted


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    # TODO: Implement
    
    df = pd.DataFrame({
        "customer_id": [None],
        "customer_name": ["Alice"],
        "city": ["Amman"],
        "total_orders": [1],
        "total_revenue": [10],
        "avg_order_value": [10],
        "top_category": ["Gadgets"]
    })

    with pytest.raises(ValueError):
        validate(df)
