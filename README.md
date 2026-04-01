[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This ETL pipeline extracts data from the Amman Digital Market PostgreSQL database, transforms it into customer-level analytics using Pandas, validates data quality, and loads the results into a new database table (`customer_analytics`) and a CSV file (`output/customer_analytics.csv`).  
It summarizes each customer's total orders, total revenue, average order value, and top product category.

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

The pipeline produces `output/customer_analytics.csv` with the following columns:

- `customer_id`: unique ID for the customer
- `customer_name`: name of the customer
- `city`: customer city
- `total_orders`: number of completed orders
- `total_revenue`: sum of all order line totals
- `avg_order_value`: total revenue divided by total orders
- `top_category`: product category with the highest revenue for the customer

The same data is also loaded into the PostgreSQL table `customer_analytics`.

## Quality Checks

The pipeline validates the transformed data with these checks:

1. No null values in `customer_id` or `customer_name` — ensures each row represents a real customer.
2. `total_revenue` > 0 — verifies the customer generated revenue.
3. No duplicate `customer_id` values — ensures unique customer summaries.
4. `total_orders` > 0 — confirms each customer has at least one order.

Each check prints `PASS` or `FAIL`. The pipeline raises an error if any critical check fails.

---

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
