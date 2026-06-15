# Task 2 Progress Report

## Scope

Task 2 computes key e-commerce clickstream metrics:

- Most active users
- Top products
- Peak activity time

## Input

Task 2 reads cleaned Parquet data from:

```text
data/processed/
```

Main tables:

- events_cleaned_parquet
- sessions_cleaned_parquet
- customers_cleaned_parquet
- products_cleaned_parquet
- orders_cleaned_parquet
- order_items_cleaned_parquet

## Implementation

Task 2 scripts are stored in `src/task2/`:

- `active_users_spark.py`
- `top_products_spark.py`
- `peak_activity_spark.py`
- `run_task2_all.py`

## Commands

Run the full Task 2 pipeline:

```bash
PYTHONPATH=src python3 -m task2.run_task2_all
```

Run each metric separately:

```bash
PYTHONPATH=src python3 -m task2.active_users_spark
PYTHONPATH=src python3 -m task2.top_products_spark
PYTHONPATH=src python3 -m task2.peak_activity_spark
```

## Output

```text
data/output/task2_metrics/most_active_users/
data/output/task2_metrics/top_products/
data/output/task2_metrics/peak_activity_by_hour/
data/output/task2_metrics/peak_activity_by_day/
data/output/task2_metrics/peak_activity_by_date/
```

## Current Status

Completed and validated. Top products now combines clickstream engagement with
purchase orders, units sold, revenue, and conversion rates. All five output
datasets pass schema, row-count, and key-null validation.
