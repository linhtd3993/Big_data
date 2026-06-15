# Task 3 Progress Report

## Scope

Task 3 groups data by product category and computes category-level statistics.

## Input

Current Task 3 scripts read cleaned Parquet data from:

```text
data/processed/
```

Main tables:

- products_cleaned_parquet
- order_items_cleaned_parquet
- events_cleaned_parquet

## Implementation

Task 3 scripts are stored in `src/task3/`:

- `check_schema.py`
- `analysis.py`
- `category_statistics.py`

Current behavior:

- Computes catalog price and margin statistics.
- Computes page views and add-to-cart activity.
- Computes orders, units sold, revenue, and estimated gross margin.
- Computes review count and average rating.
- Computes conversion rates and revenue rank.
- Reconciles products, units, revenue, and reviews with source totals.

## Commands

```bash
PYTHONPATH=src python3 -m task3.check_schema
PYTHONPATH=src python3 -m task3.analysis
```

## Output

```text
data/output/task3_category_statistics/
```

## Current Status

Completed. Validation confirms 1,197 products, 77,031 units,
USD 4,832,351.23 gross revenue, and 10,780 reviews.
