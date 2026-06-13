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

- `check_schema.py` prints schemas for products, order_items, and events.
- `analysis.py` groups products by category and computes product count, average price, min price, max price, and average margin.
- `category_statistics.py` is currently a placeholder.

## Commands

```bash
PYTHONPATH=src python3 -m task3.check_schema
PYTHONPATH=src python3 -m task3.analysis
```

## Planned Output

Recommended official output folder:

```text
data/output/task3_category_statistics/
```

## Current Status

In progress. The basic category aggregation exists, but Task 3 still needs an official output writer and final validation before it can be marked completed.
