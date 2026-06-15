# Task 1 Progress Report

## Scope

Task 1 covers data ingestion, schema checking, data cleaning, output validation, and relationship validation for the e-commerce clickstream dataset.

## Input

Raw CSV files are stored in:

```text
data/raw/kaggle_original/
```

Main files:

- customers.csv
- sessions.csv
- events.csv
- products.csv
- orders.csv
- order_items.csv
- reviews.csv

## Implementation

Task 1 scripts are stored in `src/task1/`:

- `profile_dataset.py`
- `spark_schema_check.py`
- `clean_events_spark.py`
- `clean_supporting_tables_spark.py`
- `validate_events_output.py`
- `validate_relationships_spark.py`
- `validate_business_rules.py`
- `run_task1_all.py`

Common helpers are stored in `src/common/`.

## Commands

```bash
PYTHONPATH=src python3 -m task1.profile_dataset
PYTHONPATH=src python3 -m task1.spark_schema_check
PYTHONPATH=src python3 -m task1.clean_events_spark
PYTHONPATH=src python3 -m task1.clean_supporting_tables_spark
PYTHONPATH=src python3 -m task1.validate_events_output
PYTHONPATH=src python3 -m task1.validate_relationships_spark
```

## Output

Cleaned Parquet outputs:

```text
data/processed/events_cleaned_parquet
data/processed/customers_cleaned_parquet
data/processed/sessions_cleaned_parquet
data/processed/products_cleaned_parquet
data/processed/orders_cleaned_parquet
data/processed/order_items_cleaned_parquet
data/processed/reviews_cleaned_parquet
```

Validation output:

```text
data/output/task1_validation/
```

## Current Status

Completed. All seven raw tables are cleaned and available as Parquet.

Relationship validation reports zero unmatched records for all eight joins.
Business validation covers allowed event types, positive quantities, monetary
consistency, discount ranges, margins, ratings, customer age, and event/session
time ordering.
