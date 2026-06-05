# E-commerce Clickstream Spark Project

Du an nay xay dung mot mini big data pipeline bang Apache Spark cho dataset e-commerce clickstream va transaction.

Trang thai hien tai:

- Task 1 da hoan thanh: load raw data, clean data, validate output, va luu du lieu sach dang Parquet.
- Task 2, Task 3, Task 4 se duoc cac thanh vien khac phat trien tiep dua tren output trong `data/processed/`.

---

## 1. Project Scope

Dataset:

**Kaggle E-commerce Transactions + Clickstream**

Muc tieu bai tap:

| Task | Noi dung | Trang thai |
|---|---|---|
| Task 1 | Load dataset va clean data | Completed |
| Task 2 | Compute key metrics: most active users, top products, peak activity time | Not started |
| Task 3 | Group data by category and compute statistics | Not started |
| Task 4 | Anomaly detection, trend analysis, or recommendation logic | Not started |

Team nen dung output Task 1 trong `data/processed/` de lam cac task tiep theo.

---

## 2. Folder Structure

```text
ecommerce_clickstream_spark/
  data/
    raw/kaggle_original/       # Raw CSV files
    processed/                 # Cleaned Parquet outputs
    output/                    # Validation outputs

  docs/
    project_owner_guide.md     # Detailed guide for project owner
    schema_mapping.md

  reports/                     # Final or task reports
  logs/                        # Runtime logs, ignored by Git

  src/
    common/                    # Shared code for all tasks
      config.py
      schemas.py
      spark_utils.py
      cleaning_utils.py

    task1/                     # Task 1 pipeline scripts
      profile_dataset.py
      spark_schema_check.py
      clean_events_spark.py
      clean_supporting_tables_spark.py
      validate_events_output.py
      validate_relationships_spark.py

  environment.yml
  .gitignore
  README.md
```

---

## 3. Environment Setup

Recommended: use Conda.

Create environment:

```bash
conda env create -f environment.yml
conda activate ecommerce-clickstream-spark
```

If you already use Linh's local env:

```bash
conda activate ds
```

Check environment:

```bash
python3 --version
python3 -c "import pyspark; print(pyspark.__version__)"
java -version
```

Expected main versions:

```text
Python 3.11
PySpark 4.1.2
OpenJDK 17
pandas 3.0.3
```

---

## 4. Data Setup

Raw CSV files should be placed here:

```text
data/raw/kaggle_original/
```

Required raw files:

```text
customers.csv
sessions.csv
events.csv
products.csv
orders.csv
order_items.csv
reviews.csv
```

Important:

- Do not edit raw CSV files directly.
- Do not write new output into `src/`.
- Spark output should go to `data/processed/` or `data/output/`.
- `data/processed/`, `data/output/`, and `logs/` are ignored by Git.

---

## 5. How To Run Task 1

Always run commands from project root:

```bash
cd /path/to/ecommerce_clickstream_spark
```

Because source code is organized as packages under `src/`, use:

```bash
PYTHONPATH=src
```

Recommended run order:

```bash
PYTHONPATH=src python3 -m task1.profile_dataset
PYTHONPATH=src python3 -m task1.spark_schema_check
PYTHONPATH=src python3 -m task1.clean_events_spark
PYTHONPATH=src python3 -m task1.clean_supporting_tables_spark
PYTHONPATH=src python3 -m task1.validate_events_output
PYTHONPATH=src python3 -m task1.validate_relationships_spark
```

Optional: save logs while running:

```bash
PYTHONPATH=src python3 -m task1.clean_events_spark | tee logs/clean_events_spark.log
```

---

## 6. Task 1 Outputs

Cleaned Parquet outputs:

```text
data/processed/events_cleaned_parquet
data/processed/customers_cleaned_parquet
data/processed/sessions_cleaned_parquet
data/processed/products_cleaned_parquet
data/processed/orders_cleaned_parquet
data/processed/order_items_cleaned_parquet
```

Validation outputs:

```text
data/output/task1_validation/
```

Task 2-4 members should read from:

```text
data/processed/
```

They should not read directly from `data/raw/` unless they intentionally need to inspect original data.

---

## 7. Current Cleaned Data Summary

| Table | Clean rows |
|---|---:|
| events | 760,958 |
| customers | 20,000 |
| sessions | 120,000 |
| products | 1,197 |
| orders | 33,580 |
| order_items | 59,053 |

Relationship validation result:

| Check | Unmatched count |
|---|---:|
| events.session_id -> sessions.session_id | 0 |
| events.product_id_int -> products.product_id | 0 |
| sessions.customer_id -> customers.customer_id | 0 |
| orders.customer_id -> customers.customer_id | 0 |
| order_items.order_id -> orders.order_id | 0 |
| order_items.product_id -> products.product_id | 0 |

This means the cleaned tables are ready for joins in later tasks.

---

## 8. Important Data Cleaning Decisions

### Do not globally drop nulls in `events`

The `events` table contains business-rule nulls. Null values depend on `event_type`.

Examples:

| event_type | Expected data | Valid null fields |
|---|---|---|
| page_view | product_id | qty, cart_size, payment, amount_usd |
| add_to_cart | product_id, qty | cart_size, payment, amount_usd |
| checkout | cart_size | product_id, qty, payment, amount_usd |
| purchase | payment, discount_pct, amount_usd | product_id, qty, cart_size |

So Task 1 only filters rows missing core fields:

```text
event_id
session_id
event_timestamp
event_type
```

### `sessions.csv` schema note

`sessions.csv` has 6 columns:

```text
session_id, customer_id, start_time, device, source, country
```

It does not have `end_time`.

Do not add `end_time` to the sessions schema unless the raw dataset changes.

---

## 9. Guidelines For Task 2, Task 3, Task 4

### Task 2 suggestions

Most active users:

```text
events_cleaned_parquet
sessions_cleaned_parquet
customers_cleaned_parquet
```

Join path:

```text
events.session_id -> sessions.session_id -> customers.customer_id
```

Top products:

```text
events_cleaned_parquet + products_cleaned_parquet
```

or for purchased products:

```text
order_items_cleaned_parquet + products_cleaned_parquet
```

Peak activity time:

```text
events_cleaned_parquet
```

Useful columns:

```text
event_hour
day_of_week
event_date
event_month
event_year
```

### Task 3 suggestions

Category statistics:

```text
products_cleaned_parquet
events_cleaned_parquet
order_items_cleaned_parquet
```

Useful join key:

```text
product_id
```

### Task 4 suggestions

Trend analysis:

```text
events_cleaned_parquet
orders_cleaned_parquet
order_items_cleaned_parquet
```

Recommendation logic:

```text
customers_cleaned_parquet
sessions_cleaned_parquet
events_cleaned_parquet
products_cleaned_parquet
```

Anomaly detection:

```text
orders_cleaned_parquet
order_items_cleaned_parquet
events_cleaned_parquet
```

---

## 10. Development Rules For Team

Please follow these rules:

1. Put shared helper code in `src/common/`.
2. Put Task 1-only code in `src/task1/`.
3. Create new folders for new tasks, for example:

```text
src/task2/
src/task3/
src/task4/
```

4. Do not hardcode paths in new scripts. Use `common.config`.
5. Do not redefine schemas in task scripts. Use `common.schemas`.
6. Do not commit Spark output folders.
7. Do not modify raw CSV files.
8. Always run from project root.
9. Use `PYTHONPATH=src` when running modules.

Example for new Task 2 script:

```text
src/task2/active_users_spark.py
```

Example import:

```python
from common.config import PROCESSED_DIR
from common.spark_utils import create_spark_session
```

Example run:

```bash
PYTHONPATH=src python3 -m task2.active_users_spark
```

---

## 11. Git Notes

This project is inside a larger Git repo:

```text
/home/linhtd3993/workspace/projects/Big_data
```

Current project `.gitignore` ignores:

```text
data/processed/
data/output/
logs/
__pycache__/
.venv/
.vscode/
```

The parent repo may also ignore raw data and CSV files.

Before committing, check:

```bash
git status
```

Recommended commit content:

- source code in `src/`
- docs in `docs/`
- reports in `reports/`
- config files like `.gitignore`, `environment.yml`, `README.md`

Avoid committing:

- raw CSV files
- processed Parquet folders
- runtime logs
- Python cache

---

## 12. More Documentation

For a detailed owner-level explanation, read:

```text
docs/project_owner_guide.md
```

That file explains the project history, decisions, cleaning logic, validation results, and next steps in more detail.

