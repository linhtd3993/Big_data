# Project Owner Guide - E-commerce Clickstream Spark

Tai lieu nay danh cho Linh de nam lai toan bo du an hien tai: muc tieu, cau truc folder, du lieu, pipeline Task 1, ket qua da lam, cach chay code, va cac quyet dinh ky thuat quan trong.

Tai lieu duoc viet dua tren file cap nhat gan nhat:

`ecommerce_clickstream_task1_latest_progress_report_v02.docx`

va trang thai moi nhat cua workspace sau khi da refactor code thanh `src/common/` va `src/task1/`.

---

## 1. Tom Tat Nhanh

Du an nay la bai tap Big Data ve:

**E-commerce Clickstream and Transaction Analytics using Apache Spark**

Pham vi hien tai cua Linh la:

**Task 1 - Data Ingestion: load dataset and clean data**

Task 1 da hoan thanh ve mat ky thuat:

- Doc raw CSV bang Spark.
- Khai bao schema ro rang.
- Clean bang chinh `events.csv`.
- Clean cac bang phu: `customers`, `sessions`, `products`, `orders`, `order_items`.
- Luu du lieu sach dang Parquet.
- Tao validation output.
- Kiem tra quan he giua cac bang.
- Refactor code de team co the tiep tuc Task 2, Task 3, Task 4.

Task 2, Task 3, Task 4 chua duoc implement trong folder nay. Cac thanh vien khac co the doc output tu `data/processed/`.

---

## 2. Muc Tieu Du An

Muc tieu bai tap la xay dung mot mini big data pipeline bang Spark.

Trong do:

- Task 1: Load va clean du lieu.
- Task 2: Tinh cac metric chinh nhu user hoat dong nhieu, san pham top, gio cao diem.
- Task 3: Group theo category va tinh thong ke.
- Task 4: Chon anomaly detection, trend analysis, hoac recommendation logic.

Du an hien tai tap trung vao Task 1. Task 1 tao nen lop du lieu sach de cac task sau dung lai.

---

## 3. Dataset

Dataset su dung:

**Kaggle E-commerce Transactions + Clickstream**

Raw data nam tai:

```text
data/raw/kaggle_original/
```

Cac file raw chinh:

```text
customers.csv
sessions.csv
events.csv
products.csv
orders.csv
order_items.csv
reviews.csv
```

So dong raw theo inventory:

| Bang | So dong raw | Vai tro |
|---|---:|---|
| customers | 20,000 | Thong tin khach hang |
| sessions | 120,000 | Cau noi giua customer va event |
| events | 760,958 | Bang clickstream chinh |
| products | 1,197 | Thong tin san pham va category |
| orders | 33,580 | Don hang |
| order_items | 59,163 | Chi tiet san pham trong don hang |
| reviews | 10,780 | Review san pham, chua xu ly trong Task 1 hien tai |

---

## 4. Cau Truc Project Hien Tai

Cau truc quan trong:

```text
ecommerce_clickstream_spark/
  data/
    raw/kaggle_original/
    processed/
    output/

  docs/
    dataset_inventory.csv
    schema_mapping.md
    project_owner_guide.md

  logs/
  reports/
  notebooks/
  screenshots/
  slides/

  src/
    common/
      __init__.py
      config.py
      schemas.py
      spark_utils.py
      cleaning_utils.py

    task1/
      __init__.py
      profile_dataset.py
      spark_schema_check.py
      clean_events_spark.py
      clean_supporting_tables_spark.py
      validate_events_output.py
      validate_relationships_spark.py

  .gitignore
  environment.yml
```

Y nghia:

| Folder / file | Muc dich |
|---|---|
| `src/common/` | Code dung chung cho nhieu task |
| `src/task1/` | Script rieng cua Task 1 |
| `data/raw/` | Du lieu goc |
| `data/processed/` | Du lieu da clean dang Parquet |
| `data/output/` | Ket qua validation nho |
| `docs/` | Tai lieu ky thuat |
| `logs/` | Log khi chay pipeline |
| `environment.yml` | Moi truong conda cho team |
| `.gitignore` | Chan commit cache, logs, output Spark |

---

## 5. Vai Tro Cac File Trong `src/common`

### `common/config.py`

Chua cac duong dan dung chung:

- `PROJECT_ROOT`
- `RAW_DIR`
- `PROCESSED_DIR`
- `OUTPUT_DIR`
- `DOCS_DIR`
- `TASK1_VALIDATION_DIR`
- `SUPPORTING_TABLES_VALIDATION_DIR`
- `RELATIONSHIP_VALIDATION_DIR`

Ly do co file nay:

- Khong hardcode path trong tung script.
- Neu doi folder data/output thi chi can sua mot noi.
- Task 2, Task 3 co the dung lai.

### `common/schemas.py`

Chua schema Spark khai bao thu cong cho cac bang:

- customers
- sessions
- events
- products
- orders
- order_items
- reviews

Ly do quan trong:

- Khong phu thuoc `inferSchema`.
- Tranh Spark suy doan sai kieu du lieu.
- Tranh bug do schema lech so voi CSV.

Luu y dac biet:

`sessions.csv` chi co 6 cot:

```text
session_id, customer_id, start_time, device, source, country
```

Khong co `end_time`.

Bug schema `sessions` da duoc sua. Truoc day schema co them `end_time`, co the lam lech cot `device/source/country`.

### `common/spark_utils.py`

Chua ham:

```python
create_spark_session(app_name)
```

Dung de tao SparkSession dong nhat cho cac script.

### `common/cleaning_utils.py`

Chua cac helper cleaning:

- `clean_text_columns`
- `filter_required_fields`
- `build_null_counts`

Ly do:

- Giam lap code.
- Logic cleaning nhat quan hon.
- Team co the dung lai cho Task 2/3 neu can.

---

## 6. Vai Tro Cac File Trong `src/task1`

Thu tu chay khuyen nghi:

```text
1. profile_dataset.py
2. spark_schema_check.py
3. clean_events_spark.py
4. clean_supporting_tables_spark.py
5. validate_events_output.py
6. validate_relationships_spark.py
```

### `profile_dataset.py`

Dung Pandas doc sample 1000 dong moi CSV de tao inventory.

Output:

```text
docs/dataset_inventory.csv
```

### `spark_schema_check.py`

Dung Spark voi `inferSchema` de kiem tra raw CSV:

- row count
- columns
- schema
- sample

Muc dich: doi chieu raw data truoc khi clean, tranh doan schema.

### `clean_events_spark.py`

Clean bang chinh `events.csv`.

Day la script quan trong nhat cua Task 1 vi `events` la bang clickstream chinh.

Output:

```text
data/processed/events_cleaned_parquet
data/output/task1_validation/
```

### `clean_supporting_tables_spark.py`

Clean cac bang phu:

- customers
- sessions
- products
- orders
- order_items

Output:

```text
data/processed/customers_cleaned_parquet
data/processed/sessions_cleaned_parquet
data/processed/products_cleaned_parquet
data/processed/orders_cleaned_parquet
data/processed/order_items_cleaned_parquet
```

### `validate_events_output.py`

Doc lai `events_cleaned_parquet` de kiem tra:

- row count
- schema
- event type counts
- sample

### `validate_relationships_spark.py`

Kiem tra cac quan he khoa ngoai giua bang da clean:

- events.session_id -> sessions.session_id
- events.product_id_int -> products.product_id
- sessions.customer_id -> customers.customer_id
- orders.customer_id -> customers.customer_id
- order_items.order_id -> orders.order_id
- order_items.product_id -> products.product_id

Tat ca unmatched count hien tai bang 0.

---

## 7. Cach Chay Project Hien Tai

Chay tu project root:

```bash
cd /home/linhtd3993/workspace/projects/Big_data/ecommerce_clickstream_spark
```

Kich hoat conda env:

```bash
conda activate ecommerce-clickstream-spark
```

Neu dung env cu cua Linh:

```bash
conda activate ds
```

Vi code da duoc tach thanh package trong `src`, khi chay can them:

```bash
PYTHONPATH=src
```

Lenh chay day du:

```bash
PYTHONPATH=src python3 -m task1.profile_dataset
PYTHONPATH=src python3 -m task1.spark_schema_check
PYTHONPATH=src python3 -m task1.clean_events_spark
PYTHONPATH=src python3 -m task1.clean_supporting_tables_spark
PYTHONPATH=src python3 -m task1.validate_events_output
PYTHONPATH=src python3 -m task1.validate_relationships_spark
```

Neu muon luu log:

```bash
PYTHONPATH=src python3 -m task1.clean_events_spark | tee logs/clean_events_spark.log
```

Luu y:

`logs/` dang duoc ignore trong Git, nen log chi de doc local.

---

## 8. Moi Truong Chay

Moi truong da duoc dua vao:

```text
environment.yml
```

No gom:

- Python 3.11
- pandas 3.0.3
- OpenJDK 17
- PySpark 4.1.2

Tao moi truong:

```bash
conda env create -f environment.yml
conda activate ecommerce-clickstream-spark
```

Kiem tra:

```bash
python3 --version
python3 -c "import pyspark; print(pyspark.__version__)"
java -version
```

---

## 9. Cleaning Logic Cho `events.csv`

Bang `events.csv` duoc clean theo cac buoc:

1. Doc CSV bang schema khai bao san.
2. Xoa duplicate dong hoan toan.
3. Chuan hoa `event_type` va `payment`.
4. Tao cot integer:
   - `product_id_int`
   - `qty_int`
   - `cart_size_int`
5. Doi ten `timestamp` thanh `event_timestamp`.
6. Tao cot thoi gian:
   - `event_date`
   - `event_hour`
   - `day_of_week`
   - `event_year`
   - `event_month`
7. Loc cac dong thieu field bat buoc:
   - event_id
   - session_id
   - event_timestamp
   - event_type
8. Xoa duplicate theo `event_id`.
9. Tao flag:
   - `has_product`
   - `has_amount`
   - `has_payment`
10. Luu Parquet.

Ket qua:

| Metric | Value |
|---|---:|
| raw_rows | 760,958 |
| exact_duplicate_rows_removed | 0 |
| required_field_rows_removed | 0 |
| duplicate_event_id_rows_removed | 0 |
| clean_rows | 760,958 |

Event type counts:

| event_type | count |
|---|---:|
| page_view | 539,343 |
| add_to_cart | 143,126 |
| checkout | 44,909 |
| purchase | 33,580 |

---

## 10. Quyet Dinh Quan Trong Ve Null

Khong duoc `dropna` toan bo bang `events`.

Ly do:

Null trong `events.csv` phu thuoc vao `event_type`.

Vi du:

| event_type | Cot co the co data | Cot null la hop ly |
|---|---|---|
| page_view | product_id | qty, cart_size, payment, amount_usd |
| add_to_cart | product_id, qty | cart_size, payment, amount_usd |
| checkout | cart_size | product_id, qty, payment |
| purchase | payment, discount_pct, amount_usd | product_id, qty, cart_size |

Day la **business-rule null**, khong phai dirty data.

---

## 11. Cleaning Logic Cho Bang Phu

Cac bang phu duoc clean theo pattern chung:

1. Doc CSV bang schema chuan.
2. Xoa duplicate dong hoan toan.
3. Trim/lowercase cot text.
4. Loc cac dong thieu field bat buoc.
5. Xoa duplicate theo primary key.
6. Them cot thoi gian cho sessions/orders.
7. Luu Parquet.
8. Luu validation summary va null counts.

Ket qua:

| Bang | Raw rows | Exact duplicates removed | Required removed | PK duplicates removed | Clean rows |
|---|---:|---:|---:|---:|---:|
| customers | 20,000 | 0 | 0 | 0 | 20,000 |
| sessions | 120,000 | 0 | 0 | 0 | 120,000 |
| products | 1,197 | 0 | 0 | 0 | 1,197 |
| orders | 33,580 | 0 | 0 | 0 | 33,580 |
| order_items | 59,163 | 73 | 0 | 37 | 59,053 |

---

## 12. Relationship Validation

Ket qua validate quan he:

| Relationship | Unmatched count | Result |
|---|---:|---|
| events.session_id -> sessions.session_id | 0 | Pass |
| events.product_id_int -> products.product_id | 0 | Pass |
| sessions.customer_id -> customers.customer_id | 0 | Pass |
| orders.customer_id -> customers.customer_id | 0 | Pass |
| order_items.order_id -> orders.order_id | 0 | Pass |
| order_items.product_id -> products.product_id | 0 | Pass |

Y nghia:

Du lieu da clean co the join duoc cho Task 2, Task 3, Task 4.

---

## 13. Output Cho Cac Task Sau

Cac thanh vien Task 2-4 nen doc tu:

```text
data/processed/
```

Khong nen doc lai tu:

```text
data/raw/
```

Output quan trong:

```text
events_cleaned_parquet
customers_cleaned_parquet
sessions_cleaned_parquet
products_cleaned_parquet
orders_cleaned_parquet
order_items_cleaned_parquet
```

Goi y dung cho cac task sau:

| Task sau | Nen dung bang nao |
|---|---|
| Most active users | events + sessions + customers |
| Top products | events + products hoac order_items + products |
| Peak activity time | events |
| Category statistics | products + events/order_items |
| Trend analysis | events + orders + order_items |

---

## 14. Git Va Data

`.gitignore` hien tai ignore:

```text
data/processed/
data/output/
logs/
__pycache__/
.venv/
.vscode/
```

Ngoai ra repo cha `Big_data` dang ignore:

```text
**/data/raw/*
*.csv
```

Y nghia:

- Raw data co the khong duoc commit.
- CSV output nhu `docs/dataset_inventory.csv` co the bi ignore boi repo cha.
- Neu can chia se raw data cho team, nen dung link Google Drive/Kaggle/OneDrive hoac cong cu nhu DVC/Git LFS.

---

## 15. Docker/HDFS

Docker/HDFS hien dang **postpone**.

Ly do:

- Bai tap yeu cau Spark, khong bat buoc Hadoop/HDFS.
- Local Spark pipeline da chay duoc va phu hop de debug.
- Nen hoan thanh Task 2-4 truoc, sau do moi them Docker/HDFS nhu phan nang cap kien truc.

Huong sau nay:

```text
Local raw CSV
-> HDFS raw zone
-> Spark read HDFS
-> Spark write HDFS processed zone
-> Report/slides co them architecture diagram va screenshot
```

---

## 16. Cac Van De Da Gap Va Da Sua

| Van de | Nguyen nhan | Cach xu ly |
|---|---|---|
| `pyspark` missing | Env chua cai PySpark | Cai PySpark, xac nhan version 4.1.2 |
| Java missing | Spark can JVM | Cai OpenJDK 17 |
| Schema `sessions` sai | Them nham `end_time` | Sua schema ve dung 6 cot |
| Null nhieu trong events | Null theo event_type | Khong dropna toan cuc, validate theo event_type |
| Code lap lai | Moi script tu viet path/schema/helper | Tach `common/` |
| Ten file co so dau | Kho doc, kho mo rong cho team | Doi ten va dua vao `task1/` |
| Import de gay roi | File nam ngang trong src | Chuyen sang package import `common.*` |

---

## 17. Trang Thai Hien Tai

Da hoan thanh:

- Task 1 Spark ingestion.
- Clean events.
- Clean supporting tables.
- Validate output.
- Validate relationships.
- Refactor `src/common` va `src/task1`.
- Tao `.gitignore`.
- Tao `environment.yml`.

Can lam tiep:

1. Tao README.md cho nguoi moi clone project.
2. Tao file handoff cho teammate.
3. Tao report Task 1 de nop/dua vao final report.
4. Cap nhat command moi trong docs/logs neu can.
5. Sau khi team lam Task 2-4, moi can xem lai Docker/HDFS.

---

## 18. Lenh Kiem Tra Nhanh

Compile toan bo code:

```bash
PYTHONPATH=src python3 -m py_compile $(find src -name '*.py' -print)
```

Chay profile:

```bash
PYTHONPATH=src python3 -m task1.profile_dataset
```

Chay schema check:

```bash
PYTHONPATH=src python3 -m task1.spark_schema_check
```

Chay full Task 1:

```bash
PYTHONPATH=src python3 -m task1.clean_events_spark
PYTHONPATH=src python3 -m task1.clean_supporting_tables_spark
PYTHONPATH=src python3 -m task1.validate_events_output
PYTHONPATH=src python3 -m task1.validate_relationships_spark
```

---

## 19. Ghi Chu Cho Linh

Neu ban quay lai project sau vai ngay, hay doc theo thu tu:

1. `docs/project_owner_guide.md` - file nay.
2. `src/common/config.py` - xem path.
3. `src/common/schemas.py` - xem schema.
4. `src/task1/clean_events_spark.py` - xem clean events.
5. `src/task1/clean_supporting_tables_spark.py` - xem clean bang phu.
6. `src/task1/validate_relationships_spark.py` - xem quan he giua bang.

Dieu can nho nhat:

**Task 1 khong chi clean data, ma tao mot processed data layer sach de ca team dung cho Task 2-4.**

