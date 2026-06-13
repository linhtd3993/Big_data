# E-commerce Clickstream Spark Project

Dự án này xây dựng một mini big data pipeline bằng Apache Spark cho dataset e-commerce clickstream và transaction.

Trạng thái hiện tại:

- Task 1 đã hoàn thành: load raw data, clean data, validate output, và lưu dữ liệu sạch dưới dạng Parquet.
- Task 2 đã có pipeline Spark cho most active users, top products, và peak activity time.
- Task 3 đang được triển khai bước đầu cho category statistics.
- Task 4 chưa bắt đầu.

---

## 1. Phạm Vi Dự Án

Dataset:

**Kaggle E-commerce Transactions + Clickstream**

Mục tiêu bài tập:

| Task | Nội dung | Trạng thái |
|---|---|---|
| Task 1 | Load dataset và clean data | Completed |
| Task 2 | Compute key metrics: most active users, top products, peak activity time | Implemented, cần chạy/kiểm tra output |
| Task 3 | Group data by category and compute statistics | In progress |
| Task 4 | Anomaly detection, trend analysis, or recommendation logic | Not started |

Team nên dùng output Task 1 trong `data/processed/` để làm các task tiếp theo.

---

## 2. Cấu Trúc Thư Mục

```text
ecommerce_clickstream_spark/
  data/
    raw/kaggle_original/       # Raw CSV files
    processed/                 # Cleaned Parquet outputs
    output/                    # Validation outputs

  docs/
    project_owner_guide.md     # Tài liệu chi tiết cho người phụ trách dự án
    schema_mapping.md

  reports/                     # Báo cáo cuối cùng hoặc báo cáo theo task
  logs/                        # Runtime logs, được ignore bởi Git

  src/
    common/                    # Code dùng chung cho tất cả task
      config.py
      schemas.py
      spark_utils.py
      cleaning_utils.py

    task1/                     # Các script pipeline của Task 1
      profile_dataset.py
      spark_schema_check.py
      clean_events_spark.py
      clean_supporting_tables_spark.py
      validate_events_output.py
      validate_relationships_spark.py

    task2/                     # Các script metrics của Task 2
      active_users_spark.py
      top_products_spark.py
      peak_activity_spark.py
      run_task2_all.py

    task3/                     # Các script phân tích category của Task 3
      check_schema.py
      analysis.py
      category_statistics.py

  environment.yml
  requirements.txt
  .gitignore
  README.md
```

---

## 3. Thiết Lập Môi Trường

### Cách 1: Sử dụng Conda (Khuyến nghị)

Tạo môi trường:

```bash
conda env create -f environment.yml
conda activate ecommerce-clickstream-spark
```

Nếu bạn đang dùng môi trường local cũ của Linh:

```bash
conda activate ds
```

### Cách 2: Sử dụng Python venv + pip (Nếu không dùng Conda)

> [!IMPORTANT]
> Khi sử dụng phương pháp này, bạn phải cài đặt **Java (OpenJDK 17)** trên hệ thống của mình thủ công vì `pip` không thể tự động cấu hình môi trường Java cần thiết cho Spark.

```bash
# Tạo môi trường ảo
python3 -m venv venv
source venv/bin/activate  # Trên Linux/WSL
# Hoặc trên Windows PowerShell: .\venv\Scripts\Activate.ps1

# Cài đặt các thư viện phụ thuộc
pip install -r requirements.txt
```

Kiểm tra môi trường:

```bash
python3 --version
python3 -c "import pyspark; print(pyspark.__version__)"
java -version
```

Các version chính dự kiến:

```text
Python 3.11
PySpark 4.1.2
OpenJDK 17
pandas 3.0.3
```

---

## 4. Chuẩn Bị Dữ Liệu

Các file raw CSV cần được đặt tại:

```text
data/raw/kaggle_original/
```

Các file raw bắt buộc:

```text
customers.csv
sessions.csv
events.csv
products.csv
orders.csv
order_items.csv
reviews.csv
```

Lưu ý quan trọng:

- Không chỉnh sửa trực tiếp raw CSV files.
- Không ghi output mới vào `src/`.
- Output của Spark nên được ghi vào `data/processed/` hoặc `data/output/`.
- `data/processed/`, `data/output/`, và `logs/` đang được ignore bởi Git.

---

## 5. Cách Chạy Task 1

Luôn chạy lệnh từ project root:

```bash
cd /path/to/ecommerce_clickstream_spark
```

Vì source code được tổ chức thành package trong `src/`, cần cấu hình biến môi trường `PYTHONPATH` khi chạy:

### Trên Linux/WSL/macOS (Bash):
```bash
PYTHONPATH=src python3 -m task1.profile_dataset
PYTHONPATH=src python3 -m task1.spark_schema_check
PYTHONPATH=src python3 -m task1.clean_events_spark
PYTHONPATH=src python3 -m task1.clean_supporting_tables_spark
PYTHONPATH=src python3 -m task1.validate_events_output
PYTHONPATH=src python3 -m task1.validate_relationships_spark
```

### Trên Windows (PowerShell):
```powershell
$env:PYTHONPATH="src"
python3 -m task1.profile_dataset
python3 -m task1.spark_schema_check
python3 -m task1.clean_events_spark
python3 -m task1.clean_supporting_tables_spark
python3 -m task1.validate_events_output
python3 -m task1.validate_relationships_spark
```

Nếu muốn lưu log khi chạy (trên Linux/WSL):
```bash
PYTHONPATH=src python3 -m task1.clean_events_spark | tee logs/clean_events_spark.log
```

---

## 6. Cách Chạy Task 2

Chạy toàn bộ Task 2:

```bash
PYTHONPATH=src python3 -m task2.run_task2_all
```

Hoặc chạy từng phần:

```bash
PYTHONPATH=src python3 -m task2.active_users_spark
PYTHONPATH=src python3 -m task2.top_products_spark
PYTHONPATH=src python3 -m task2.peak_activity_spark
```

Task 2 đọc dữ liệu từ `data/processed/` và ghi kết quả vào `data/output/task2_metrics/`.

---

## 7. Cách Chạy Task 3

Kiểm tra schema các bảng processed cần cho Task 3:

```bash
PYTHONPATH=src python3 -m task3.check_schema
```

Chạy phân tích category bước đầu:

```bash
PYTHONPATH=src python3 -m task3.analysis
```

Lưu ý: `src/task3/category_statistics.py` hiện là file placeholder và cần được hoàn thiện nếu Task 3 cần ghi output chính thức.

---

## 8. Output Của Task 1

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

Thành viên làm Task 2-4 nên đọc dữ liệu từ:

```text
data/processed/
```

Không nên đọc trực tiếp từ `data/raw/` trừ khi cần kiểm tra dữ liệu gốc.

---

## 9. Output Của Task 2

Task 2 ghi CSV output vào các thư mục sau:

```text
data/output/task2_metrics/most_active_users/
data/output/task2_metrics/top_products/
data/output/task2_metrics/peak_activity_by_hour/
data/output/task2_metrics/peak_activity_by_day/
data/output/task2_metrics/peak_activity_by_date/
```

Các thư mục output này đang được ignore bởi Git, nên cần chạy lại pipeline khi clone project sang máy mới.

---

## 10. Tóm Tắt Dữ Liệu Đã Clean

| Bảng | Số dòng sau clean |
|---|---:|
| events | 760,958 |
| customers | 20,000 |
| sessions | 120,000 |
| products | 1,197 |
| orders | 33,580 |
| order_items | 59,053 |

Kết quả relationship validation:

| Kiểm tra | Unmatched count |
|---|---:|
| events.session_id -> sessions.session_id | 0 |
| events.product_id_int -> products.product_id | 0 |
| sessions.customer_id -> customers.customer_id | 0 |
| orders.customer_id -> customers.customer_id | 0 |
| order_items.order_id -> orders.order_id | 0 |
| order_items.product_id -> products.product_id | 0 |

Điều này có nghĩa là các bảng đã clean sẵn sàng để join trong các task sau.

---

## 11. Các Quyết Định Cleaning Quan Trọng

### Không drop null toàn cục trong `events`

Bảng `events` chứa các giá trị null theo quy tắc nghiệp vụ. Null phụ thuộc vào `event_type`.

Ví dụ:

| event_type | Dữ liệu dự kiến có | Các field null vẫn hợp lệ |
|---|---|---|
| page_view | product_id | qty, cart_size, payment, amount_usd |
| add_to_cart | product_id, qty | cart_size, payment, amount_usd |
| checkout | cart_size | product_id, qty, payment, amount_usd |
| purchase | payment, discount_pct, amount_usd | product_id, qty, cart_size |

Vì vậy Task 1 chỉ lọc các dòng thiếu core fields:

```text
event_id
session_id
event_timestamp
event_type
```

### Lưu ý schema của `sessions.csv`

`sessions.csv` có 6 cột:

```text
session_id, customer_id, start_time, device, source, country
```

File này không có `end_time`.

Không thêm `end_time` vào sessions schema trừ khi raw dataset thay đổi.

---

## 12. Gợi Ý Cho Task 2, Task 3, Task 4

### Gợi ý cho Task 2

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

Hoặc nếu muốn phân tích sản phẩm đã được mua:

```text
order_items_cleaned_parquet + products_cleaned_parquet
```

Peak activity time:

```text
events_cleaned_parquet
```

Các cột hữu ích:

```text
event_hour
day_of_week
event_date
event_month
event_year
```

### Gợi ý cho Task 3

Category statistics:

```text
products_cleaned_parquet
events_cleaned_parquet
order_items_cleaned_parquet
```

Join key hữu ích:

```text
product_id
```

### Gợi ý cho Task 4

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

## 13. Quy Tắc Phát Triển Cho Team

Vui lòng tuân thủ các quy tắc sau:

1. Code helper dùng chung đặt trong `src/common/`.
2. Code chỉ dành riêng cho từng task đặt trong folder tương ứng như `src/task1/`, `src/task2/`, `src/task3/`.
3. Các task mới nên tạo folder riêng, ví dụ:

```text
src/task2/
src/task3/
src/task4/
```

4. Không hardcode path trong script mới. Hãy dùng `common.config`.
5. Không định nghĩa lại schema trong task scripts. Hãy dùng `common.schemas`.
6. Không commit Spark output folders.
7. Không chỉnh sửa raw CSV files.
8. Luôn chạy lệnh từ project root.
9. Dùng `PYTHONPATH=src` khi chạy module.

Ví dụ script mới cho Task 2:

```text
src/task2/active_users_spark.py
```

Ví dụ import:

```python
from common.config import PROCESSED_DIR
from common.spark_utils import create_spark_session
```

Ví dụ chạy:

```bash
PYTHONPATH=src python3 -m task2.active_users_spark
```

---

## 14. Ghi Chú Về Git

Dự án này nằm trong một Git repo lớn hơn:

```text
/home/linhtd3993/workspace/projects/Big_data
```

`.gitignore` hiện tại của project ignore:

```text
data/processed/
data/output/
logs/
__pycache__/
.venv/
.vscode/
```

Repo cha cũng có thể đang ignore raw data và CSV files.

Trước khi commit, kiểm tra:

```bash
git status
```

Nên commit:

- source code trong `src/`
- tài liệu trong `docs/`
- báo cáo trong `reports/`
- config files như `.gitignore`, `environment.yml`, `README.md`

Tránh commit:

- raw CSV files
- processed Parquet folders
- runtime logs
- Python cache

---

## 15. Tài Liệu Chi Tiết Hơn

Để đọc phần giải thích chi tiết cho người phụ trách dự án, xem:

```text
docs/project_owner_guide.md
```

File này giải thích lịch sử dự án, các quyết định kỹ thuật, cleaning logic, validation results, và các bước tiếp theo chi tiết hơn.
