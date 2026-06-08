from pyspark.sql import functions as F

from common.cleaning_utils import (
    build_null_counts,
    clean_text_columns,
    filter_required_fields,
)
from common.config import (
    PROCESSED_DIR,
    RAW_DIR,
    SUPPORTING_TABLES_VALIDATION_DIR,
    TASK1_SUPPORTING_TABLE_NAMES,
)
from common.schemas import SCHEMAS
from common.spark_utils import create_spark_session

# Dam bao thu muc output ton tai truoc khi ghi Parquet va validation CSV.
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
SUPPORTING_TABLES_VALIDATION_DIR.mkdir(parents=True, exist_ok=True)


spark = create_spark_session("Task1CleanSupportingTables")


# Khoa chinh dung de loai duplicate theo tung bang.
primary_keys = {
    "customers": ["customer_id"],
    "sessions": ["session_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "product_id"],
}


# Field bat buoc: dong nao thieu cac field nay se bi loai bo.
required_fields = {
    "customers": ["customer_id"],
    "sessions": ["session_id", "customer_id"],
    "products": ["product_id", "category"],
    "orders": ["order_id", "customer_id", "order_time"],
    "order_items": ["order_id", "product_id", "quantity"],
}


# Cac cot text can trim; lowercase se do helper xu ly cho cac cot thong dung.
text_columns = {
    "customers": ["name", "email", "country"],
    "sessions": ["device", "source", "country"],
    "products": ["category", "name"],
    "orders": ["payment_method", "country", "device", "source"],
    "order_items": [],
}


def read_table(table_name: str):
    """Doc mot bang raw CSV bang schema chuan trong schemas.py."""
    path = RAW_DIR / f"{table_name}.csv"

    return (
        spark.read
        .option("header", True)
        .schema(SCHEMAS[table_name])
        .csv(str(path))
    )


def clean_table(table_name: str):
    """Clean mot bang phu va ghi ca Parquet lan file validation."""
    print("=" * 100)
    print(f"CLEAN TABLE: {table_name}")

    raw_df = read_table(table_name)
    raw_count = raw_df.count()

    print("Raw rows:", raw_count)
    print("Raw schema:")
    raw_df.printSchema()

    # Buoc 1: xoa dong trung lap hoan toan.
    df = raw_df.dropDuplicates()
    after_exact_dedup_count = df.count()
    exact_duplicate_rows_removed = raw_count - after_exact_dedup_count

    # Buoc 2: chuan hoa cac cot text de gia tri nhat quan hon.
    df = clean_text_columns(df, text_columns[table_name])

    # Buoc 3: loc cac dong thieu field cot loi.
    before_required_filter_count = df.count()
    df = filter_required_fields(df, required_fields[table_name])

    after_required_filter_count = df.count()
    required_field_rows_removed = before_required_filter_count - after_required_filter_count

    # Buoc 4: xoa duplicate theo khoa chinh cua tung bang.
    before_pk_dedup_count = df.count()
    df = df.dropDuplicates(primary_keys[table_name])
    after_pk_dedup_count = df.count()
    duplicate_pk_rows_removed = before_pk_dedup_count - after_pk_dedup_count

    # Buoc 5: them cot thoi gian huu ich, nhung chua lam phan tich Task 2 tai day.
    if table_name == "sessions":
        df = (
            df
            .withColumn("session_start_date", F.to_date(F.col("start_time")))
            .withColumn("session_start_hour", F.hour(F.col("start_time")))
        )

    if table_name == "orders":
        df = (
            df
            .withColumn("order_date", F.to_date(F.col("order_time")))
            .withColumn("order_hour", F.hour(F.col("order_time")))
            .withColumn("order_year", F.year(F.col("order_time")))
            .withColumn("order_month", F.month(F.col("order_time")))
        )

    clean_count = df.count()

    print("Clean rows:", clean_count)
    print("Exact duplicate rows removed:", exact_duplicate_rows_removed)
    print("Required field rows removed:", required_field_rows_removed)
    print("Duplicate primary-key rows removed:", duplicate_pk_rows_removed)

    print("Clean schema:")
    df.printSchema()

    print("Sample:")
    df.show(10, truncate=False)

    null_counts = build_null_counts(df)

    print("Null counts:")
    null_counts.show(truncate=False)

    processed_path = PROCESSED_DIR / f"{table_name}_cleaned_parquet"
    validation_path = SUPPORTING_TABLES_VALIDATION_DIR / table_name

    # Luu bang da clean de cac script validate/phan tich doc lai.
    df.write.mode("overwrite").parquet(str(processed_path))

    # Luu tom tat cleaning giup report biet moi bang da bi loai bao nhieu dong.
    summary_rows = [
        ("table_name", table_name),
        ("raw_rows", str(raw_count)),
        ("after_exact_dedup_rows", str(after_exact_dedup_count)),
        ("exact_duplicate_rows_removed", str(exact_duplicate_rows_removed)),
        ("required_field_rows_removed", str(required_field_rows_removed)),
        ("duplicate_primary_key_rows_removed", str(duplicate_pk_rows_removed)),
        ("clean_rows", str(clean_count)),
        ("processed_output_path", str(processed_path)),
    ]

    summary_df = spark.createDataFrame(summary_rows, ["metric", "value"])

    summary_df.write.mode("overwrite").csv(
        str(validation_path / "summary_csv"),
        header=True
    )

    null_counts.write.mode("overwrite").csv(
        str(validation_path / "null_counts_csv"),
        header=True
    )

    print(f"Written processed data: {processed_path}")
    print(f"Written validation data: {validation_path}")


for table_name in TASK1_SUPPORTING_TABLE_NAMES:
    clean_table(table_name)


print("=" * 100)
print("TASK 1 SUPPORTING TABLE CLEANING COMPLETED")

spark.stop()
