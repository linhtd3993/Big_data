from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from common.cleaning_utils import (
    build_null_counts,
    clean_text_columns,
    filter_required_fields,
)
from common.config import PROCESSED_DIR, RAW_DIR, TASK1_VALIDATION_DIR
from common.schemas import EVENTS_SCHEMA
from common.spark_utils import create_spark_session


# Duong dan input/output rieng cho bang events.
EVENTS_RAW_PATH = RAW_DIR / "events.csv"
EVENTS_PROCESSED_PATH = PROCESSED_DIR / "events_cleaned_parquet"

TASK1_VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


spark = create_spark_session("Task1CleanEvents")


# In thong tin dau vao/dau ra de log de doc khi chay pipeline.
print("=" * 100)
print("TASK 1 - CLEAN EVENTS")
print(f"Raw path: {EVENTS_RAW_PATH}")
print(f"Processed path: {EVENTS_PROCESSED_PATH}")
print(f"Validation path: {TASK1_VALIDATION_DIR}")


# Doc events bang schema khai bao san de tranh Spark suy doan sai kieu du lieu.
raw_events = (
    spark.read
    .option("header", True)
    .schema(EVENTS_SCHEMA)
    .csv(str(EVENTS_RAW_PATH))
)

raw_count = raw_events.count()

print("=" * 100)
print("RAW EVENTS")
print("Raw rows:", raw_count)
raw_events.printSchema()
raw_events.show(10, truncate=False)


# 1. Xoa cac dong trung lap hoan toan.
events = raw_events.dropDuplicates()
after_exact_dedup_count = events.count()
exact_duplicate_rows_removed = raw_count - after_exact_dedup_count


# 2. Chuan hoa text de cac gia tri nhu " Purchase " va "purchase" duoc xem nhu nhau.
events = clean_text_columns(events, ["event_type", "payment"])


# 3. Tao cot integer sach hon cho cac cot dang duoc doc tu CSV o dang double.
events = (
    events
    .withColumn("product_id_int", F.col("product_id").cast(IntegerType()))
    .withColumn("qty_int", F.col("qty").cast(IntegerType()))
    .withColumn("cart_size_int", F.col("cart_size").cast(IntegerType()))
)


# 4. Tao cac cot thoi gian phuc vu phan tich o cac task sau.
events = (
    events
    .withColumnRenamed("timestamp", "event_timestamp")
    .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    .withColumn("event_hour", F.hour(F.col("event_timestamp")))
    .withColumn("day_of_week", F.date_format(F.col("event_timestamp"), "E"))
    .withColumn("event_year", F.year(F.col("event_timestamp")))
    .withColumn("event_month", F.month(F.col("event_timestamp")))
)


# 5. Chi loc cac field cot loi bat buoc.
# Khong drop tat ca null vi null trong events phu thuoc event_type va co y nghia nghiep vu.
before_required_filter_count = events.count()

events = filter_required_fields(
    events,
    ["event_id", "session_id", "event_timestamp", "event_type"],
)

after_required_filter_count = events.count()
required_field_rows_removed = before_required_filter_count - after_required_filter_count


# 6. Giu duy nhat mot dong cho moi event_id, vi event_id la khoa chinh cua events.
before_event_id_dedup_count = events.count()
events = events.dropDuplicates(["event_id"])
after_event_id_dedup_count = events.count()
duplicate_event_id_rows_removed = before_event_id_dedup_count - after_event_id_dedup_count


# 7. Them cac co data quality de phan tich nhanh muc do day du cua thong tin.
events = (
    events
    .withColumn(
        "has_product",
        F.col("product_id_int").isNotNull()
    )
    .withColumn(
        "has_amount",
        F.col("amount_usd").isNotNull()
    )
    .withColumn(
        "has_payment",
        F.col("payment").isNotNull()
    )
)


clean_count = events.count()


print("=" * 100)
print("CLEAN EVENTS SUMMARY")
print("Raw rows:", raw_count)
print("After exact duplicate removal:", after_exact_dedup_count)
print("Exact duplicate rows removed:", exact_duplicate_rows_removed)
print("Rows removed because required fields are missing:", required_field_rows_removed)
print("Duplicate event_id rows removed:", duplicate_event_id_rows_removed)
print("Clean rows:", clean_count)


print("=" * 100)
print("EVENT TYPE COUNTS")
event_type_counts = events.groupBy("event_type").count().orderBy(F.desc("count"))
event_type_counts.show(100, truncate=False)


print("=" * 100)
print("NULL COUNTS")
null_counts = build_null_counts(events)
null_counts.show(truncate=False)


print("=" * 100)
print("NULL PROFILE BY EVENT TYPE")
# Kiem tra null theo event_type vi moi loai event co bo cot hop le khac nhau.
event_type_null_profile = (
    events
    .groupBy("event_type")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("product_id_int").isNull().cast("int")).alias("null_product_id"),
        F.sum(F.col("qty_int").isNull().cast("int")).alias("null_qty"),
        F.sum(F.col("cart_size_int").isNull().cast("int")).alias("null_cart_size"),
        F.sum(F.col("payment").isNull().cast("int")).alias("null_payment"),
        F.sum(F.col("discount_pct").isNull().cast("int")).alias("null_discount_pct"),
        F.sum(F.col("amount_usd").isNull().cast("int")).alias("null_amount_usd"),
    )
    .orderBy("event_type")
)

event_type_null_profile.show(100, truncate=False)


print("=" * 100)
print("CLEANED SAMPLE")
events.show(20, truncate=False)


# 8. Luu du lieu sach o dinh dang Parquet de cac buoc sau doc nhanh hon CSV.
events.write.mode("overwrite").parquet(str(EVENTS_PROCESSED_PATH))


# 9. Luu cac bang validate nho de dua vao report hoac kiem tra lai ket qua.
summary_rows = [
    ("raw_rows", raw_count),
    ("after_exact_dedup_rows", after_exact_dedup_count),
    ("exact_duplicate_rows_removed", exact_duplicate_rows_removed),
    ("required_field_rows_removed", required_field_rows_removed),
    ("duplicate_event_id_rows_removed", duplicate_event_id_rows_removed),
    ("clean_rows", clean_count),
]

summary_df = spark.createDataFrame(summary_rows, ["metric", "value"])

summary_df.write.mode("overwrite").csv(
    str(TASK1_VALIDATION_DIR / "summary_csv"),
    header=True
)

event_type_counts.write.mode("overwrite").csv(
    str(TASK1_VALIDATION_DIR / "event_type_counts_csv"),
    header=True
)

null_counts.write.mode("overwrite").csv(
    str(TASK1_VALIDATION_DIR / "null_counts_csv"),
    header=True
)

event_type_null_profile.write.mode("overwrite").csv(
    str(TASK1_VALIDATION_DIR / "event_type_null_profile_csv"),
    header=True
)


print("=" * 100)
print("OUTPUT WRITTEN")
print(f"Cleaned events Parquet: {EVENTS_PROCESSED_PATH}")
print(f"Validation output: {TASK1_VALIDATION_DIR}")
print("=" * 100)
print("TASK 1 EVENTS CLEANING COMPLETED")

spark.stop()
