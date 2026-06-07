from pyspark.sql import functions as F

from src.common.config import PROCESSED_DIR, RELATIONSHIP_VALIDATION_DIR
from src.common.spark_utils import create_spark_session


# Thu muc luu ket qua validate quan he giua cac bang da clean.
RELATIONSHIP_VALIDATION_DIR.mkdir(parents=True, exist_ok=True)


spark = create_spark_session("Task1ValidateRelationships")


# Doc cac bang Parquet da clean tu data/processed.
events = spark.read.parquet(str(PROCESSED_DIR / "events_cleaned_parquet"))
customers = spark.read.parquet(str(PROCESSED_DIR / "customers_cleaned_parquet"))
sessions = spark.read.parquet(str(PROCESSED_DIR / "sessions_cleaned_parquet"))
products = spark.read.parquet(str(PROCESSED_DIR / "products_cleaned_parquet"))
orders = spark.read.parquet(str(PROCESSED_DIR / "orders_cleaned_parquet"))
order_items = spark.read.parquet(str(PROCESSED_DIR / "order_items_cleaned_parquet"))


def count_left_unmatched(left_df, right_df, left_key, right_key):
    """Dem so khoa ben trai khong tim thay trong bang ben phai."""
    return (
        left_df.alias("l")
        .join(
            right_df.alias("r"),
            F.col(f"l.{left_key}") == F.col(f"r.{right_key}"),
            "left_anti"
        )
        .count()
    )


checks = []

# Ghi lai tong so dong cua tung bang truoc khi kiem tra khoa ngoai.
events_total = events.count()
sessions_total = sessions.count()
customers_total = customers.count()
products_total = products.count()
orders_total = orders.count()
order_items_total = order_items.count()

checks.append(("events_total_rows", events_total))
checks.append(("sessions_total_rows", sessions_total))
checks.append(("customers_total_rows", customers_total))
checks.append(("products_total_rows", products_total))
checks.append(("orders_total_rows", orders_total))
checks.append(("order_items_total_rows", order_items_total))


# Quan he 1: events.session_id -> sessions.session_id.
events_session_unmatched = count_left_unmatched(
    events.select("session_id").dropDuplicates(),
    sessions.select("session_id").dropDuplicates(),
    "session_id",
    "session_id"
)
checks.append(("unmatched_events_session_id_to_sessions", events_session_unmatched))


# Quan he 2: events.product_id_int -> products.product_id.
# Chi kiem tra cac event co product vi checkout/purchase co the null product_id trong bang events.
events_with_product = events.filter(F.col("product_id_int").isNotNull())

events_product_unmatched = count_left_unmatched(
    events_with_product.select("product_id_int").dropDuplicates(),
    products.select("product_id").dropDuplicates(),
    "product_id_int",
    "product_id"
)
checks.append(("unmatched_events_product_id_to_products", events_product_unmatched))


# Quan he 3: sessions.customer_id -> customers.customer_id.
sessions_customer_unmatched = count_left_unmatched(
    sessions.select("customer_id").dropDuplicates(),
    customers.select("customer_id").dropDuplicates(),
    "customer_id",
    "customer_id"
)
checks.append(("unmatched_sessions_customer_id_to_customers", sessions_customer_unmatched))


# Quan he 4: orders.customer_id -> customers.customer_id.
orders_customer_unmatched = count_left_unmatched(
    orders.select("customer_id").dropDuplicates(),
    customers.select("customer_id").dropDuplicates(),
    "customer_id",
    "customer_id"
)
checks.append(("unmatched_orders_customer_id_to_customers", orders_customer_unmatched))


# Quan he 5: order_items.order_id -> orders.order_id.
order_items_order_unmatched = count_left_unmatched(
    order_items.select("order_id").dropDuplicates(),
    orders.select("order_id").dropDuplicates(),
    "order_id",
    "order_id"
)
checks.append(("unmatched_order_items_order_id_to_orders", order_items_order_unmatched))


# Quan he 6: order_items.product_id -> products.product_id.
order_items_product_unmatched = count_left_unmatched(
    order_items.select("product_id").dropDuplicates(),
    products.select("product_id").dropDuplicates(),
    "product_id",
    "product_id"
)
checks.append(("unmatched_order_items_product_id_to_products", order_items_product_unmatched))


# Chuyen danh sach metric thanh DataFrame de vua in ra console vua ghi thanh CSV.
result_df = spark.createDataFrame(
    [(metric, str(value)) for metric, value in checks],
    ["metric", "value"]
)

print("=" * 100)
print("TASK 1 RELATIONSHIP VALIDATION")
result_df.show(100, truncate=False)

result_df.write.mode("overwrite").csv(
    str(RELATIONSHIP_VALIDATION_DIR / "relationship_summary_csv"),
    header=True
)

print("=" * 100)
print(f"Relationship validation written to: {RELATIONSHIP_VALIDATION_DIR}")

spark.stop()
