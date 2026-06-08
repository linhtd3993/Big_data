from common.spark_utils import create_spark_session
from common.config import PROCESSED_DIR

spark = create_spark_session("check_schema")

products = spark.read.parquet(
    f"{PROCESSED_DIR}/products_cleaned_parquet"
)

order_items = spark.read.parquet(
    f"{PROCESSED_DIR}/order_items_cleaned_parquet"
)

events = spark.read.parquet(
    f"{PROCESSED_DIR}/events_cleaned_parquet"
)

print("\n=== PRODUCTS ===")
products.printSchema()

print("\n=== ORDER ITEMS ===")
order_items.printSchema()

print("\n=== EVENTS ===")
events.printSchema()

spark.stop()