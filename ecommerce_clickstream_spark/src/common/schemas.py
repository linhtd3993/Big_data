from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Schema duoc khai bao thu cong de Spark doc CSV on dinh va khong phu thuoc inferSchema.
# Luu y: thu tu field phai trung voi thu tu cot trong file CSV raw.
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("signup_date", DateType(), True),
    StructField("marketing_opt_in", BooleanType(), True),
])

# sessions.csv chi co 6 cot; khong them end_time neu raw data khong co cot nay.
SESSIONS_SCHEMA = StructType([
    StructField("session_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("device", StringType(), True),
    StructField("source", StringType(), True),
    StructField("country", StringType(), True),
])

EVENTS_SCHEMA = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("session_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", DoubleType(), True),
    StructField("qty", DoubleType(), True),
    StructField("cart_size", DoubleType(), True),
    StructField("payment", StringType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("amount_usd", DoubleType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("cost_usd", DoubleType(), True),
    StructField("margin_usd", DoubleType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_time", TimestampType(), True),
    StructField("payment_method", StringType(), True),
    StructField("discount_pct", IntegerType(), True),
    StructField("subtotal_usd", DoubleType(), True),
    StructField("total_usd", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    StructField("source", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("unit_price_usd", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("line_total_usd", DoubleType(), True),
])

REVIEWS_SCHEMA = StructType([
    StructField("review_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_text", StringType(), True),
    StructField("review_time", TimestampType(), True),
])

SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "sessions": SESSIONS_SCHEMA,
    "events": EVENTS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
}
