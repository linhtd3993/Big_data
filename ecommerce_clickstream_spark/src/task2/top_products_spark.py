from pyspark.sql import functions as F

from common.config import PROCESSED_DIR, TASK2_OUTPUT_DIR
from common.spark_utils import create_spark_session


OUTPUT_DIR = TASK2_OUTPUT_DIR / "top_products"


def load_data(spark):
    events = spark.read.parquet(str(PROCESSED_DIR / "events_cleaned_parquet"))
    products = spark.read.parquet(str(PROCESSED_DIR / "products_cleaned_parquet"))
    orders = spark.read.parquet(str(PROCESSED_DIR / "orders_cleaned_parquet"))
    order_items = spark.read.parquet(
        str(PROCESSED_DIR / "order_items_cleaned_parquet")
    )
    return events, products, orders, order_items


def compute_top_products(events, products, orders, order_items):
    engagement = (
        events.filter(F.col("product_id_int").isNotNull())
        .groupBy(F.col("product_id_int").alias("product_id"))
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias(
                "view_count"
            ),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("add_to_cart_count"),
        )
    )

    purchases = (
        order_items.join(
            orders.select("order_id", "customer_id"), "order_id", "inner"
        )
        .groupBy("product_id")
        .agg(
            F.countDistinct("order_id").alias("purchase_order_count"),
            F.countDistinct("customer_id").alias("purchasing_customer_count"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total_usd"), 2).alias("gross_revenue_usd"),
        )
    )

    return (
        products.select(
            "product_id",
            F.col("name").alias("product_name"),
            "category",
            "price_usd",
            "margin_usd",
        )
        .join(engagement, "product_id", "left")
        .join(purchases, "product_id", "left")
        .fillna(
            0,
            subset=[
                "view_count",
                "add_to_cart_count",
                "purchase_order_count",
                "purchasing_customer_count",
                "units_sold",
                "gross_revenue_usd",
            ],
        )
        .withColumn(
            "view_to_cart_rate",
            F.when(
                F.col("view_count") > 0,
                F.round(F.col("add_to_cart_count") / F.col("view_count"), 4),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(
                F.col("add_to_cart_count") > 0,
                F.round(
                    F.col("purchase_order_count") / F.col("add_to_cart_count"), 4
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "engagement_score",
            F.col("view_count")
            + F.col("add_to_cart_count") * 3
            + F.col("units_sold") * 5,
        )
        .orderBy(F.desc("engagement_score"), F.desc("gross_revenue_usd"))
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session("Task2TopProducts")

    try:
        result = compute_top_products(*load_data(spark))
        result.show(20, truncate=False)
        (
            result.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(OUTPUT_DIR))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
