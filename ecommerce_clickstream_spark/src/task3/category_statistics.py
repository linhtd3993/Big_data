from pyspark.sql import functions as F
from pyspark.sql import Window

from common.config import PROCESSED_DIR, TASK3_OUTPUT_DIR
from common.spark_utils import create_spark_session


def load_data(spark):
    names = ["products", "events", "orders", "order_items", "reviews"]
    return {
        name: spark.read.parquet(
            str(PROCESSED_DIR / f"{name}_cleaned_parquet")
        )
        for name in names
    }


def compute_category_statistics(data):
    products = data["products"]

    catalog = products.groupBy("category").agg(
        F.countDistinct("product_id").alias("product_count"),
        F.round(F.avg("price_usd"), 2).alias("avg_price_usd"),
        F.round(F.min("price_usd"), 2).alias("min_price_usd"),
        F.round(F.max("price_usd"), 2).alias("max_price_usd"),
        F.round(F.avg("margin_usd"), 2).alias("avg_margin_usd"),
    )

    engagement = (
        data["events"]
        .filter(F.col("product_id_int").isNotNull())
        .join(
            products.select(
                F.col("product_id").alias("catalog_product_id"), "category"
            ),
            F.col("product_id_int") == F.col("catalog_product_id"),
            "inner",
        )
        .groupBy("category")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias(
                "view_count"
            ),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("add_to_cart_count"),
        )
    )

    sales = (
        data["order_items"]
        .join(data["orders"].select("order_id", "customer_id"), "order_id", "inner")
        .join(
            products.select("product_id", "category", "cost_usd"),
            "product_id",
            "inner",
        )
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("purchasing_customer_count"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total_usd"), 2).alias("gross_revenue_usd"),
            F.round(
                F.sum(
                    F.col("line_total_usd")
                    - F.col("cost_usd") * F.col("quantity")
                ),
                2,
            ).alias("estimated_gross_margin_usd"),
        )
    )

    review_stats = (
        data["reviews"]
        .join(products.select("product_id", "category"), "product_id", "inner")
        .groupBy("category")
        .agg(
            F.count("*").alias("review_count"),
            F.round(F.avg("rating"), 3).alias("avg_rating"),
        )
    )

    return (
        catalog.join(engagement, "category", "left")
        .join(sales, "category", "left")
        .join(review_stats, "category", "left")
        .fillna(0)
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
                F.round(F.col("order_count") / F.col("add_to_cart_count"), 4),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "revenue_rank",
            F.dense_rank().over(
                Window.orderBy(F.desc("gross_revenue_usd"))
            ),
        )
        .orderBy("revenue_rank", "category")
    )


def validate(result, data):
    totals = result.agg(
        F.sum("product_count").alias("result_products"),
        F.sum("units_sold").alias("result_units"),
        F.round(F.sum("gross_revenue_usd"), 2).alias("result_revenue"),
        F.sum("review_count").alias("result_reviews"),
    ).first()

    source_products = data["products"].select("product_id").distinct().count()
    source_units = data["order_items"].agg(F.sum("quantity")).first()[0]
    source_revenue = round(
        data["order_items"].agg(F.sum("line_total_usd")).first()[0], 2
    )
    source_reviews = data["reviews"].count()

    rows = [
        ("product_count", float(totals.result_products), float(source_products)),
        ("units_sold", float(totals.result_units), float(source_units)),
        ("gross_revenue_usd", float(totals.result_revenue), float(source_revenue)),
        ("review_count", float(totals.result_reviews), float(source_reviews)),
    ]
    return result.sparkSession.createDataFrame(
        [
            (metric, actual, expected, abs(actual - expected) <= 0.02)
            for metric, actual, expected in rows
        ],
        ["metric", "category_total", "source_total", "passed"],
    )


def main():
    TASK3_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session("Task3CategoryStatistics")

    try:
        data = load_data(spark)
        result = compute_category_statistics(data)
        validation = validate(result, data)

        result.show(100, truncate=False)
        validation.show(100, truncate=False)

        (
            result.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(TASK3_OUTPUT_DIR / "category_statistics"))
        )
        (
            validation.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(TASK3_OUTPUT_DIR / "validation"))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
