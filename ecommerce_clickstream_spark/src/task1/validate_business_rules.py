from pyspark.sql import functions as F

from common.config import BUSINESS_VALIDATION_DIR, PROCESSED_DIR
from common.spark_utils import create_spark_session


def violation_count(df, condition):
    return df.filter(condition).count()


def main():
    BUSINESS_VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session("Task1BusinessValidation")

    try:
        events = spark.read.parquet(str(PROCESSED_DIR / "events_cleaned_parquet"))
        customers = spark.read.parquet(
            str(PROCESSED_DIR / "customers_cleaned_parquet")
        )
        sessions = spark.read.parquet(
            str(PROCESSED_DIR / "sessions_cleaned_parquet")
        )
        products = spark.read.parquet(str(PROCESSED_DIR / "products_cleaned_parquet"))
        orders = spark.read.parquet(str(PROCESSED_DIR / "orders_cleaned_parquet"))
        order_items = spark.read.parquet(
            str(PROCESSED_DIR / "order_items_cleaned_parquet")
        )
        reviews = spark.read.parquet(str(PROCESSED_DIR / "reviews_cleaned_parquet"))

        allowed_events = ["page_view", "add_to_cart", "checkout", "purchase"]
        checks = [
            (
                "customers",
                "age_between_18_and_100",
                violation_count(customers, ~F.col("age").between(18, 100)),
            ),
            (
                "events",
                "event_type_allowed",
                violation_count(events, ~F.col("event_type").isin(allowed_events)),
            ),
            (
                "events",
                "quantity_positive_when_present",
                violation_count(
                    events,
                    F.col("qty_int").isNotNull() & (F.col("qty_int") <= 0),
                ),
            ),
            (
                "events",
                "cart_size_non_negative_when_present",
                violation_count(
                    events,
                    F.col("cart_size_int").isNotNull()
                    & (F.col("cart_size_int") < 0),
                ),
            ),
            (
                "events",
                "discount_between_0_and_100",
                violation_count(
                    events,
                    F.col("discount_pct").isNotNull()
                    & ~F.col("discount_pct").between(0, 100),
                ),
            ),
            (
                "events",
                "amount_non_negative",
                violation_count(
                    events,
                    F.col("amount_usd").isNotNull() & (F.col("amount_usd") < 0),
                ),
            ),
            (
                "products",
                "price_and_cost_non_negative",
                violation_count(
                    products,
                    (F.col("price_usd") < 0) | (F.col("cost_usd") < 0),
                ),
            ),
            (
                "products",
                "margin_equals_price_minus_cost",
                violation_count(
                    products,
                    F.abs(
                        F.col("margin_usd")
                        - (F.col("price_usd") - F.col("cost_usd"))
                    )
                    > 0.02,
                ),
            ),
            (
                "orders",
                "money_non_negative",
                violation_count(
                    orders,
                    (F.col("subtotal_usd") < 0) | (F.col("total_usd") < 0),
                ),
            ),
            (
                "orders",
                "discount_between_0_and_100",
                violation_count(orders, ~F.col("discount_pct").between(0, 100)),
            ),
            (
                "orders",
                "total_matches_discounted_subtotal",
                violation_count(
                    orders,
                    F.abs(
                        F.col("total_usd")
                        - F.col("subtotal_usd")
                        * (1 - F.col("discount_pct") / F.lit(100))
                    )
                    > 0.02,
                ),
            ),
            (
                "order_items",
                "quantity_and_money_valid",
                violation_count(
                    order_items,
                    (F.col("quantity") <= 0)
                    | (F.col("unit_price_usd") < 0)
                    | (F.col("line_total_usd") < 0),
                ),
            ),
            (
                "order_items",
                "line_total_matches_price_times_quantity",
                violation_count(
                    order_items,
                    F.abs(
                        F.col("line_total_usd")
                        - F.col("unit_price_usd") * F.col("quantity")
                    )
                    > 0.02,
                ),
            ),
            (
                "reviews",
                "rating_between_1_and_5",
                violation_count(reviews, ~F.col("rating").between(1, 5)),
            ),
            (
                "events",
                "event_not_before_session_start",
                violation_count(
                    events.join(
                        sessions.select("session_id", "start_time"),
                        "session_id",
                        "inner",
                    ),
                    F.col("event_timestamp") < F.col("start_time"),
                ),
            ),
        ]

        result = spark.createDataFrame(
            [
                (table, rule, count, count == 0)
                for table, rule, count in checks
            ],
            ["table_name", "rule", "violation_count", "passed"],
        )

        result.orderBy("table_name", "rule").show(100, truncate=False)
        (
            result.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(BUSINESS_VALIDATION_DIR / "rules_csv"))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
