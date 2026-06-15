from pyspark.sql import functions as F

from common.config import TASK2_OUTPUT_DIR
from common.spark_utils import create_spark_session


EXPECTED_OUTPUTS = {
    "most_active_users": [
        "customer_id",
        "total_events",
        "total_sessions",
        "page_view_count",
        "add_to_cart_count",
        "checkout_count",
        "purchase_count",
    ],
    "top_products": [
        "product_id",
        "view_count",
        "add_to_cart_count",
        "purchase_order_count",
        "units_sold",
        "gross_revenue_usd",
    ],
    "peak_activity_by_hour": ["event_hour", "total_events"],
    "peak_activity_by_day": ["day_of_week", "total_events"],
    "peak_activity_by_date": ["event_date", "total_events"],
}


def main():
    spark = create_spark_session("Task2ValidateOutputs")
    checks = []

    try:
        for output_name, required_columns in EXPECTED_OUTPUTS.items():
            path = TASK2_OUTPUT_DIR / output_name
            df = spark.read.option("header", True).option("inferSchema", True).csv(
                str(path)
            )
            missing_columns = sorted(set(required_columns) - set(df.columns))
            row_count = df.count()
            null_key_count = df.filter(F.col(required_columns[0]).isNull()).count()
            checks.append(
                (
                    output_name,
                    row_count,
                    ",".join(missing_columns),
                    null_key_count,
                    row_count > 0 and not missing_columns and null_key_count == 0,
                )
            )

        result = spark.createDataFrame(
            checks,
            [
                "output_name",
                "row_count",
                "missing_columns",
                "null_key_count",
                "passed",
            ],
        )
        result.show(100, truncate=False)
        (
            result.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(TASK2_OUTPUT_DIR / "validation"))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
