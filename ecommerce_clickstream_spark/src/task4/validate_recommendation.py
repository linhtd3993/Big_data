from pathlib import Path

from pyspark.sql import functions as F

from common.config import TASK4_OUTPUT_DIR
from common.spark_utils import create_spark_session


def main():
    spark = create_spark_session("Task4ValidateRecommendation")

    try:
        recommendations = spark.read.parquet(
            str(TASK4_OUTPUT_DIR / "recommendations_parquet")
        )
        metrics = spark.read.option("header", True).option(
            "inferSchema", True
        ).csv(str(TASK4_OUTPUT_DIR / "evaluation_metrics"))

        checks = [
            (
                "recommendations_not_empty",
                recommendations.count() > 0,
                str(recommendations.count()),
            ),
            (
                "rank_between_1_and_10",
                recommendations.filter(~F.col("rank").between(1, 10)).count() == 0,
                str(
                    recommendations.filter(
                        ~F.col("rank").between(1, 10)
                    ).count()
                ),
            ),
            (
                "unique_user_rank",
                recommendations.groupBy("customer_id", "rank")
                .count()
                .filter(F.col("count") > 1)
                .count()
                == 0,
                "duplicate user-rank pairs",
            ),
            (
                "four_metrics_per_model",
                metrics.groupBy("model_name")
                .count()
                .filter(F.col("count") != 4)
                .count()
                == 0,
                "Precision, Recall, MAP, NDCG",
            ),
            (
                "metrics_in_valid_range",
                metrics.filter(~F.col("value").between(0.0, 1.0)).count() == 0,
                "expected [0, 1]",
            ),
            (
                "metrics_image_exists",
                Path(
                    TASK4_OUTPUT_DIR / "images" / "model_metrics_comparison.svg"
                ).exists(),
                "model_metrics_comparison.svg",
            ),
            (
                "interaction_image_exists",
                Path(
                    TASK4_OUTPUT_DIR / "images" / "interaction_distribution.svg"
                ).exists(),
                "interaction_distribution.svg",
            ),
        ]

        result = spark.createDataFrame(
            checks, ["check_name", "passed", "details"]
        )
        result.show(100, truncate=False)
        (
            result.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(TASK4_OUTPUT_DIR / "validation"))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
