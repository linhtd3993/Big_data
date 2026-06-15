from pathlib import Path
from xml.sax.saxutils import escape

from pyspark.ml.evaluation import RankingEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import PROCESSED_DIR, TASK4_OUTPUT_DIR
from common.spark_utils import create_spark_session


TOP_K = 10
CANDIDATE_K = 100


def load_data(spark):
    names = ["events", "sessions", "orders", "order_items", "products"]
    return {
        name: spark.read.parquet(
            str(PROCESSED_DIR / f"{name}_cleaned_parquet")
        )
        for name in names
    }


def build_interactions(data):
    clickstream = (
        data["events"]
        .filter(
            F.col("product_id_int").isNotNull()
            & F.col("event_type").isin("page_view", "add_to_cart")
        )
        .join(
            data["sessions"].select("session_id", "customer_id"),
            "session_id",
            "inner",
        )
        .select(
            F.col("customer_id").cast("int"),
            F.col("product_id_int").cast("int").alias("product_id"),
            F.col("event_timestamp").alias("interaction_timestamp"),
            F.when(F.col("event_type") == "page_view", F.lit(1.0))
            .otherwise(F.lit(3.0))
            .alias("score"),
            F.col("event_type").alias("interaction_type"),
        )
    )

    purchases = (
        data["orders"]
        .select("order_id", "customer_id", "order_time")
        .join(data["order_items"], "order_id", "inner")
        .select(
            F.col("customer_id").cast("int"),
            F.col("product_id").cast("int"),
            F.col("order_time").alias("interaction_timestamp"),
            (F.lit(5.0) * F.col("quantity")).alias("score"),
            F.lit("purchase").alias("interaction_type"),
        )
    )

    detailed = clickstream.unionByName(purchases)
    aggregated = (
        detailed.groupBy("customer_id", "product_id")
        .agg(
            F.sum("score").cast("float").alias("rating"),
            F.max("interaction_timestamp").alias("last_interaction_timestamp"),
            F.count("*").alias("interaction_count"),
            F.max(
                F.when(F.col("interaction_type") == "purchase", 1).otherwise(0)
            ).alias("has_purchase"),
        )
        .filter(F.col("rating") > 0)
    )
    return detailed, aggregated


def temporal_split(interactions):
    user_window = Window.partitionBy("customer_id")
    newest_first = user_window.orderBy(
        F.desc("last_interaction_timestamp"), F.desc("product_id")
    )

    ranked = (
        interactions.withColumn("user_item_count", F.count("*").over(user_window))
        .filter(F.col("user_item_count") >= 2)
        .withColumn(
            "test_item_count",
            F.greatest(
                F.lit(1),
                F.ceil(F.col("user_item_count") * F.lit(0.2)).cast("int"),
            ),
        )
        .withColumn("recency_rank", F.row_number().over(newest_first))
    )

    test = ranked.filter(F.col("recency_rank") <= F.col("test_item_count"))
    train = ranked.filter(F.col("recency_rank") > F.col("test_item_count"))
    return train.drop("recency_rank"), test.drop("recency_rank")


def fit_als(train):
    als = ALS(
        rank=20,
        maxIter=12,
        regParam=0.08,
        alpha=1.0,
        userCol="customer_id",
        itemCol="product_id",
        ratingCol="rating",
        implicitPrefs=True,
        nonnegative=True,
        coldStartStrategy="drop",
        seed=42,
    )
    return als.fit(train)


def remove_seen_and_rank(candidates, train_pairs, score_column):
    rank_window = Window.partitionBy("customer_id").orderBy(
        F.desc(score_column), F.asc("product_id")
    )
    return (
        candidates.join(
            train_pairs,
            ["customer_id", "product_id"],
            "left_anti",
        )
        .withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") <= TOP_K)
    )


def als_recommendations(model, train):
    raw = (
        model.recommendForAllUsers(CANDIDATE_K)
        .select("customer_id", F.explode("recommendations").alias("rec"))
        .select(
            "customer_id",
            F.col("rec.product_id").alias("product_id"),
            F.col("rec.rating").alias("recommendation_score"),
        )
    )
    train_pairs = train.select("customer_id", "product_id").distinct()
    return remove_seen_and_rank(raw, train_pairs, "recommendation_score")


def popularity_recommendations(train, evaluation_users):
    popularity = (
        train.groupBy("product_id")
        .agg(F.sum("rating").alias("popularity_score"))
        .orderBy(F.desc("popularity_score"))
        .limit(CANDIDATE_K)
    )
    candidates = evaluation_users.crossJoin(popularity)
    train_pairs = train.select("customer_id", "product_id").distinct()
    return remove_seen_and_rank(candidates, train_pairs, "popularity_score")


def ranking_frame(recommendations, test):
    predicted = (
        recommendations.groupBy("customer_id")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("rank", "product_id"))
            ).alias("ranked_items")
        )
        .select(
            "customer_id",
            F.expr(
                "transform(ranked_items, x -> cast(x.product_id as double))"
            ).alias(
                "prediction"
            ),
        )
    )
    labels = test.groupBy("customer_id").agg(
        F.collect_set(F.col("product_id").cast("double")).alias("label")
    )
    return predicted.join(labels, "customer_id", "inner")


def evaluate_model(name, recommendations, test):
    evaluation = ranking_frame(recommendations, test).cache()
    metric_names = [
        ("precisionAtK", "precision_at_k"),
        ("recallAtK", "recall_at_k"),
        ("meanAveragePrecisionAtK", "map_at_k"),
        ("ndcgAtK", "ndcg_at_k"),
    ]
    rows = []
    for spark_metric, output_metric in metric_names:
        evaluator = RankingEvaluator(
            predictionCol="prediction",
            labelCol="label",
            metricName=spark_metric,
            k=TOP_K,
        )
        rows.append((name, output_metric, TOP_K, float(evaluator.evaluate(evaluation))))
    evaluation.unpersist()
    return rows


def enrich_recommendations(recommendations, products):
    score_col = (
        "recommendation_score"
        if "recommendation_score" in recommendations.columns
        else "popularity_score"
    )
    return (
        recommendations.join(
            products.select(
                "product_id",
                F.col("name").alias("product_name"),
                "category",
            ),
            "product_id",
            "inner",
        )
        .select(
            "customer_id",
            "product_id",
            "product_name",
            "category",
            F.col(score_col).alias("recommendation_score"),
            "rank",
        )
        .orderBy("customer_id", "rank")
    )


def write_svg_bar_chart(path, title, labels, series):
    width, height = 960, 540
    left, right, top, bottom = 90, 30, 70, 110
    plot_width = width - left - right
    plot_height = height - top - bottom
    all_values = [value for _, values, _ in series for value in values]
    max_value = max(all_values) if all_values else 1
    max_value = max(max_value, 1e-9)
    group_width = plot_width / max(len(labels), 1)
    bar_width = group_width / (len(series) + 1)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{width / 2}" y="36" text-anchor="middle" '
        f'font-family="Arial" font-size="24" font-weight="bold">{escape(title)}</text>',
        f'<line x1="{left}" y1="{top + plot_height}" x2="{width-right}" '
        f'y2="{top + plot_height}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_height}" stroke="#333"/>',
    ]

    for index, label in enumerate(labels):
        group_x = left + index * group_width
        for series_index, (series_name, values, color) in enumerate(series):
            value = values[index]
            bar_height = value / max_value * plot_height
            x = group_x + (series_index + 0.5) * bar_width
            y = top + plot_height - bar_height
            parts.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width * 0.8:.2f}" '
                f'height="{bar_height:.2f}" fill="{color}"/>'
            )
            parts.append(
                f'<text x="{x + bar_width * 0.4:.2f}" y="{max(y - 5, 15):.2f}" '
                f'text-anchor="middle" font-family="Arial" font-size="11">'
                f'{value:.4f}</text>'
            )
        parts.append(
            f'<text x="{group_x + group_width / 2:.2f}" y="{top + plot_height + 25}" '
            f'text-anchor="middle" font-family="Arial" font-size="13">'
            f'{escape(label)}</text>'
        )

    legend_x = left
    for series_name, _, color in series:
        parts.append(
            f'<rect x="{legend_x}" y="{height - 42}" width="16" height="16" fill="{color}"/>'
        )
        parts.append(
            f'<text x="{legend_x + 22}" y="{height - 29}" font-family="Arial" '
            f'font-size="13">{escape(series_name)}</text>'
        )
        legend_x += 190

    parts.append("</svg>")
    Path(path).write_text("\n".join(parts), encoding="utf-8")


def save_visualizations(metrics_df, detailed_interactions):
    image_dir = TASK4_OUTPUT_DIR / "images"
    image_dir.mkdir(parents=True, exist_ok=True)

    metric_rows = metrics_df.collect()
    metric_order = ["precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k"]
    by_model = {
        model: {
            row.metric: row.value
            for row in metric_rows
            if row.model_name == model
        }
        for model in ["ALS", "Popularity"]
    }
    write_svg_bar_chart(
        image_dir / "model_metrics_comparison.svg",
        f"Recommendation Model Metrics at K={TOP_K}",
        metric_order,
        [
            ("ALS", [by_model["ALS"].get(m, 0.0) for m in metric_order], "#2563eb"),
            (
                "Popularity",
                [by_model["Popularity"].get(m, 0.0) for m in metric_order],
                "#f59e0b",
            ),
        ],
    )

    source_rows = (
        detailed_interactions.groupBy("interaction_type")
        .count()
        .orderBy("interaction_type")
        .collect()
    )
    write_svg_bar_chart(
        image_dir / "interaction_distribution.svg",
        "Interaction Distribution Used for Training",
        [row.interaction_type for row in source_rows],
        [("Interactions", [float(row["count"]) for row in source_rows], "#10b981")],
    )


def write_dataframe(df, path, file_format="parquet"):
    writer = df.write.mode("overwrite")
    if file_format == "csv":
        writer.option("header", True).csv(str(path))
    else:
        writer.parquet(str(path))


def main():
    TASK4_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session("Task4RecommendationEndToEnd")

    try:
        data = load_data(spark)
        detailed, interactions = build_interactions(data)
        train, test = temporal_split(interactions)

        train = train.cache()
        test = test.cache()
        model = fit_als(train)

        als_recs = als_recommendations(model, train).cache()
        evaluation_users = test.select("customer_id").distinct()
        popularity_recs = popularity_recommendations(
            train, evaluation_users
        ).cache()

        metric_rows = evaluate_model("ALS", als_recs, test)
        metric_rows += evaluate_model("Popularity", popularity_recs, test)
        metrics = spark.createDataFrame(
            metric_rows, ["model_name", "metric", "k", "value"]
        )

        final_recommendations = enrich_recommendations(
            als_recs, data["products"]
        )
        split_summary = spark.createDataFrame(
            [
                ("detailed_interactions", detailed.count()),
                ("user_product_pairs", interactions.count()),
                ("train_pairs", train.count()),
                ("test_pairs", test.count()),
                ("evaluation_users", evaluation_users.count()),
                ("recommended_users", als_recs.select("customer_id").distinct().count()),
            ],
            ["metric", "value"],
        )

        metrics.orderBy("model_name", "metric").show(100, truncate=False)
        split_summary.show(100, truncate=False)
        final_recommendations.show(30, truncate=False)

        write_dataframe(interactions, TASK4_OUTPUT_DIR / "interactions")
        write_dataframe(train, TASK4_OUTPUT_DIR / "train")
        write_dataframe(test, TASK4_OUTPUT_DIR / "test")
        write_dataframe(
            final_recommendations,
            TASK4_OUTPUT_DIR / "recommendations_parquet",
        )
        write_dataframe(
            final_recommendations.coalesce(1),
            TASK4_OUTPUT_DIR / "recommendations_csv",
            "csv",
        )
        write_dataframe(
            metrics.coalesce(1), TASK4_OUTPUT_DIR / "evaluation_metrics", "csv"
        )
        write_dataframe(
            split_summary.coalesce(1), TASK4_OUTPUT_DIR / "split_summary", "csv"
        )
        model.write().overwrite().save(str(TASK4_OUTPUT_DIR / "als_model"))
        save_visualizations(metrics, detailed)

        als_recs.unpersist()
        popularity_recs.unpersist()
        train.unpersist()
        test.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
