from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# TASK 2.2 — TOP PRODUCTS
# ============================================================
# Ý tưởng:
# - Mục tiêu là tìm sản phẩm nổi bật nhất dựa trên hành vi clickstream.
# - Bảng events có product_id_int, cho biết event đó liên quan đến sản phẩm nào.
# - Bảng products có thông tin sản phẩm.
# - Ta join:
#   events.product_id_int -> products.product_id
# - Sau khi join, group by theo product_id.
# - Sau đó tính:
#   + total_product_events: tổng số event liên quan đến sản phẩm
#   + view_count: số lượt xem sản phẩm
#   + add_to_cart_count: số lượt thêm sản phẩm vào giỏ
#   + purchase_event_count: số event mua hàng liên quan đến sản phẩm
#
# Output:
# - data/output/task2_metrics/top_products/
# ============================================================


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Task2_2_Top_Products")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_paths():
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    output_dir = project_root / "data" / "output" / "task2_metrics" / "top_products"

    output_dir.mkdir(parents=True, exist_ok=True)

    return processed_dir, output_dir


def load_data(spark, processed_dir):
    events = spark.read.parquet(str(processed_dir / "events_cleaned_parquet"))
    products = spark.read.parquet(str(processed_dir / "products_cleaned_parquet"))

    return events, products


def compute_top_products(events, products):
    # Chỉ lấy các event có liên quan đến sản phẩm.
    product_events = (
        events
        .filter(F.col("product_id_int").isNotNull())
        .alias("e")
        .join(
            products.alias("p"),
            F.col("e.product_id_int") == F.col("p.product_id"),
            how="inner"
        )
    )

    # Chọn các cột cần xuất.
    # product_id chắc chắn cần có.
    select_columns = [
        F.col("p.product_id").alias("product_id"),
        F.col("e.event_type").alias("event_type"),
    ]

    # Nếu products có product_name thì xuất thêm.
    if "product_name" in products.columns:
        select_columns.append(F.col("p.product_name").alias("product_name"))

    # Nếu products có category thì xuất thêm.
    if "category" in products.columns:
        select_columns.append(F.col("p.category").alias("category"))

    product_events_selected = product_events.select(*select_columns)

    # Các cột dùng để group by là các cột mô tả sản phẩm, bỏ event_type.
    group_cols = [
        col_name
        for col_name in product_events_selected.columns
        if col_name != "event_type"
    ]

    result = (
        product_events_selected
        .groupBy(*group_cols)
        .agg(
            F.count("*").alias("total_product_events"),

            F.sum(
                F.when(F.col("event_type") == "page_view", 1).otherwise(0)
            ).alias("view_count"),

            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("add_to_cart_count"),

            F.sum(
                F.when(F.col("event_type") == "purchase", 1).otherwise(0)
            ).alias("purchase_event_count"),
        )
        .orderBy(F.desc("total_product_events"))
    )

    return result


def write_output(df, output_dir):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(output_dir))
    )


def main():
    spark = create_spark_session()

    try:
        processed_dir, output_dir = get_paths()

        print("========== TASK 2.2: TOP PRODUCTS ==========")
        print(f"Input path: {processed_dir}")
        print(f"Output path: {output_dir}")

        print("\n[1] Loading data...")
        events, products = load_data(spark, processed_dir)

        print("\n[2] Computing top products...")
        result = compute_top_products(events, products)

        print("\n[3] Preview result:")
        result.show(10, truncate=False)

        print("\n[4] Writing output...")
        write_output(result, output_dir)

        print("\nTask 2.2 completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()