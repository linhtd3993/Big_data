from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# TASK 2.1 — MOST ACTIVE USERS
# ============================================================
# Ý tưởng:
# - Bảng events ghi lại hành vi người dùng, nhưng không có trực tiếp customer_id.
# - Muốn biết user nào hoạt động nhiều nhất, ta cần nối:
#
#   events.session_id -> sessions.session_id -> sessions.customer_id
#
# - Sau khi có customer_id, ta group by theo customer_id.
# - Sau đó tính:
#   + tổng số event của mỗi user
#   + tổng số session của mỗi user
#   + số lần page_view
#   + số lần add_to_cart
#   + số lần checkout
#   + số lần purchase



def create_spark_session():
    """
    Tạo SparkSession cho phần Most Active Users.
    """
    spark = (
        SparkSession.builder
        .appName("Task2_1_Most_Active_Users")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_paths():
    """
    Lấy đường dẫn project root, input và output.

    File hiện tại nằm trong:
    src/task2/active_users_spark.py

    Project root là:
    ecommerce_clickstream_spark/
    """
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    output_dir = project_root / "data" / "output" / "task2_metrics" / "most_active_users"

    output_dir.mkdir(parents=True, exist_ok=True)

    return processed_dir, output_dir


def load_data(spark, processed_dir):
    """
    Load các bảng cần thiết cho phần most active users.

    Cần dùng:
    - events_cleaned_parquet: chứa hành vi người dùng
    - sessions_cleaned_parquet: chứa session_id và customer_id
    - customers_cleaned_parquet: chứa danh sách customer hợp lệ
    """
    events = spark.read.parquet(str(processed_dir / "events_cleaned_parquet"))
    sessions = spark.read.parquet(str(processed_dir / "sessions_cleaned_parquet"))
    customers = spark.read.parquet(str(processed_dir / "customers_cleaned_parquet"))

    return events, sessions, customers


def compute_most_active_users(events, sessions, customers):
    """
    Tính người dùng hoạt động nhiều nhất.

    Bước xử lý:
    1. Join events với sessions bằng session_id để lấy customer_id.
    2. Join tiếp với customers để đảm bảo customer_id là hợp lệ.
    3. Group by customer_id.
    4. Tính các chỉ số hoạt động.
    5. Sắp xếp giảm dần theo total_events.
    """

    # Bước 1 + 2:
    # Gắn mỗi event với customer_id thông qua session_id.
    events_with_users = (
        events.alias("e")
        .join(
            sessions.select("session_id", "customer_id").alias("s"),
            on="session_id",
            how="inner"
        )
        .join(
            customers.select("customer_id").alias("c"),
            on="customer_id",
            how="inner"
        )
    )

    # Bước 3 + 4 + 5:
    # Group by theo customer_id và tính các metrics.
    most_active_users = (
        events_with_users
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("session_id").alias("total_sessions"),

            # Đếm số lần user xem sản phẩm/trang.
            F.sum(
                F.when(F.col("event_type") == "page_view", 1).otherwise(0)
            ).alias("page_view_count"),

            # Đếm số lần user thêm sản phẩm vào giỏ.
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("add_to_cart_count"),

            # Đếm số lần user checkout.
            F.sum(
                F.when(F.col("event_type") == "checkout", 1).otherwise(0)
            ).alias("checkout_count"),

            # Đếm số lần user purchase.
            F.sum(
                F.when(F.col("event_type") == "purchase", 1).otherwise(0)
            ).alias("purchase_count"),
        )
        .orderBy(F.desc("total_events"))
    )

    return most_active_users


def write_output(df, output_dir):
    """
    Ghi kết quả ra CSV.

    Lưu ý:
    - Spark ghi CSV thành một folder.
    - coalesce(1) giúp kết quả chỉ có 1 file part CSV để dễ xem hơn.
    """
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

        print("========== TASK 2.1: MOST ACTIVE USERS ==========")
        print(f"Input path: {processed_dir}")
        print(f"Output path: {output_dir}")

        print("\n[1] Loading data...")
        events, sessions, customers = load_data(spark, processed_dir)

        print("\n[2] Computing most active users...")
        result = compute_most_active_users(events, sessions, customers)

        print("\n[3] Preview result:")
        result.show(10, truncate=False)

        print("\n[4] Writing output...")
        write_output(result, output_dir)

        print("\nTask 2.1 completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()