from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# TASK 2.3 — PEAK ACTIVITY TIME
# ============================================================
# Ý tưởng:
# - Mục tiêu là tìm thời điểm website có nhiều hoạt động nhất.
# - Bảng events chứa toàn bộ hành vi người dùng.
# - Task 1 đã tạo sẵn các cột thời gian:
#   + event_hour: giờ xảy ra event
#   + day_of_week: ngày trong tuần
#   + event_date: ngày cụ thể
# - Ta group by theo từng cột thời gian.
# - Sau đó đếm số event để biết thời điểm nào cao điểm nhất.
# Output:
# - data/output/task2_metrics/peak_activity_by_hour/
# - data/output/task2_metrics/peak_activity_by_day/
# - data/output/task2_metrics/peak_activity_by_date/
# ============================================================


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Task2_3_Peak_Activity_Time")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_paths():
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    output_base_dir = project_root / "data" / "output" / "task2_metrics"

    output_base_dir.mkdir(parents=True, exist_ok=True)

    return processed_dir, output_base_dir


def load_data(spark, processed_dir):
    events = spark.read.parquet(str(processed_dir / "events_cleaned_parquet"))
    return events


def compute_peak_by_hour(events):
    # Group theo giờ để tìm giờ có nhiều hoạt động nhất.
    return (
        events
        .groupBy("event_hour")
        .agg(F.count("*").alias("total_events"))
        .orderBy(F.desc("total_events"))
    )


def compute_peak_by_day(events):
    # Group theo ngày trong tuần để tìm ngày có nhiều hoạt động nhất.
    return (
        events
        .groupBy("day_of_week")
        .agg(F.count("*").alias("total_events"))
        .orderBy(F.desc("total_events"))
    )


def compute_peak_by_date(events):
    # Group theo ngày cụ thể để tìm ngày có nhiều event nhất.
    return (
        events
        .groupBy("event_date")
        .agg(F.count("*").alias("total_events"))
        .orderBy(F.desc("total_events"))
    )


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
        processed_dir, output_base_dir = get_paths()

        print("========== TASK 2.3: PEAK ACTIVITY TIME ==========")
        print(f"Input path: {processed_dir}")
        print(f"Output base path: {output_base_dir}")

        print("\n[1] Loading data...")
        events = load_data(spark, processed_dir)

        print("\n[2] Computing peak activity by hour...")
        peak_by_hour = compute_peak_by_hour(events)
        peak_by_hour.show(10, truncate=False)
        write_output(peak_by_hour, output_base_dir / "peak_activity_by_hour")

        print("\n[3] Computing peak activity by day...")
        peak_by_day = compute_peak_by_day(events)
        peak_by_day.show(10, truncate=False)
        write_output(peak_by_day, output_base_dir / "peak_activity_by_day")

        print("\n[4] Computing peak activity by date...")
        peak_by_date = compute_peak_by_date(events)
        peak_by_date.show(10, truncate=False)
        write_output(peak_by_date, output_base_dir / "peak_activity_by_date")

        print("\nTask 2.3 completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()