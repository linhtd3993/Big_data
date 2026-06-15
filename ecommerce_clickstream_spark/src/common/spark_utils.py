from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """Tao SparkSession dung chung cho cac script xu ly local."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
