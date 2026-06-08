import os
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:

    # Fix Windows Hadoop issue
    os.environ["HADOOP_HOME"] = os.getcwd()
    os.environ["hadoop.home.dir"] = os.getcwd()

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.io.nativeio.NativeIO", "false")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark