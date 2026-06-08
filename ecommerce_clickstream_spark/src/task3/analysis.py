from common.spark_utils import create_spark_session
from pyspark.sql.functions import count, avg, min, max

spark = create_spark_session("task3-category-analysis")

products = spark.read.parquet("data/processed/products_cleaned_parquet")

result = products.groupBy("category").agg(
    count("product_id").alias("product_count"),
    avg("price_usd").alias("avg_price"),
    min("price_usd").alias("min_price"),
    max("price_usd").alias("max_price"),
    avg("margin_usd").alias("avg_margin")
)

result.show(truncate=False)