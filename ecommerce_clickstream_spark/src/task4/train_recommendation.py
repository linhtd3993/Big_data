from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

from common.config import PROCESSED_DIR
from common.spark_utils import create_spark_session

# Khoi tao Spark Session
spark = create_spark_session("Task4TrainRecommendation")

print("=" * 80)
print("TASK 4 - TRAINING RECOMMENDATION MODEL (ALS)")
print("=" * 80)

# 1. Doc du lieu sach tu processed folder
events_df = spark.read.parquet(str(PROCESSED_DIR / "events_cleaned_parquet"))
sessions_df = spark.read.parquet(str(PROCESSED_DIR / "sessions_cleaned_parquet"))
products_df = spark.read.parquet(str(PROCESSED_DIR / "products_cleaned_parquet"))

# 2. Join events voi sessions de lay customer_id cho tung event
user_events_df = events_df.join(sessions_df, "session_id")

# 3. Gan diem so tuong tac (Implicit Ratings) dua tren loai event
# page_view = 1, add_to_cart = 2, purchase = 5
user_ratings = user_events_df.withColumn(
    "score",
    F.when(F.col("event_type") == "page_view", 1)
    .when(F.col("event_type") == "add_to_cart", 2)
    .when(F.col("event_type") == "purchase", 5)
    .otherwise(0)
)

# 4. Gom nhom theo customer_id va product_id_int de tinh tong diem tuong tac
dataset = (
    user_ratings
    .groupBy("customer_id", "product_id_int")
    .agg(F.sum("score").alias("rating"))
    .filter(F.col("product_id_int").isNotNull())
)

# 5. Chia du lieu thanh training va test set de danh gia mo hinh
(training, test) = dataset.randomSplit([0.8, 0.2], seed=42)

# 6. Cau hinh va huan luyen mo hinh ALS (Loc cong tac gian tiep)
als = ALS(
    maxIter=15,
    regParam=0.1,
    userCol="customer_id",
    itemCol="product_id_int",
    ratingCol="rating",
    implicitPrefs=True,
    coldStartStrategy="drop"
)

print("Dang huan luyen mo hinh ALS...")
model = als.fit(training)
print("Huan luyen hoan tat.")

# 7. Danh gia mo hinh bang RMSE (Root Mean Squared Error) tren tap test
predictions = model.transform(test)
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
try:
    rmse = evaluator.evaluate(predictions)
    print(f"Chi so RMSE tren tap test: {rmse:.4f}")
except Exception as e:
    print(f"Khong the tinh toan RMSE: {e}")

# 8. Sinh top 5 goi y san pham cho tat ca nguoi dung
print("Dang sinh top 5 goi y cho moi nguoi dung...")
user_recs = model.recommendForAllUsers(5)

# 9. Bien doi tu array struct sang dang bang phang de de doc hon
flat_recs = (
    user_recs
    .withColumn("rec", F.explode("recommendations"))
    .select(
        F.col("customer_id"),
        F.col("rec.product_id_int").alias("product_id"),
        F.col("rec.rating").alias("predicted_rating")
    )
)

# 10. Join voi bang products de lay thong tin chi tiet san pham
recommendations_with_info = (
    flat_recs
    .join(products_df, flat_recs.product_id == products_df.product_id, "inner")
    .select(
        flat_recs.customer_id,
        flat_recs.product_id,
        products_df.name.alias("product_name"),
        products_df.category.alias("product_category"),
        flat_recs.predicted_rating
    )
    .orderBy("customer_id", F.desc("predicted_rating"))
)

# 11. Luu ket qua goi y duoi dang Parquet
output_path = PROCESSED_DIR / "recommendations_output"
print(f"Dang ghi ket qua goi y vao: {output_path}")
recommendations_with_info.write.mode("overwrite").parquet(str(output_path))

print("=" * 80)
print("HOAN THANH CONG VIEC MO HINH GOI Y TASK 4")
print("=" * 80)

spark.stop()
