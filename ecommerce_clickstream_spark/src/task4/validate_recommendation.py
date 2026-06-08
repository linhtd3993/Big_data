from common.config import PROCESSED_DIR
from common.spark_utils import create_spark_session

# Khoi tao Spark Session
spark = create_spark_session("Task4ValidateRecommendation")

print("=" * 80)
print("TASK 4 - VALIDATION OF RECOMMENDATION OUTPUT")
print("=" * 80)

output_path = PROCESSED_DIR / "recommendations_output"

try:
    # Doc du lieu goi y da duoc luu dang Parquet
    recs_df = spark.read.parquet(str(output_path))
    
    # Lay tong so dong
    total_recs = recs_df.count()
    print(f"Tong so dong trong ket qua goi y: {total_recs}")
    
    print("\nCau truc schema cua ket qua goi y:")
    recs_df.printSchema()
    
    # Hien thi vi du goi y cua 5 khach hang dau tien
    print("\nVi du goi y cho 5 khach hang dau tien:")
    sample_customers = recs_df.select("customer_id").distinct().limit(5).collect()
    customer_ids = [row.customer_id for row in sample_customers]
    
    filtered_df = recs_df.filter(recs_df.customer_id.isin(customer_ids))
    filtered_df.orderBy("customer_id", recs_df.predicted_rating.desc()).show(30, truncate=False)
    
except Exception as e:
    print(f"Loi khi doc ket qua goi y: {e}")
    print("Hay dam bao rang ban da chay file train_recommendation.py truoc.")

print("=" * 80)
print("KET THUC KIEM TRA GOI Y TASK 4")
print("=" * 80)

spark.stop()
