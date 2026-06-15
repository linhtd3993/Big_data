from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("EDA") \
    .getOrCreate()

folder = "/home/luan/project/Big_data/ecommerce_clickstream_spark/data/raw/kaggle_original"

files = [
    "customers.csv",
    "events.csv",
    "order_items.csv",
    "products.csv",
    "reviews.csv",
    "sessions.csv",
    "orders.csv"
]

for file in files:
    print("\n" + "="*50)
    print(f"FILE: {file}")
    print("="*50)

    df = spark.read.csv(
        os.path.join(folder, file),
        header=True,
        inferSchema=True
    )

    df.printSchema()
print("eda cơ bản cho mỗi file")
print("===========================================================================")
for file in files:

    df = spark.read.csv(
        os.path.join(folder, file),
        header=True,
        inferSchema=True
    )

    print(f"\n===== {file} =====")

    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")

    df.printSchema()

    df.show(5, truncate=False)
print("Xuất bảng Schema đẹp")
print("===========================================================================")
for file in files:

    df = spark.read.csv(
        os.path.join(folder, file),
        header=True,
        inferSchema=True
    )

    print(f"\n===== {file} =====")

    schema_info = [
        (field.name, str(field.dataType))
        for field in df.schema.fields
    ]

    schema_df = spark.createDataFrame(
        schema_info,
        ["Column", "Data Type"]
    )

    schema_df.show(truncate=False)