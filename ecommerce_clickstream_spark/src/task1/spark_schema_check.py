from common.config import RAW_DIR, RAW_TABLE_NAMES
from common.spark_utils import create_spark_session

# Script nay dung de kiem tra schema Spark doc duoc tu raw CSV.
# Khac voi schemas.py, o day inferSchema giup doi chieu nhanh voi du lieu goc.
spark = create_spark_session("EcommerceClickstreamSchemaCheck")

for table_name in RAW_TABLE_NAMES:
    file_name = f"{table_name}.csv"
    path = str(RAW_DIR / file_name)

    print("=" * 100)
    print(f"FILE: {file_name}")
    print(f"PATH: {path}")

    df = (
        spark.read
        .option("header", True)
        # inferSchema chi dung cho buoc kiem tra, khong dung cho cleaning chinh.
        .option("inferSchema", True)
        .csv(path)
    )

    print("ROW COUNT:", df.count())
    print("COLUMNS:")
    print(df.columns)

    print("SCHEMA:")
    df.printSchema()

    print("SAMPLE:")
    df.show(5, truncate=False)

spark.stop()
