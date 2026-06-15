from pyspark.sql import functions as F

from common.config import PROCESSED_DIR
from common.spark_utils import create_spark_session


EVENTS_PROCESSED_PATH = PROCESSED_DIR / "events_cleaned_parquet"

spark = create_spark_session("ValidateTask1Output")

# Doc output events da clean de kiem tra nhanh schema, row count va phan bo event_type.
events = spark.read.parquet(str(EVENTS_PROCESSED_PATH))

print("=" * 100)
print("VALIDATE TASK 1 OUTPUT")
print("Path:", EVENTS_PROCESSED_PATH)
print("Rows:", events.count())
print("Columns:", events.columns)

print("=" * 100)
print("Schema:")
events.printSchema()

print("=" * 100)
print("Event type counts:")
# Event type counts giup xac nhan cac loai event chinh van du sau khi clean.
events.groupBy("event_type").count().orderBy(F.desc("count")).show(100, truncate=False)

print("=" * 100)
print("Sample:")
events.show(20, truncate=False)

spark.stop()
