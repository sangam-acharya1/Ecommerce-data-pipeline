from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("clean_reviews") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/opt/airflow/data/raw/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True
)

cleaned = df \
    .dropDuplicates(["review_id"]) \
    .filter(col("order_id").isNotNull()) \
    .withColumn("review_score", col("review_score").cast("integer")) \
    .withColumn("review_creation_date", to_timestamp("review_creation_date")) \
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))

cleaned.write.mode("overwrite").parquet("/opt/airflow/data/silver/reviews/")
print(f"✅ clean_reviews done — {cleaned.count()} rows")

spark.stop()