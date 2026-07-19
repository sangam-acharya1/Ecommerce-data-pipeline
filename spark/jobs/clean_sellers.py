from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder \
    .appName("clean_sellers") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/opt/airflow/data/raw/olist_sellers_dataset.csv",
    header=True,
    inferSchema=True
)

cleaned = df \
    .dropDuplicates(["seller_id"]) \
    .filter(col("seller_id").isNotNull()) \
    .withColumn("seller_state", upper(col("seller_state")))

cleaned.write.mode("overwrite").parquet("/opt/airflow/data/silver/sellers/")
print(f"✅ clean_sellers done — {cleaned.count()} rows")

spark.stop()