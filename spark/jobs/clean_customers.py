from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder \
    .appName("clean_customers") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/opt/airflow/data/raw/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

cleaned = df \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("customer_state", upper(col("customer_state")))

cleaned.write.mode("overwrite").parquet("/opt/airflow/data/silver/customers/")
print(f"✅ clean_customers done — {cleaned.count()} rows")

spark.stop()