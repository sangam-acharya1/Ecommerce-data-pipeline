from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("clean_order_items") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/opt/airflow/data/raw/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

cleaned = df \
    .dropDuplicates(["order_id", "order_item_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("freight_value", col("freight_value").cast("double")) \
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))

cleaned.write.mode("overwrite").parquet("/opt/airflow/data/silver/order_items/")
print(f"✅ clean_order_items done — {cleaned.count()} rows")

spark.stop()