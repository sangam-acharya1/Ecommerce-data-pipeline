from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("clean_orders") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "/opt/airflow/data/raw/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

cleaned = df \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
    .withColumn("order_approved_at", to_timestamp("order_approved_at")) \
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date")) \
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))

cleaned.write.mode("overwrite").parquet("/opt/airflow/data/silver/orders/")
print(f"✅ clean_orders done — {cleaned.count()} rows")

spark.stop()