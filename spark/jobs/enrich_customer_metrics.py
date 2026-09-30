from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, \
    avg, min as spark_min, max as spark_max, datediff, round as spark_round

spark = SparkSession.builder \
    .appName("enrich_customer_metrics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

orders    = spark.read.parquet("/opt/airflow/data/gold/orders_enriched/")
customers = spark.read.parquet("/opt/airflow/data/silver/customers/")

# bring in customer_unique_id — the stable identity across orders
orders_with_identity = orders.join(
    customers.select("customer_id", "customer_unique_id"),
    on="customer_id",
    how="left"
)

customer_metrics = orders_with_identity \
    .filter(col("order_status") == "delivered") \
    .groupBy("customer_unique_id") \
    .agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("total_order_value"), 2).alias("total_spent"),
        spark_round(avg("total_order_value"), 2).alias("avg_order_value"),
        spark_min("order_purchase_timestamp").alias("first_order_date"),
        spark_max("order_purchase_timestamp").alias("last_order_date")
    ) \
    .withColumn(
        "customer_age_days",
        datediff(col("last_order_date"), col("first_order_date"))
    )

customer_metrics.write.mode("overwrite").parquet("/opt/airflow/data/gold/customer_metrics/")
print(f"✅ enrich_customer_metrics done — {customer_metrics.count()} rows")

spark.stop()