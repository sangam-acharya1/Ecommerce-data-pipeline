from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, \
    avg, round as spark_round

spark = SparkSession.builder \
    .appName("enrich_seller_metrics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

order_items = spark.read.parquet("/opt/airflow/data/gold/order_items_enriched/")
reviews     = spark.read.parquet("/opt/airflow/data/silver/reviews/")

# join item-level data with reviews (review is per order, applied to each item row)
items_reviews = order_items.join(
    reviews.select("order_id", "review_score"),
    on="order_id",
    how="left"
)

seller_metrics = items_reviews \
    .groupBy("seller_id", "seller_state") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("*").alias("total_items_sold"),
        spark_round(spark_sum("item_total_value"), 2).alias("total_revenue"),
        spark_round(avg("review_score"), 2).alias("avg_review_score"),
        spark_round(avg("delivery_days"), 1).alias("avg_delivery_days"),
        spark_round(
            spark_sum(col("is_late").cast("integer")) * 100.0 / count("*"),
            2
        ).alias("late_delivery_rate")
    )

seller_metrics.write.mode("overwrite").parquet("/opt/airflow/data/gold/seller_metrics/")
print(f"✅ enrich_seller_metrics done — {seller_metrics.count()} rows")

spark.stop()