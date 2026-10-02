```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    round as spark_round,
    max as spark_max
)

# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("enrich_seller_metrics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ---------------------------------------------------------
# Read source data
# ---------------------------------------------------------

order_items = spark.read.parquet(
    "/opt/airflow/data/gold/order_items_enriched/"
)

reviews = spark.read.parquet(
    "/opt/airflow/data/silver/reviews/"
)


# ---------------------------------------------------------
# 1. Create seller-item metrics
#
# Grain:
# one row per order item
# ---------------------------------------------------------

seller_item_metrics = order_items \
    .groupBy(
        "seller_id",
        "seller_state"
    ) \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("*").alias("total_items_sold"),

        spark_round(
            spark_sum("item_total_value"),
            2
        ).alias("total_revenue")
    )


# ---------------------------------------------------------
# 2. Create order-level seller metrics
#
# Important:
# A seller can have multiple items in the same order.
# We therefore deduplicate to one seller + order
# before calculating review and delivery metrics.
# ---------------------------------------------------------

seller_orders = order_items \
    .select(
        "order_id",
        "seller_id",
        "seller_state",
        "delivery_days",
        "is_late"
    ) \
    .dropDuplicates([
        "order_id",
        "seller_id"
    ])


# ---------------------------------------------------------
# 3. Attach order-level reviews
# ---------------------------------------------------------

seller_orders_reviews = seller_orders.join(
    reviews.select(
        "order_id",
        "review_score"
    ),
    on="order_id",
    how="left"
)


# ---------------------------------------------------------
# 4. Calculate order-level seller metrics
# ---------------------------------------------------------

seller_order_metrics = seller_orders_reviews \
    .groupBy(
        "seller_id",
        "seller_state"
    ) \
    .agg(

        spark_round(
            avg("review_score"),
            2
        ).alias("avg_review_score"),

        spark_round(
            avg("delivery_days"),
            1
        ).alias("avg_delivery_days"),

        spark_round(
            spark_sum(
                col("is_late").cast("integer")
            ) * 100.0 / count("*"),
            2
        ).alias("late_delivery_rate")
    )


# ---------------------------------------------------------
# 5. Combine item-level and order-level metrics
# ---------------------------------------------------------

seller_metrics = seller_item_metrics.join(
    seller_order_metrics,
    on=[
        "seller_id",
        "seller_state"
    ],
    how="left"
)


# ---------------------------------------------------------
# 6. Write final seller metrics
# ---------------------------------------------------------

seller_metrics.write \
    .mode("overwrite") \
    .parquet(
        "/opt/airflow/data/gold/seller_metrics/"
    )


# ---------------------------------------------------------
# 7. Logging
# ---------------------------------------------------------

row_count = seller_metrics.count()

print(
    f"✅ enrich_seller_metrics done — "
    f"{row_count} seller rows"
)


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()
```
