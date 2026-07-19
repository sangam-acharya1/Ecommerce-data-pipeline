from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when, sum as spark_sum, first

spark = SparkSession.builder \
    .appName("enrich_orders") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# read silver tables
orders   = spark.read.parquet("/opt/airflow/data/silver/orders/")
items    = spark.read.parquet("/opt/airflow/data/silver/order_items/")
products = spark.read.parquet("/opt/airflow/data/silver/products/")
customers = spark.read.parquet("/opt/airflow/data/silver/customers/")
sellers  = spark.read.parquet("/opt/airflow/data/silver/sellers/")
payments = spark.read.parquet("/opt/airflow/data/silver/payments/")

# aggregate items per order
items_agg = items.groupBy("order_id").agg(
    spark_sum("price").alias("total_price"),
    spark_sum("freight_value").alias("freight_value")
)

# get one product and seller per order
items_single = items.select(
    "order_id", "product_id", "seller_id"
).dropDuplicates(["order_id"])

# aggregate payments per order
payments_agg = payments.groupBy("order_id").agg(
    spark_sum("payment_value").alias("payment_value"),
    first("payment_type").alias("payment_type"),
    first("payment_installments").alias("payment_installments")
)

# join everything together
enriched = orders \
    .join(items_agg, on="order_id", how="left") \
    .join(items_single, on="order_id", how="left") \
    .join(products.select(
        "product_id",
        "product_category_english",
        "product_weight_g"
    ), on="product_id", how="left") \
    .join(customers.select(
        "customer_id",
        "customer_state"
    ), on="customer_id", how="left") \
    .join(sellers.select(
        "seller_id",
        "seller_state"
    ), on="seller_id", how="left") \
    .join(payments_agg.select(
        "order_id",
        "payment_value",
        "payment_type",
        "payment_installments"
    ), on="order_id", how="left")

# compute derived columns
enriched = enriched \
    .withColumn(
        "total_order_value",
        col("total_price") + col("freight_value")
    ) \
    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    ) \
    .withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date") >
            col("order_estimated_delivery_date"), True
        ).otherwise(False)
    ) \
    .withColumn(
        "price", col("total_price")
    )

enriched.write.mode("overwrite").parquet("/opt/airflow/data/gold/orders_enriched/")
print(f"✅ enrich_orders done — {enriched.count()} rows")

spark.stop()