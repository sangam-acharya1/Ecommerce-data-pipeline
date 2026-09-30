from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, datediff, when, sum as spark_sum, count as spark_count,
    countDistinct, row_number
)

spark = SparkSession.builder \
    .appName("enrich_orders") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------- read silver tables ----------
orders    = spark.read.parquet("/opt/airflow/data/silver/orders/")
items     = spark.read.parquet("/opt/airflow/data/silver/order_items/")
products  = spark.read.parquet("/opt/airflow/data/silver/products/")
customers = spark.read.parquet("/opt/airflow/data/silver/customers/")
sellers   = spark.read.parquet("/opt/airflow/data/silver/sellers/")
payments  = spark.read.parquet("/opt/airflow/data/silver/payments/")


# =========================================================
# 1. ITEM-LEVEL GOLD TABLE — the real grain
#    one row per (order_id, product_id, seller_id) line item
# =========================================================

order_items_enriched = items \
    .join(
        products.select("product_id", "product_category_english", "product_weight_g"),
        on="product_id", how="left"
    ) \
    .join(
        sellers.select("seller_id", "seller_state"),
        on="seller_id", how="left"
    ) \
    .join(
        orders.select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ),
        on="order_id", how="left"
    ) \
    .join(
        customers.select("customer_id", "customer_state"),
        on="customer_id", how="left"
    ) \
    .withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    ) \
    .withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"), True
        ).otherwise(False)
    ) \
    .withColumn("item_total_value", col("price") + col("freight_value"))

order_items_enriched.write.mode("overwrite").parquet(
    "/opt/airflow/data/gold/order_items_enriched/"
)
print(f"✅ order_items_enriched done — {order_items_enriched.count()} rows")


# =========================================================
# 2. PAYMENTS — deterministic "primary" payment per order
#    rule: the payment record with the HIGHEST payment_value
#    represents the order's dominant payment method.
#    Ties broken by lowest payment_sequential (first attempt).
# =========================================================

payment_totals = payments.groupBy("order_id").agg(
    spark_sum("payment_value").alias("payment_value"),
    spark_count("*").alias("payment_installment_records")
)

primary_payment_window = Window.partitionBy("order_id") \
    .orderBy(col("payment_value").desc(), col("payment_sequential").asc())

primary_payment = payments \
    .withColumn("rn", row_number().over(primary_payment_window)) \
    .filter(col("rn") == 1) \
    .select(
        "order_id",
        col("payment_type").alias("primary_payment_type"),
        col("payment_installments").alias("primary_payment_installments")
    )

payments_agg = payment_totals.join(primary_payment, on="order_id", how="left")


# =========================================================
# 3. ORDER-LEVEL GOLD TABLE — true aggregate, no fabricated
#    single product/seller. Multi-seller orders are flagged.
# =========================================================

order_agg = items.groupBy("order_id").agg(
    spark_sum("price").alias("total_price"),
    spark_sum("freight_value").alias("total_freight_value"),
    spark_count("*").alias("item_count"),
    countDistinct("product_id").alias("distinct_product_count"),
    countDistinct("seller_id").alias("distinct_seller_count")
)

orders_enriched = orders \
    .join(order_agg, on="order_id", how="left") \
    .join(
        customers.select("customer_id", "customer_state"),
        on="customer_id", how="left"
    ) \
    .join(payments_agg, on="order_id", how="left") \
    .withColumn(
        "total_order_value",
        col("total_price") + col("total_freight_value")
    ) \
    .withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    ) \
    .withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"), True
        ).otherwise(False)
    ) \
    .withColumn(
        "is_multi_seller_order",
        when(col("distinct_seller_count") > 1, True).otherwise(False)
    )

orders_enriched.write.mode("overwrite").parquet(
    "/opt/airflow/data/gold/orders_enriched/"
)
print(f"✅ orders_enriched done — {orders_enriched.count()} rows")

spark.stop()