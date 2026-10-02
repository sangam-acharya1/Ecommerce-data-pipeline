```python
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    datediff,
    when,
    sum as spark_sum,
    count as spark_count,
    countDistinct,
    row_number,
    coalesce,
    lit
)

# =========================================================
# Spark session
# =========================================================

spark = SparkSession.builder \
    .appName("enrich_orders") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# =========================================================
# Read silver tables
# =========================================================

orders = spark.read.parquet(
    "/opt/airflow/data/silver/orders/"
)

items = spark.read.parquet(
    "/opt/airflow/data/silver/order_items/"
)

products = spark.read.parquet(
    "/opt/airflow/data/silver/products/"
)

customers = spark.read.parquet(
    "/opt/airflow/data/silver/customers/"
)

sellers = spark.read.parquet(
    "/opt/airflow/data/silver/sellers/"
)

payments = spark.read.parquet(
    "/opt/airflow/data/silver/payments/"
)


# =========================================================
# Customer lookup
#
# customer_id -> customer_unique_id + customer_state
# =========================================================

customer_lookup = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_state"
)


# =========================================================
# 1. ITEM-LEVEL GOLD TABLE
#
# Grain:
# one row per order item
# identified by (order_id, order_item_id)
# =========================================================

order_items_enriched = items \
    .join(
        products.select(
            "product_id",
            "product_category_english",
            "product_weight_g"
        ),
        on="product_id",
        how="left"
    ) \
    .join(
        sellers.select(
            "seller_id",
            "seller_state"
        ),
        on="seller_id",
        how="left"
    ) \
    .join(
        orders.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ),
        on="order_id",
        how="left"
    ) \
    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    ) \
    .withColumn(
        "delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull()
            & col("order_purchase_timestamp").isNotNull(),
            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp")
            )
        ).otherwise(None)
    ) \
    .withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date").isNull()
            | col("order_estimated_delivery_date").isNull(),
            None
        ).otherwise(
            col("order_delivered_customer_date")
            > col("order_estimated_delivery_date")
        )
    ) \
    .withColumn(
        "item_total_value",
        col("price") + col("freight_value")
    )


order_items_enriched.write \
    .mode("overwrite") \
    .parquet(
        "/opt/airflow/data/gold/order_items_enriched/"
    )

print(
    f"✅ order_items_enriched done — "
    f"{order_items_enriched.count()} rows"
)


# =========================================================
# 2. PAYMENT AGGREGATION
#
# payment_value:
# total payment value recorded for the order
#
# primary payment:
# payment record with the highest payment value
#
# ties:
# lowest payment_sequential wins
# =========================================================

payment_totals = payments \
    .groupBy("order_id") \
    .agg(
        spark_sum("payment_value").alias("payment_value"),
        spark_count("*").alias("payment_installment_records")
    )


primary_payment_window = Window \
    .partitionBy("order_id") \
    .orderBy(
        col("payment_value").desc(),
        col("payment_sequential").asc()
    )


primary_payment = payments \
    .withColumn(
        "rn",
        row_number().over(primary_payment_window)
    ) \
    .filter(
        col("rn") == 1
    ) \
    .select(
        "order_id",
        col("payment_type").alias(
            "primary_payment_type"
        ),
        col("payment_installments").alias(
            "primary_payment_installments"
        )
    )


payments_agg = payment_totals.join(
    primary_payment,
    on="order_id",
    how="left"
)


# =========================================================
# 3. ORDER-LEVEL AGGREGATION
#
# Grain:
# one row per order
# =========================================================

order_agg = items \
    .groupBy("order_id") \
    .agg(
        spark_sum("price").alias("total_price"),

        spark_sum(
            "freight_value"
        ).alias("total_freight_value"),

        spark_count("*").alias(
            "item_count"
        ),

        countDistinct(
            "product_id"
        ).alias(
            "distinct_product_count"
        ),

        countDistinct(
            "seller_id"
        ).alias(
            "distinct_seller_count"
        )
    )


# =========================================================
# 4. ORDER-LEVEL GOLD TABLE
#
# Grain:
# one row per order
# =========================================================

orders_enriched = orders \
    .join(
        order_agg,
        on="order_id",
        how="left"
    ) \
    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    ) \
    .join(
        payments_agg,
        on="order_id",
        how="left"
    ) \
    .withColumn(
        "total_order_value",
        coalesce(
            col("total_price"),
            lit(0)
        )
        +
        coalesce(
            col("total_freight_value"),
            lit(0)
        )
    ) \
    .withColumn(
        "delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull()
            & col("order_purchase_timestamp").isNotNull(),
            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp")
            )
        ).otherwise(None)
    ) \
    .withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date").isNull()
            | col("order_estimated_delivery_date").isNull(),
            None
        ).otherwise(
            col("order_delivered_customer_date")
            > col("order_estimated_delivery_date")
        )
    ) \
    .withColumn(
        "is_multi_seller_order",
        when(
            col("distinct_seller_count") > 1,
            True
        ).otherwise(False)
    )


# =========================================================
# 5. WRITE ORDER-LEVEL GOLD TABLE
# =========================================================

orders_enriched.write \
    .mode("overwrite") \
    .parquet(
        "/opt/airflow/data/gold/orders_enriched/"
    )

print(
    f"✅ orders_enriched done — "
    f"{orders_enriched.count()} rows"
)


# =========================================================
# Stop Spark
# =========================================================

spark.stop()
```
