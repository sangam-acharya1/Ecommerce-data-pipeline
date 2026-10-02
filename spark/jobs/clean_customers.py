```python id="z8v3ne"
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim

# =========================================================
# Spark session
# =========================================================

spark = SparkSession.builder \
    .appName("clean_customers") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# =========================================================
# Read raw customer data
# =========================================================

df = spark.read.csv(
    "/opt/airflow/data/raw/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)


# =========================================================
# Basic cleaning
# =========================================================

cleaned = df \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("customer_unique_id").isNotNull()) \
    .withColumn(
        "customer_id",
        trim(col("customer_id"))
    ) \
    .withColumn(
        "customer_unique_id",
        trim(col("customer_unique_id"))
    ) \
    .withColumn(
        "customer_state",
        upper(trim(col("customer_state")))
    )


# =========================================================
# Remove exact duplicate records
# =========================================================

cleaned = cleaned.dropDuplicates()


# =========================================================
# Validate customer_id uniqueness
#
# Each customer_id should represent one customer record
# in this source table.
# =========================================================

duplicate_customer_ids = cleaned \
    .groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1)


duplicate_count = duplicate_customer_ids.count()

if duplicate_count > 0:
    raise ValueError(
        f"❌ Data quality check failed: "
        f"{duplicate_count} customer_id values have multiple records."
    )


# =========================================================
# Write Silver table
# =========================================================

cleaned.write \
    .mode("overwrite") \
    .parquet(
        "/opt/airflow/data/silver/customers/"
    )


# =========================================================
# Logging
# =========================================================

row_count = cleaned.count()

print(
    f"✅ clean_customers done — "
    f"{row_count} rows"
)


# =========================================================
# Stop Spark
# =========================================================

spark.stop()
```
