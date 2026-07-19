
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("clean_payments") \
    .master("local[*]") \
    .getOrCreate()

# ---------- read raw data ----------
df = spark.read.csv(
    "/opt/airflow/data/raw/olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)

print("Raw row count:", df.count())
df.printSchema()

# ---------- clean ----------
df_clean = (
    df
    # cast payment_value to double (in case inferSchema got it wrong)
    .withColumn("payment_value", col("payment_value").cast(DoubleType()))
    .withColumn("payment_installments", col("payment_installments").cast("int"))
    # drop rows with no order_id — can't join without it
    .filter(col("order_id").isNotNull())
    # drop exact duplicate rows
    .dropDuplicates()
)

print("Clean row count:", df_clean.count())

# ---------- write to silver ----------
df_clean.write.mode("overwrite").parquet("/opt/airflow/data/silver/payments/")

print("✅ clean_payments.py finished — written to data/silver/payments/")

spark.stop()