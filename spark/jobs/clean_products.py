from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("clean_products") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

products = spark.read.csv(
    "/opt/airflow/data/raw/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

translation = spark.read.csv(
    "/opt/airflow/data/raw/product_category_name_translation.csv",
    header=True,
    inferSchema=True
)

cleaned = products \
    .dropDuplicates(["product_id"]) \
    .filter(col("product_id").isNotNull()) \
    .withColumn("product_category_name",
        when(col("product_category_name").isNull(), "unknown")
        .otherwise(col("product_category_name"))
    ) \
    .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
    .withColumn("product_length_cm", col("product_length_cm").cast("double")) \
    .withColumn("product_height_cm", col("product_height_cm").cast("double")) \
    .withColumn("product_width_cm", col("product_width_cm").cast("double"))

enriched = cleaned.join(translation, on="product_category_name", how="left") \
    .withColumnRenamed("product_category_name_english", "product_category_english")

enriched.write.mode("overwrite").parquet("/opt/airflow/data/silver/products/")
print(f"✅ clean_products done — {enriched.count()} rows")

spark.stop()