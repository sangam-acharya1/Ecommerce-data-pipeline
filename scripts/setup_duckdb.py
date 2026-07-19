import duckdb

con = duckdb.connect("/opt/airflow/dbt/ecommerce_data/ecommerce_data.duckdb")

con.execute("""
    CREATE OR REPLACE TABLE orders_enriched AS
    SELECT *
    FROM read_parquet('/opt/airflow/data/gold/orders_enriched/**/*.parquet')
""")

con.execute("""
    CREATE OR REPLACE TABLE customer_metrics AS
    SELECT *
    FROM read_parquet('/opt/airflow/data/gold/enrich_customer_metrices/*.parquet')
""")

con.execute("""
    CREATE OR REPLACE TABLE seller_metrics AS
    SELECT *
    FROM read_parquet('/opt/airflow/data/gold/enrich_seller_metrices/*.parquet')
""")

print("✅ Tables registered in DuckDB:")
for row in con.execute("SHOW TABLES").fetchall():
    print("-", row[0])

con.close()