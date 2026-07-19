from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "de-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="End to end e-commerce batch pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "batch", "spark", "dbt"],
    max_active_tasks=2,
) as dag:

    # -----------------------------
    # Spark Cleaning Tasks
    # -----------------------------
    spark_clean_orders = BashOperator(
        task_id="spark_clean_orders",
        bash_command="python /opt/airflow/spark/jobs/clean_orders.py",
    )

    spark_clean_order_items = BashOperator(
        task_id="spark_clean_order_items",
        bash_command="python /opt/airflow/spark/jobs/clean_order_items.py",
    )

    spark_clean_products = BashOperator(
        task_id="spark_clean_products",
        bash_command="python /opt/airflow/spark/jobs/clean_products.py",
    )

    spark_clean_customers = BashOperator(
        task_id="spark_clean_customers",
        bash_command="python /opt/airflow/spark/jobs/clean_customers.py",
    )

    spark_clean_sellers = BashOperator(
        task_id="spark_clean_sellers",
        bash_command="python /opt/airflow/spark/jobs/clean_sellers.py",
    )

    spark_clean_payments = BashOperator(
        task_id="spark_clean_payments",
        bash_command="python /opt/airflow/spark/jobs/clean_payments.py",
    )

    spark_clean_reviews = BashOperator(
        task_id="spark_clean_reviews",
        bash_command="python /opt/airflow/spark/jobs/clean_reviews.py",
    )

    # -----------------------------
    # Spark Enrichment Tasks
    # -----------------------------
    spark_enrich_orders = BashOperator(
        task_id="spark_enrich_orders",
        bash_command="python /opt/airflow/spark/jobs/enrich_orders.py",
    )

    spark_enrich_customer_metrics = BashOperator(
        task_id="spark_enrich_customer_metrics",
        bash_command="python /opt/airflow/spark/jobs/enrich_customer_metrics.py",
    )

    spark_enrich_seller_metrics = BashOperator(
        task_id="spark_enrich_seller_metrics",
        bash_command="python /opt/airflow/spark/jobs/enrich_seller_metrics.py",
    )

    # -----------------------------
    # Register Parquet into DuckDB
    # -----------------------------
    setup_duckdb = BashOperator(
        task_id="setup_duckdb",
        bash_command="python /opt/airflow/scripts/setup_duckdb.py",
    )

    # -----------------------------
    # dbt Run
    # -----------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt/ecommerce_data &&
        dbt run --profiles-dir /opt/airflow/dbt --target dev
        """,
    )

    # -----------------------------
    # dbt Test
    # -----------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt/ecommerce_data &&
        dbt test --profiles-dir /opt/airflow/dbt --target dev
        """,
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    clean_jobs = [
        spark_clean_orders,
        spark_clean_order_items,
        spark_clean_products,
        spark_clean_customers,
        spark_clean_sellers,
        spark_clean_payments,
        spark_clean_reviews,
    ]

    enrich_jobs = [
        spark_enrich_orders,
        spark_enrich_customer_metrics,
        spark_enrich_seller_metrics,
    ]

    # all clean jobs must finish before each enrich job starts
    for enrich_task in enrich_jobs:
        clean_jobs >> enrich_task

    # all enrich jobs must finish before duckdb setup
    for enrich_task in enrich_jobs:
        enrich_task >> setup_duckdb

    setup_duckdb >> dbt_run >> dbt_test