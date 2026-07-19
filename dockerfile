FROM apache/airflow:2.8.1

USER root

# install Java (required for pyspark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean

USER airflow

# install pyspark, pinned duckdb (prebuilt wheel for py3.8), and matching dbt-duckdb
RUN pip install --no-cache-dir \
    pyspark \
    "duckdb==0.9.2" \
    "dbt-duckdb==1.7.2"