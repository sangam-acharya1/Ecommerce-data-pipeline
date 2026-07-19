# E-commerce Batch Data Pipeline

An end-to-end batch data pipeline that transforms raw e-commerce order data into analytics-ready fact and dimension tables — built with PySpark, dbt, DuckDB, and Airflow, fully containerized with Docker.

Built on the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset (9 raw source tables, ~100K orders).

---

## What it does

- Cleans and validates 7 raw source tables (orders, order items, products, customers, sellers, payments, reviews) with PySpark
- Enriches and joins the cleaned data into order-level, customer-level, and seller-level datasets
- Loads the enriched data into DuckDB and models it with dbt across staging → intermediate → mart layers
- Runs 35+ automated data quality tests to catch issues before they reach reporting
- Orchestrates the entire pipeline — cleaning, enrichment, loading, transformation, and testing — as a single Airflow DAG
- Outputs analytics-ready tables covering **99,000+ orders**, **96,000+ customers**, and **3,000+ sellers**, representing **$15.8M** in transaction value

---

## Architecture

```
Raw CSVs
   │
   ▼
Spark (clean)      → type casting, null handling, deduplication
   │
   ▼
Spark (enrich)      → joins, aggregations, derived business columns
   │
   ▼
DuckDB              → warehouse layer
   │
   ▼
dbt
 ├── staging          (1:1 with source tables)
 ├── intermediate      (business logic, segmentation)
 └── marts             (fact/dim tables for analytics)
   │
   ▼
dbt tests            → 35+ automated data quality checks

Entire flow orchestrated by Airflow, running in Docker
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Processing | Python, PySpark, Spark SQL |
| Transformation & Modeling | dbt (staging / intermediate / marts) |
| Warehouse | DuckDB |
| Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| Version Control | Git |

---

## Data Model

**Staging** — 1:1 cleaned versions of source tables (`stg_order`, `stg_customer`, `stg_seller`)

**Intermediate** — business logic layered on top of staging:
- `int_orders_with_status` — delivery status classification, revenue bands, first-purchase flags
- `int_customer_segmented` — customer segment (new/repeat/loyal), spending tier, recency, and value tier
- `int_seller_ranked` — seller revenue/review ranking, performance tier, delivery reliability

**Marts** — final analytics-ready tables:
- `fct_orders` — one row per order, the single source of truth for order-level analysis
- `fct_daily_revenue` / `fct_monthly_revenue` — revenue rollups with month-over-month growth
- `dim_customers` — customer profile with segmentation
- `dim_seller` — seller profile with performance scoring
- `dim_products` — product category-level metrics

---

## Key Results

- Unified **9 raw source tables** into clean, tested, analytics-ready datasets
- Processed **99,441 orders**, **96,096 customers**, and **3,095 sellers**
- Surfaced **$15.8M** in total transaction value for revenue analysis
- Identified that **10.9%** of orders were delivered late, enabling delivery-performance monitoring
- Built RFM-style customer segmentation and seller performance scoring to flag at-risk customers and underperforming sellers
- Enforced data trustworthiness with **35+ automated dbt tests** across every model layer

---

## Project Structure

```
ecommerce-de-project/
├── data/
│   ├── raw/                 # source CSVs
│   ├── silver/               # Spark clean output
│   └── gold/                 # Spark enrich output
├── spark/
│   └── jobs/                 # all Spark clean + enrich scripts
├── scripts/
│   └── setup_duckdb.py       # registers Spark output as DuckDB tables
├── dbt/
│   └── ecommerce_data/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       └── ecommerce_data.duckdb
├── dags/
│   └── ecommerce_batch_dag.py
├── Dockerfile
├── docker-compose.yaml
└── README.md
```

---

## Running the Pipeline

### Prerequisites
- Docker & Docker Compose
- Java 11+ (bundled in the custom image)

### Setup

```bash
git clone <your-repo-url>
cd ecommerce-de-project

# download the Olist dataset from Kaggle and place CSVs in data/raw/

docker-compose build
docker-compose up -d
```

Airflow UI available at `localhost:8080` (default credentials: `airflow` / `airflow`).

Toggle the `ecommerce_batch_pipeline` DAG on and trigger a run. The pipeline executes:

```
7 parallel Spark clean jobs
        ↓
3 Spark enrich jobs
        ↓
DuckDB registration
        ↓
dbt run
        ↓
dbt test
```

### Running locally without Docker

Each layer can also be run and tested independently:

```bash
python -m venv venv && source venv/bin/activate
pip install pyspark dbt-duckdb duckdb

# run any Spark job directly
python spark/jobs/clean_orders.py

# run dbt
cd dbt/ecommerce_data
dbt build --target dev
```

---

## Data Quality

Every model layer is covered by dbt tests, including:
- `unique` and `not_null` on all primary keys
- `accepted_values` on categorical fields (order status, delivery status, customer segment)
- `relationships` tests validating foreign key integrity between fact and dimension tables

---

## Future Improvements

- Migrate the warehouse layer from DuckDB to a cloud data warehouse for production-scale queries
- Move Spark execution to a managed cluster for larger data volumes
- Add CI/CD to automatically run `dbt test` on every pull request
- Add monitoring/alerting on pipeline failures
- Extend to incremental/streaming ingestion instead of full batch reprocessing

---

## License

This project uses the publicly available [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) for educational and portfolio purposes.
