# E-commerce Batch Data Pipeline

An end-to-end batch data pipeline that transforms raw e-commerce order data into analytics-ready fact and dimension tables — built with PySpark, dbt, DuckDB, and Airflow, fully containerized with Docker.

Built on the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset (9 raw source tables, ~100K orders).

---

## What it does

- Cleans and validates 7 raw source tables (orders, order items, products, customers, sellers, payments, reviews) with PySpark
- Enriches the cleaned data at **two grains**: order-level (`orders_enriched`) and order-item-level (`order_items_enriched`) — so seller and product metrics are attributed to the correct party even when an order contains items from multiple sellers
- Loads the enriched data into DuckDB and models it with dbt across staging → intermediate → mart layers
- Runs 35+ automated dbt tests to catch issues before they reach reporting
- Orchestrates the entire pipeline — cleaning, enrichment, loading, transformation, and testing — as a single Airflow DAG
- Outputs analytics-ready tables covering **99,000+ orders**, **96,000+ unique customers**, and **3,000+ sellers**

---

## Data Notes (read this before trusting a number from this repo)

This dataset and pipeline have some characteristics that matter if you're going to report on the output — documented here rather than left implicit:

- **Currency:** all monetary figures (`total_order_value`, `total_revenue`, `total_spent`, etc.) are in **Brazilian Reais (BRL)**, the native currency of the source Olist dataset. No currency conversion has been applied anywhere in this pipeline.
- **Timestamps:** `order_purchase_timestamp` and derived date fields (`order_date`, `order_year`, `order_month`) are used as-is from the source data, with no timezone conversion applied. The source dataset does not publish an explicit timezone for these values.
- **Revenue definition:** `fct_daily_revenue` and `fct_monthly_revenue` define revenue as `total_order_value` (item prices + freight), **excluding canceled orders**. This is a deliberate choice, not the only valid one — see each model's description in `schema.yml` for the exact logic.
- **Order grain:** an order can contain items from more than one seller. `fct_orders` is order-grain (one row per `order_id`, enforced with a `unique` test) and does **not** carry a single `seller_id` or `product_id` — it exposes `distinct_seller_count`, `distinct_product_count`, and `is_multi_seller_order` instead. Seller- and product-level analysis is done from `fct_order_items`, which is grain-correct (one row per order/product/seller line item).
- **Payment attribution:** an order can have multiple payment records (e.g. split payments). `fct_orders.payment_type` reflects the single highest-value payment record for that order (ties broken by earliest `payment_sequential`) — it is a representative value, not a complete picture of split payments.
- **Seller review attribution:** Olist records one review per order, not per seller. For multi-seller orders, that review score is currently attributed to all sellers on the order — this is a real limitation of the source data, not something this pipeline can fully resolve.

---

## Architecture

```
Raw CSVs
   │
   ▼
Spark (clean)      → type casting, null handling, deduplication
   │
   ▼
Spark (enrich)      → order-level AND order-item-level joins, aggregations, derived columns
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

**Staging** — 1:1 cleaned versions of source tables:
- `stg_order`, `stg_order_items`, `stg_customer`, `stg_seller`

**Intermediate** — business logic layered on top of staging:
- `int_orders_with_status` — delivery status classification, revenue bands, first-purchase flags
- `int_customer_segmented` — customer segment (new/repeat/loyal), spending tier, recency, and value tier
- `int_seller` — seller revenue/review ranking, delivery reliability, computed from order-item grain

**Marts** — final analytics-ready tables:
- `fct_orders` — one row per order; order-level financials, delivery, and customer classification. Does not carry seller/product columns — see Data Notes above.
- `fct_order_items` — one row per order-item; the correct grain for any seller- or product-level analysis
- `fct_daily_revenue` / `fct_monthly_revenue` — revenue rollups, excluding canceled orders, with month-over-month growth
- `dim_customers` — customer profile with segmentation, keyed on `customer_unique_id` (the stable customer identity, not the per-order `customer_id`)
- `dim_seller` — seller profile with performance scoring, built from `fct_order_items`
- `dim_product` — product category-level metrics, built from `fct_order_items`

---

## Key Results

- Unified **9 raw source tables** into clean, tested, analytics-ready datasets at two grains (order and order-item)
- Processed **99,441 orders**, **96,096 unique customers**, and **3,095 sellers**
- Identified that **10.9%** of orders were delivered late, enabling delivery-performance monitoring
- Built RFM-style customer segmentation and seller performance scoring to flag at-risk customers and underperforming sellers
- Enforced data trustworthiness with **35+ automated dbt tests**, including uniqueness tests on the primary key of every fact and dimension table

---

## Project Structure

```
ecommerce-de-project/
├── data/
│   ├── raw/                 # source CSVs
│   ├── silver/               # Spark clean output
│   └── gold/                 # Spark enrich output (order-level + order-item-level)
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
│       └── ecommerce_data.duckdb   # not tracked in git — generated by the pipeline
├── dags/
│   └── ecommerce_batch_dag.py
├── Dockerfile
├── docker-compose.yaml
└── README.md
```

`.duckdb` files, `logs/`, and `.env` are excluded via `.gitignore` and regenerated by running the pipeline — they are not committed to this repository.

---

## Running the Pipeline

### Prerequisites
- Docker & Docker Compose
- Java 11+ (bundled in the custom image)

### Setup

```bash
git clone https://github.com/<your-username>/ecommerce-de-project.git
cd ecommerce-de-project

# download the Olist dataset from Kaggle and place the CSVs in data/raw/

docker-compose build
docker-compose up -d
```

Airflow UI available at `localhost:8080` (default credentials: `airflow` / `airflow`).

Toggle the `ecommerce_batch_pipeline` DAG on and trigger a run. The pipeline executes:

```
7 parallel Spark clean jobs
        ↓
Spark enrich jobs (order-level + order-item-level)
        ↓
DuckDB registration
        ↓
dbt run
        ↓
dbt test
```

### Running locally without Docker

Each dbt layer can be run and tested independently once the DuckDB file exists:

```bash
python -m venv venv && source venv/bin/activate
pip install "duckdb==0.9.2" "dbt-duckdb==1.7.2"

cd dbt/ecommerce_data
dbt build --target dev
```

Spark job scripts use container-style `/opt/airflow/...` paths, so running them outside Docker requires either adjusting those paths for your local filesystem or running them via `docker exec` against the running Airflow worker container, e.g.:

```bash
docker exec -it <airflow-worker-container> python /opt/airflow/spark/jobs/clean_orders.py
```

### Verified run

The pipeline was last run end-to-end on 2026-07-14, with all 13 Airflow tasks and all 49 dbt nodes (14 models, 35 tests) completing successfully:

```
Finished running 7 view models, 35 tests, 7 table models in 2.06 seconds.
Done. PASS=37 WARN=0 ERROR=0 SKIP=0 TOTAL=49
```

---

## Data Quality

Every model layer is covered by dbt tests, including:
- `unique` and `not_null` on the primary key of every fact and dimension table (`fct_orders.order_id`, `dim_customers.customer_unique_id`, `dim_seller.seller_id`, `dim_product.product_category`)
- `accepted_values` on categorical fields (order status, delivery status, customer segment, seller delivery reliability)
- Explicit documentation (not just tests) of definitional choices — revenue scope, currency, and grain — directly in each model's `schema.yml`, since some of these can't be enforced as a test but still need to be stated

---

## Known Limitations

- **DuckDB, not a cloud warehouse** — this runs entirely locally/in Docker. See "Future Improvements."
- **Seller-level review scores are approximate** for multi-seller orders, since Olist reviews are recorded per order, not per seller (see Data Notes).
- **Payment type is a representative value**, not a full breakdown, for orders with split payments.
- **No incremental processing** — every run fully reprocesses the dataset from raw CSVs.

## Future Improvements

- Migrate the warehouse layer from DuckDB to a cloud data warehouse for production-scale queries
- Move Spark execution to a managed cluster (Dataproc) for larger data volumes
- Add CI/CD to automatically run `dbt test` on every pull request
- Add monitoring/alerting on pipeline failures
- Extend to incremental/streaming ingestion instead of full batch reprocessing

---

## License

This project uses the publicly available [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) for educational and portfolio purposes.
