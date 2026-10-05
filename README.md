# Retail Sales Data Engineering Pipeline

Production-style data engineering and analytics portfolio project for a retail sales workflow using AWS S3, Python ETL, Snowflake, dbt, Airflow, and a Power BI-ready analytics layer.

## Business Problem

Retail teams need a repeatable way to collect order-level sales data, land it in cloud storage, load it into a warehouse, validate data quality, model clean analytics tables, and expose metrics for business dashboards.

This project demonstrates that flow end to end with synthetic retail sales data.

## Architecture

```text
                 RETAIL SALES DATA
                        |
                        v
                    AWS S3
                        |
                        v
                 Python / ETL
                        |
                        v
                Snowflake RAW
                        |
                        v
                       dbt
                        |
                        v
             Snowflake ANALYTICS
                        |
                        v
                   Power BI
```

Airflow orchestrates the pipeline:

```text
generate_data
      |
      v
upload_to_s3
      |
      v
load_snowflake_raw
      |
      v
dbt_run
      |
      v
dbt_test
```

The working ingestion path is `S3 -> Python/boto3 -> Snowflake RAW`. The project is not dependent on a Snowflake external stage. An earlier external-stage approach was intentionally avoided because of AWS AssumeRole/SAML/SCP constraints.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Synthetic data generation and ETL scripts |
| Pandas / NumPy | Retail dataset generation |
| AWS S3 | Raw CSV object storage |
| boto3 | S3 discovery/download/upload |
| Snowflake | Cloud data warehouse |
| dbt | SQL transformations, tests, and incremental modeling |
| Apache Airflow | Pipeline orchestration |
| Docker | Recommended Airflow runtime on Windows |
| Power BI | Dashboard layer over Snowflake ANALYTICS |

## Data Flow

1. `scripts/generate_retail_data.py` creates a timestamped CSV in `data/raw/`.
2. `scripts/upload_to_s3.py` uploads the newest CSV to `s3://soham-retail-sales-pipeline-2026/raw/`.
3. `scripts/load_snowflake.py` downloads the newest S3 CSV and merges rows into `RETAIL_SALES_DB.RAW.RETAIL_SALES`.
4. dbt builds staging, dimension, fact, and aggregate models in `RETAIL_SALES_DB.ANALYTICS`.
5. dbt tests validate required fields, uniqueness, and relationships.
6. Power BI connects to the Snowflake analytics tables.

## Warehouse Design

```text
RETAIL_SALES_DB
|
+-- RAW
|   +-- RETAIL_SALES
|
+-- ANALYTICS
    +-- STG_RETAIL_SALES
    +-- DIM_CUSTOMER
    +-- DIM_PRODUCT
    +-- FACT_SALES
    +-- SALES_DAILY
```

`RAW.RETAIL_SALES` stores landed order-level records with minimal transformation.

`ANALYTICS.STG_RETAIL_SALES` standardizes the raw table for downstream dbt models.

`ANALYTICS.DIM_CUSTOMER` contains one row per customer with customer city.

`ANALYTICS.DIM_PRODUCT` contains one row per product with category and unit price attributes.

`ANALYTICS.FACT_SALES` contains transaction-level sales facts and is built as an incremental dbt model keyed by `ORDER_ID`.

`ANALYTICS.SALES_DAILY` aggregates sales by date for dashboard KPIs.

## Incremental Processing

The Python RAW load uses a Snowflake `MERGE` on `ORDER_ID`. If the latest S3 file is processed again, existing orders are skipped instead of duplicated.

The dbt `fact_sales` model is incremental with `unique_key='ORDER_ID'`. On incremental runs it selects rows from staging where the order does not already exist in the target fact table. This avoids the common `ORDER_ID > max(ORDER_ID)` late-arriving-data issue.

## Data Quality

dbt tests cover:

- `ORDER_ID` not null and unique
- `CUSTOMER_ID` not null
- `PRODUCT` not null
- `TOTAL_AMOUNT` not null
- `SALES_DATE` not null and unique
- Fact-to-dimension relationships for customer and product

The generator intentionally leaves a small number of `PAYMENT_METHOD` values blank, so that column is not tested as required.

## Environment Variables

Copy `.env.example` to `.env` and fill in local values. Do not commit `.env`.

Required values:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE
SNOWFLAKE_PRIVATE_KEY_PATH
S3_BUCKET_NAME
S3_PREFIX
AWS_PROFILE
```

`scripts/load_snowflake.py` supports key-pair authentication through `SNOWFLAKE_PRIVATE_KEY_PATH`. It can still use `SNOWFLAKE_PASSWORD` if no private key path is set, but passwords should not be committed or placed in tracked files.

## Snowflake Key-Pair Auth

The private key should stay in `secrets/snowflake_key.p8` and is ignored by Git. Register only the public key with Snowflake:

```sql
ALTER USER SOHAM SET RSA_PUBLIC_KEY='<public key body without BEGIN/END lines>';
```

See `docs/snowflake_key_pair_auth.sql` for the prepared statement.

Update `~/.dbt/profiles.yml` so dbt uses `private_key_path` instead of `externalbrowser` or a password.

## Local Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Verify AWS CLI access:

```powershell
aws s3 ls s3://soham-retail-sales-pipeline-2026/raw/
```

Verify dbt profile configuration:

```powershell
dbt debug --project-dir retail_sales_dbt
```

## Run the Pipeline Locally

```powershell
python scripts/generate_retail_data.py
python scripts/upload_to_s3.py
python scripts/load_snowflake.py
dbt run --project-dir retail_sales_dbt
dbt test --project-dir retail_sales_dbt
```

## Run with Airflow

Airflow is best run through Docker/WSL on Windows.

```powershell
cd airflow
docker compose build
docker compose up -d
```

Open `http://localhost:8080` and trigger the `retail_sales_pipeline` DAG.

## Power BI Dashboard Layer

Connect Power BI to Snowflake and use:

- `ANALYTICS.SALES_DAILY`
- `ANALYTICS.FACT_SALES`
- `ANALYTICS.DIM_CUSTOMER`
- `ANALYTICS.DIM_PRODUCT`

Recommended KPIs:

- Total Revenue
- Total Orders
- Units Sold
- Average Order Value

Recommended visuals:

- Revenue by Date
- Revenue by Category
- Revenue by Product
- Revenue by City
- Payment Method Distribution
- Top Products
- Customer Analysis

See `docs/power_bi_setup.md` for setup notes.

## Project Structure

```text
retail-sales-data-engineering/
|-- airflow/
|   |-- dags/
|   |   `-- retail_sales_pipeline.py
|   |-- Dockerfile
|   `-- docker-compose.yaml
|-- data/
|   `-- raw/
|-- docs/
|   |-- architecture.md
|   |-- power_bi_setup.md
|   `-- snowflake_key_pair_auth.sql
|-- retail_sales_dbt/
|   |-- models/
|   |   |-- staging/
|   |   |-- dim_customer.sql
|   |   |-- dim_product.sql
|   |   |-- fact_sales.sql
|   |   |-- sales_daily.sql
|   |   `-- schema.yml
|   `-- dbt_project.yml
|-- scripts/
|   |-- generate_retail_data.py
|   |-- upload_to_s3.py
|   `-- load_snowflake.py
|-- .env.example
|-- .gitignore
|-- README.md
`-- requirements.txt
```

## Security

- `.env`, `.env.*`, `secrets/`, `*.p8`, and `*.pem` are ignored.
- Private keys, passwords, AWS secret keys, and tokens must never be committed.
- Snowflake receives only the public key body.
- Airflow receives credentials through environment variables, mounted local config, or secrets.

## Validation Commands

```powershell
python -m py_compile scripts/generate_retail_data.py scripts/upload_to_s3.py scripts/load_snowflake.py airflow/dags/retail_sales_pipeline.py
dbt parse --project-dir retail_sales_dbt
dbt debug --project-dir retail_sales_dbt
dbt run --project-dir retail_sales_dbt
dbt test --project-dir retail_sales_dbt
git status --short
git diff --check
```

## Future Improvements

- Add source freshness checks once batch timing is scheduled.
- Add snapshots for slowly changing dimensions.
- Add alerting for Airflow failures.
- Add CI checks for dbt parse and Python syntax.
- Deploy orchestration to a managed Airflow service.
- Publish a Power BI dashboard screenshot after the BI layer is connected.

## Resume Bullets

- Built an end-to-end retail sales data engineering pipeline using Python, AWS S3, Snowflake, dbt, and Airflow.
- Implemented idempotent S3-to-Snowflake ingestion with boto3 and Snowflake `MERGE` logic.
- Modeled analytics-ready fact, dimension, and aggregate tables in dbt with incremental processing and data quality tests.
- Designed a Power BI-ready Snowflake analytics layer for revenue, orders, product, customer, and city-level reporting.
#   c l o u d - r e t a i l - d a t a - e n g i n e e r i n g  
 