# Retail Sales Data Engineering Pipeline

An end-to-end retail sales data engineering pipeline built using Python, AWS S3, Snowflake, dbt, Docker, and Apache Airflow.

The pipeline generates retail sales data in batches, uploads the data to Amazon S3, loads new files into Snowflake, transforms the data using dbt, validates the transformed data, and orchestrates the complete workflow using Airflow.

## Architecture

```text
Python Data Generator
        |
        v
Retail CSV Batch
        |
        v
AWS S3
(Raw Data Layer)
        |
        v
Snowflake RAW
        |
        v
dbt Transformations
        |
        +----------------------+
        |                      |
        v                      v
Dimension Tables          FACT_SALES
                          |
                          v
                     SALES_DAILY
                          |
                          v
                    Analytics Layer
```
## Project Screenshots

### Airflow Pipeline

![Airflow successful DAG](docs/screenshots/airflow-success.png)

### Snowflake Analytics

![Snowflake FACT_SALES](docs/screenshots/snowflake-fact-sales.png)

### dbt Data Quality Tests

![dbt tests](docs/screenshots/dbt-tests.png)

### GitHub Actions CI

![GitHub Actions CI](docs/screenshots/github-actions.png)

### Execution Environment

```text
Windows Local Development
        |
        v
Docker Environment
        |
        +-------------------+
        |                   |
        v                   v
Apache Airflow            dbt
        |
        +-------------------+
        |
        v
Python Scripts
        |
        +-------------------+
        |                   |
        v                   v
     AWS S3             Snowflake
```

Airflow orchestrates the complete workflow, while Docker provides the local execution environment.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data generation and ingestion scripts |
| Pandas | Dataset creation and processing |
| NumPy | Synthetic data generation |
| AWS S3 | Cloud object storage for raw CSV files |
| Snowflake | Cloud data warehouse |
| SQL | Data loading and analytical queries |
| dbt | Data transformation, modeling, and testing |
| Apache Airflow | Workflow orchestration |
| Docker | Local containerized execution environment |
| Git & GitHub | Version control and project hosting |

## Pipeline Workflow

```text
Generate Data
     |
     v
Upload to S3
     |
     v
Load New File into Snowflake RAW
     |
     v
Run dbt Transformations
     |
     v
Run dbt Data Tests
     |
     v
Analytics-Ready Data
```

The complete workflow is orchestrated by the Airflow DAG:

```text
generate_data
      |
      v
upload_to_s3
      |
      v
load_snowflake
      |
      v
dbt_run
      |
      v
dbt_test
```

## 1. Data Generation

Retail sales data is generated using Python, Pandas, and NumPy.

The generator creates batch-based CSV files containing:

- Order ID
- Order date
- Customer ID
- Product
- Category
- Quantity
- Unit price
- City
- Payment method
- Total amount

Each execution generates a new timestamped batch.

Example:

```text
retail_sales_20260913161039.csv
```

Controlled data-quality issues such as missing payment methods are introduced to demonstrate validation and data-quality testing.

## 2. AWS S3 Raw Data Layer

Generated CSV files are uploaded to an Amazon S3 bucket.

Example structure:

```text
retail-sales-analytics-pipeline-2728/
└── raw/
    ├── retail_sales_20260913161039.csv
    ├── retail_sales_20260913171118.csv
    └── ...
```

S3 acts as the raw data layer and source for Snowflake ingestion.

## 3. Snowflake Data Warehouse

Snowflake contains two schemas:

```text
RETAIL_SALES_DB
├── RAW
│   ├── RETAIL_SALES
│   └── RETAIL_SALES_STAGE
│
└── ANALYTICS
    ├── STG_RETAIL_SALES
    ├── DIM_CUSTOMER
    ├── DIM_PRODUCT
    ├── FACT_SALES
    └── SALES_DAILY
```

### RAW Layer

The `RAW.RETAIL_SALES` table stores the ingested source data.

### ANALYTICS Layer

dbt transforms the raw data into analytics-ready models.

## 4. dbt Transformations

The dbt project contains five primary models.

### Staging

`stg_retail_sales`

Provides a staging view over the raw retail sales data for downstream transformations.

### Dimension Models

`dim_customer`

Contains customer-level information such as:

- Customer ID
- City

`dim_product`

Contains product-level information such as:

- Product
- Category
- Unit price

### Fact Model

`fact_sales`

Contains sales transaction-level data.

The model is configured as an incremental model using `ORDER_ID` as the unique key.

```sql
{{ config(
    materialized='incremental',
    unique_key='ORDER_ID'
) }}
```

On incremental runs, only records with a greater `ORDER_ID` than the existing maximum are processed.

### Aggregated Model

`sales_daily`

Aggregates sales by date and calculates:

- Total orders
- Total units sold
- Total revenue
- Average order value

## 5. Incremental Processing

The pipeline supports incremental batch ingestion.

The workflow is:

```text
New CSV Batch
     |
     v
S3
     |
     v
Snowflake COPY INTO
     |
     v
RAW.RETAIL_SALES
     |
     v
dbt Incremental FACT_SALES
     |
     v
SALES_DAILY
```

Snowflake load history prevents previously loaded files from being loaded again.

dbt incremental processing prevents already processed fact records from being rebuilt unnecessarily.

## 6. Data Quality

The dbt project includes automated data-quality tests.

The tests validate aspects such as:

- Required fields using not-null tests
- Unique order IDs
- Unique daily sales dates
- Unique customer and product records
- Required analytical metrics

The pipeline successfully completed:

```text
5 dbt models
21 dbt data tests
0 errors
0 warnings
```

## 7. Apache Airflow

Apache Airflow orchestrates the complete pipeline.

The DAG is:

```text
retail_sales_pipeline
```

Tasks:

```text
generate_data
      |
upload_to_s3
      |
load_snowflake
      |
dbt_run
      |
dbt_test
```

Airflow runs inside Docker along with the supporting local services.

## 8. Docker Environment

Docker provides a reproducible local environment for Airflow and dbt.

Main Airflow components include:

```text
Airflow API Server
Airflow Scheduler
Airflow DAG Processor
```

The dbt project is mounted into the Docker environment and executed as part of the Airflow workflow.

## Project Structure

```text
Retail-Sales-Data-Engineering/
│
├── airflow/
│   ├── dags/
│   │   └── retail_sales_pipeline.py
│   ├── Dockerfile
│   └── docker-compose.yaml
│
├── data/
│   ├── raw/
│   └── processed/
│
├── retail_sales_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_retail_sales.sql
│   │   ├── dim_customer.sql
│   │   ├── dim_product.sql
│   │   ├── fact_sales.sql
│   │   ├── sales_daily.sql
│   │   └── schema.yml
│   └── dbt_project.yml
│
├── scripts/
│   ├── generate_retail_data.py
│   ├── upload_to_s3.py
│   └── load_snowflake.py
│
├── .gitignore
└── README.md
```

## Running the Project

### 1. Generate Retail Data

```powershell
python scripts/generate_retail_data.py
```

This generates a new timestamped CSV batch inside:

```text
data/raw/
```

### 2. Upload Data to S3

```powershell
python scripts/upload_to_s3.py
```

The latest generated CSV file is uploaded to the S3 raw data layer.

### 3. Load Data into Snowflake

```powershell
python scripts/load_snowflake.py
```

The script identifies the latest CSV file in the Snowflake stage and loads it into the RAW table.

Snowflake load history prevents previously loaded files from being loaded again.

### 4. Run dbt

From the dbt project directory:

```powershell
cd retail_sales_dbt
dbt run
```

Run data-quality tests:

```powershell
dbt test
```

### 5. Run Airflow

Start the Docker environment:

```powershell
cd airflow
docker compose up -d
```

Open Airflow:

```text
http://localhost:8080
```

Trigger:

```text
retail_sales_pipeline
```

## Analytics

The final `SALES_DAILY` model provides daily sales metrics.

Example analytical query:

```sql
SELECT
    SALES_DATE,
    TOTAL_ORDERS,
    TOTAL_UNITS_SOLD,
    TOTAL_REVENUE,
    AVERAGE_ORDER_VALUE
FROM RETAIL_SALES_DB.ANALYTICS.SALES_DAILY
ORDER BY SALES_DATE DESC;
```

These datasets can be used for:

- Sales performance analysis
- Revenue trends
- Product analysis
- Customer analysis
- Daily sales reporting
- Business intelligence dashboards

## Key Data Engineering Concepts Demonstrated

- ETL / ELT pipeline design
- Batch data processing
- Cloud object storage
- Snowflake data warehousing
- External stages
- Incremental loading
- dbt transformations
- Data-quality testing
- Workflow orchestration
- Docker-based development
- SQL analytics
- Git version control
- Reproducible data pipelines

## Challenges Solved

### Incremental File Processing

The pipeline generates timestamped files and processes newly generated batches without repeatedly loading previously processed files.

### Duplicate File Protection

Snowflake load history identifies previously loaded files and skips them.

### Incremental dbt Models

`FACT_SALES` uses dbt incremental materialization to process new records efficiently.

### Data Quality

Controlled missing values and dbt tests demonstrate how data-quality issues can be detected and validated.

### Workflow Orchestration

Airflow connects generation, cloud storage, warehouse ingestion, transformation, and testing into one workflow.

### Reproducible Local Environment

Docker provides a consistent environment for Airflow and dbt.

## Project Outcome

The project demonstrates an end-to-end modern data engineering workflow:

```text
Python
  ↓
AWS S3
  ↓
Snowflake RAW
  ↓
dbt
  ↓
Snowflake ANALYTICS
  ↓
SQL / BI Analytics
```

The complete workflow is orchestrated using Apache Airflow and executed in a Docker-based local environment.

## Future Improvements

Potential extensions include:

- Scheduled Airflow DAG execution
- Airflow connections and secret management
- More granular AWS IAM permissions
- Slowly Changing Dimensions
- Additional data-quality checks
- Monitoring and alerting
- Snowflake performance optimization
- BI dashboard integration
- Extended CI/CD for dbt and Airflow deployment
- Cloud deployment of Airflow

## Author

**Yaswanth**

Data Engineering | Python | SQL | Snowflake | AWS | Airflow | dbt
