# Ecommerce Data Pipeline

## Introduction
This project implements an end-to-end data engineering pipeline using an ecommerce OLTP dataset.

The pipeline ingests raw CSV data, processes it in a data lake, loads it into a data warehouse, and generates business insights through a Power BI dashboard.

## Architecture

Pipeline Overview:

CSV → S3 Raw → Spark Transform → S3 Silver → Spark Load → Redshift → dbt Models → Power BI Dashboard

Technologies used:

- Airflow (orchestration)
- Amazon S3 (data lake)
- Apache Spark (data processing)
- Amazon Redshift (data warehouse)
- dbt (data modeling)
- Power BI (business intelligence)

## Source Data

Dataset contains 8 CSV tables from an ecommerce OLTP system including:

- customers
- orders
- order_items
- payments
- products
- sellers
- reviews
- geolocation

## Final Data Model

The warehouse uses a star schema:

Fact Tables
- fct_orders
- fct_order_items
- fct_payments

Dimension Tables
- dim_customers
- dim_products
- dim_sellers
- dim_date

SCD Type 2 is implemented on customer dimension using dbt snapshots.

## Tech Stack

| Tool | Purpose |
|-----|------|
Airflow | Pipeline orchestration |
S3 | Data lake storage |
Spark | Data transformation |
Redshift | Data warehouse |
dbt | Modeling + SCD Type 2 |
Power BI | Business dashboard |

## Running the Pipeline

Steps:

1. Upload CSV files to local `data/source`
2. Run Airflow DAG
3. Ingest data into S3 raw
4. Spark cleans and converts to Parquet
5. Spark loads into Redshift staging
6. dbt builds dimensional models
7. Power BI connects to warehouse

## Business Insights

Power BI dashboard provides:

- revenue trends
- payment behavior
- customer analysis
- product performance

Recommendations are included in the dashboard analysis.

## Challenges and Lessons Learned

- Handling inconsistent CSV schemas
- Designing a dimensional model
- Implementing SCD Type 2 with dbt snapshots