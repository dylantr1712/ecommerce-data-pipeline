# Ecommerce Data Pipeline

## Overview

This project demonstrates the **design and modelling of an end‑to‑end
data engineering pipeline** using an ecommerce OLTP dataset.\
The goal is to transform raw transactional data into a **scalable
analytics warehouse** that supports reliable business intelligence and
reporting.

The project showcases modern **data engineering practices**, including:

-   Data lake architecture
-   Distributed processing
-   Cloud data warehouse design
-   dbt transformation workflows
-   Dimensional modelling
-   Business intelligence dashboards

------------------------------------------------------------------------

# Architecture

    CSV Dataset
         │
         ▼
    Amazon S3 (Bronze Layer)
         │
         ▼
    Spark Processing
         │
         ▼
    Amazon S3 (Silver Layer)
         │
         ▼
    Amazon Redshift (Data Warehouse)
         │
         ▼
    dbt Transformations
         │
         ▼
    Power BI Dashboards

This architecture follows a **modern ELT pipeline design**, separating
ingestion, storage, transformation, and visualization.

------------------------------------------------------------------------

# Source Data

The dataset originates from an **ecommerce transactional system
(OLTP)**.

## Tables

-   customers
-   orders
-   order_items
-   order_payments
-   order_reviews
-   products
-   sellers
-   product_category_translation

These tables represent a normalized transactional schema optimized for
operational workloads.

------------------------------------------------------------------------

# Why Transformation is Required

OLTP systems are **not designed for analytics**. They often require:

-   complex joins
-   repeated aggregations
-   slow query performance

To solve this, the pipeline transforms the normalized schema into a
**star schema optimized for analytical queries**.

------------------------------------------------------------------------

# Data Lake

## Bronze Layer

Raw CSV files are stored in **Amazon S3**.

Characteristics:

-   Immutable storage
-   Historical traceability
-   Raw ingestion layer

Example structure:

    data-lake/
      bronze/
        customers/
        orders/
        order_items/
        products/
        sellers/
        payments/
        reviews/

------------------------------------------------------------------------

## Silver Layer

Data is cleaned and standardized using **Apache Spark**.

Spark transformations include:

-   schema validation
-   column normalization
-   data cleaning
-   data standardization

Processed datasets are written back to S3 as the **Silver layer**.

------------------------------------------------------------------------

# Data Warehouse

The analytical warehouse is implemented using **Amazon Redshift**.

Data modelling and transformations are managed using **dbt (data build
tool)**.

## dbt Model Layers

    RAW
     ↓
    STAGING
     ↓
    INTERMEDIATE
     ↓
    DIMENSIONS
     ↓
    FACTS

### Staging Layer

Standardizes raw source tables.

Examples:

-   stg_orders
-   stg_customers
-   stg_order_items
-   stg_products

Responsibilities:

-   column renaming
-   type casting
-   data cleaning

------------------------------------------------------------------------

### Intermediate Layer

Prepares datasets for dimensional modelling.

Examples:

-   int_orders_enriched
-   int_order_items_enriched
-   int_products_enriched
-   int_customers_deduped

Responsibilities:

-   deduplication
-   enrichment
-   business logic

------------------------------------------------------------------------

# Final Data Model

The warehouse uses a **Star Schema**.

## Fact Tables

### fct_order_items

Primary sales dataset.

Grain: 1 row per product within an order

Measures:

-   price
-   freight_value
-   total_item_value

------------------------------------------------------------------------

### fct_order_payments

Captures payment behaviour.

Grain: 1 row per payment transaction

Supports analysis of multiple payment methods per order.

------------------------------------------------------------------------

### fct_order_reviews

Captures customer feedback.

Grain: 1 row per review

Used for customer satisfaction analysis.

------------------------------------------------------------------------

## Dimension Tables

### dim_customers (SCD Type 2)

Tracks historical customer changes.

Columns:

-   customer_key
-   customer_id
-   effective_date
-   end_date
-   is_current

Other dimensions:

-   dim_products
-   dim_sellers
-   dim_payment_type
-   dim_order_status
-   dim_date

------------------------------------------------------------------------

# Project Structure

    ecommerce-data-pipeline/

    airflow/
      dags/

    spark/
      processing_scripts/

    data/
      raw_datasets/

    dbt/
      ecommerce_data_pipeline/
        models/
          staging/
          intermediate/
          marts/
            dimensions/
            facts/

    docs/
      erd_diagrams/

    dashboards/
      powerbi_reports/

------------------------------------------------------------------------

# Tech Stack

  Tool              Purpose
  ----------------- ------------------------
  Airflow           Pipeline orchestration
  Amazon S3         Data lake storage
  Apache Spark      Data processing
  Amazon Redshift   Data warehouse
  dbt               Data transformation
  Power BI          Business intelligence

------------------------------------------------------------------------

# Business Insights

The warehouse enables analytics such as:

-   revenue trends
-   customer purchasing behaviour
-   seller performance
-   payment method analysis
-   delivery impact on customer reviews

------------------------------------------------------------------------

# Challenges & Lessons Learned

## Dimensional Modelling

Transforming normalized OLTP schemas into a star schema requires careful
definition of **fact table grain**.

## SCD Type 2

Tracking historical customer attributes required implementing
**surrogate keys and effective dating logic**.

## Data Quality

Several fields contained missing or inconsistent values requiring
flexible cleaning strategies.

------------------------------------------------------------------------

# Work In Progress

The following components are currently being implemented:

-   Airflow DAG orchestration
-   Spark ingestion pipeline
-   Power BI dashboard
-   Slack pipeline alerts
-   dbt lineage documentation

These additions will complete the **full end‑to‑end data engineering
pipeline**.
