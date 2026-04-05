# Ecommerce Data Pipeline

## 1. Introduction

This project implements the design and implementation of an end-to-end data engineering pipeline using an ecommerce OLTP dataset.

The goal of the project is to transform raw transactional data into a scalable analytical data warehouse that enables business intelligence and analytics.

The pipeline demonstrates the architecture of a modern data stack including:

- Data Lake storage
- Distributed data processing
- Cloud Data Warehouse
- dbt-based transformations
- Dimensional modelling
- Workflow orchestration
- Business Intelligence dashboards

The dataset represents an ecommerce platform containing customer orders, payments, products, sellers, and reviews.

Key concepts demonstrated in this project include:

- OLTP → OLAP transformation
- Dimensional modelling
- Star schema design
- Slowly Changing Dimension (SCD Type 2)
- Modern ELT architecture
- Cloud-native data pipeline orchestration


---

# 2. Source Data (OLTP System)

The dataset consists of **8 relational tables** representing an ecommerce transactional system.

### Tables

- customers  
- orders  
- order_items  
- order_payments  
- order_reviews  
- products  
- sellers  
- product_category_translation  


## Source ERD

![Source ERD](./docs/ecomerce-data-pipeline-raw-erd.png)


### Key Characteristics

The source schema follows a typical **OLTP design**:

- highly normalized  
- optimized for transactions  
- contains multiple one-to-many relationships  

Example relationships:

- 1 customer → many orders  
- 1 order → many order items  
- 1 order → many payments  

While suitable for transactional workloads, this structure is not optimized for analytical queries because it requires heavy joins and can cause duplication in aggregations.


---

# 3. Architecture Design

The pipeline is designed following a **modern cloud data engineering architecture**.

Raw CSV Dataset  
↓  
Amazon S3 (Bronze Layer)  
↓  
Spark Processing (EMR Serverless)  
↓  
Amazon S3 (Silver Layer)  
↓  
Amazon Redshift (Data Warehouse)  
↓  
dbt Transformations  
↓  
Analytics / BI Layer  

Pipeline orchestration is handled by **Apache Airflow**, running locally through **Docker Compose**.

## Architecture Diagram

![Architecture](./architecture/pipeline_architecture.png)


---

# 4. Data Lake

## Bronze Layer (Raw Data)

Raw CSV files are ingested into **Amazon S3** as the Bronze layer using a Python ingestion script.

Characteristics:

- immutable storage  
- full historical traceability  
- schema preservation  

Example structure:

```
bronze/
   customers/
   orders/
   order_items/
   products/
   sellers/
   order_payments/
   order_reviews/
```

---

## Silver Layer (Processed Data)

Spark performs light transformations before loading into the warehouse.

Transformations include:

- schema validation  
- column normalization  
- type casting  
- basic data cleaning  
- deduplication  
- metadata column creation  

The cleaned datasets are stored as **Parquet files in the Silver layer** before loading into Redshift.


---

# 5. Data Warehouse

The analytics warehouse is implemented using **Amazon Redshift**.

Data modelling and transformations are managed using **dbt**.

## dbt Transformation Layers

```
RAW
  ↓
STAGING
  ↓
INTERMEDIATE
  ↓
DIMENSIONS
  ↓
FACTS
```


### Staging Layer

Standardizes raw source data.

Examples:

- stg_orders  
- stg_customers  
- stg_order_items  
- stg_products  

Responsibilities:

- column renaming  
- type casting  
- basic cleaning  


### Intermediate Layer

Intermediate models prepare datasets for dimensional modelling.

Examples:

- int_orders_enriched  
- int_order_items_enriched  
- int_products_enriched  
- int_customers_deduped  

Responsibilities:

- deduplication  
- enrichment  
- business logic  


---

# 6. Slowly Changing Dimension (SCD Type 2)

Customer history tracking is implemented using **dbt snapshots**.

Instead of manually implementing SCD Type 2 logic with SQL, the project uses dbt's built-in snapshot functionality to automatically track changes to dimensional attributes.

The snapshot monitors changes to selected customer attributes such as:

- customer_city  
- customer_state  
- customer_zip_code_prefix  

When a change is detected, dbt automatically creates a new historical record while preserving previous versions.

Snapshot records include:

- dbt_valid_from  
- dbt_valid_to  

The **dim_customers** table then derives the analytical SCD fields:

- effective_date  
- end_date  
- is_current  

This approach simplifies SCD implementation and follows modern dbt best practices for maintaining historical dimensional data.


---

# 7. Final Data Model (Star Schema)

The warehouse uses a **star schema optimized for analytical queries**.

## Star Schema Diagram

![Star Schema](./docs/dimensional-db-scdtype2-v1.png)


---

# Fact Tables

### fct_order_items

Primary sales dataset.

**Grain**

1 row per product in an order

**Measures**

- price  
- freight_value  
- total_item_value  


### fct_order_payments

Captures payment behaviour.

**Grain**

1 row per payment transaction.

Handles multiple payments per order.


### fct_order_reviews

Stores customer feedback.

**Grain**

1 row per review.

Supports analysis of customer satisfaction.


---

# Dimension Tables

### dim_customers (SCD Type 2)

Tracks historical changes in customer attributes.

Key fields:

- customer_key  
- customer_id  
- effective_date  
- end_date  
- is_current  


### Other Dimensions

- dim_products  
- dim_sellers  
- dim_order_status  
- dim_payment_type  
- dim_date  


---

# 8. Record Counts (Example)

| Table | Approx Rows |
|------|-------------|
| dim_customers | ~100k |
| dim_products | ~32k |
| dim_sellers | ~3k |
| dim_date | ~3000 |
| fct_order_items | ~112k |
| fct_order_payments | ~103k |
| fct_order_reviews | ~100k |


---

# 9. Project Structure

```
ecommerce-data-pipeline/

dags/
   ecommerce_pipeline.py

scripts/
   ingestion/
      upload_to_s3.py
   spark/
      bronze_to_silver.py
   loaders/
      run_redshift_sql.py

sql/
   schemas_and_tables_redshift.sql
   load_parquet_redshift.sql

models/
   staging/
   intermediate/
   marts/
      dimensions/
      facts/

snapshots/
   customers_snapshot.sql

data/
   source/

docs/
   ERD diagrams

architecture/
   pipeline diagrams

docker-compose.yaml
dbt_project.yml
packages.yml
requirements.txt
```


---

# 10. Tech Stack

| Tool | Responsibility |
|-----|---------------|
| Apache Airflow | Pipeline orchestration |
| Docker | Local Airflow environment |
| Amazon S3 | Data lake storage |
| Apache Spark | Data processing |
| EMR Serverless | Distributed Spark execution |
| Amazon Redshift | Data warehouse |
| dbt | Data modelling |
| Power BI | Business intelligence |


---

# 11. Business Use Cases

The dimensional model supports the following analytics:

- Revenue analysis by product or category  
- Customer purchasing behaviour  
- Seller performance tracking  
- Payment method analysis  
- Delivery performance vs customer reviews  


---

# 12. Example Business Recommendations

### Improve Delivery Performance

Analyze how delivery delays influence review scores and customer satisfaction.

### Optimize Payment Installments

Evaluate whether installment plans increase order value and conversion rates.


---

# 13. Challenges and Lessons Learned

### Dimensional Modelling

Transforming normalized OLTP data into an analytical star schema required careful definition of fact grains.

### SCD Type 2 Implementation

Customer history tracking was implemented using **dbt snapshots**, simplifying the process of maintaining historical records compared to traditional SQL-based SCD logic.

### Data Quality

Several fields contained missing timestamps or inconsistent values, requiring flexible modelling and data validation tests.


---

# 14. Future Improvements

Planned enhancements include:

- Power BI dashboard development  
- Additional dbt data quality tests  
- Pipeline monitoring and alerting  
- CI/CD integration  
- Infrastructure-as-Code deployment  
