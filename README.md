# Ecommerce Data Pipeline

## 1. Introduction

This project implements the design and modelling phase of an end-to-end data engineering pipeline using an ecommerce OLTP dataset.

The goal of the project is to transform raw transactional data into a scalable analytical data warehouse that enables business intelligence and analytics.

The pipeline demonstrates the architecture of a modern data stack including:

- Data Lake storage
- Distributed data processing
- Cloud Data Warehouse
- dbt-based transformations
- Dimensional modelling
- Business Intelligence dashboards

The dataset represents an ecommerce platform containing customer orders, payments, products, sellers, and reviews.

Key concepts demonstrated in this project include:

- OLTP → OLAP transformation
- Dimensional modelling
- Star schema design
- Slowly Changing Dimension (SCD Type 2)
- Modern ELT architecture
- Data pipeline design


# 2. Source Data (OLTP System)

The dataset consists of 8 relational tables representing an ecommerce transactional system.

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

The source schema follows a typical OLTP design:

- highly normalized
- optimized for transactions
- contains multiple one-to-many relationships

Example relationships:

- 1 customer → many orders  
- 1 order → many order items  
- 1 order → many payments  

While suitable for transactional workloads, this structure is not optimized for analytical queries because it requires heavy joins and can cause duplication in aggregations.


# 3. Architecture Design

The pipeline is designed following a modern data engineering architecture.

CSV Dataset  
↓  
Amazon S3 (Bronze Layer)  
↓  
Spark Processing  
↓  
Amazon S3 (Silver Layer)  
↓  
Amazon Redshift (Data Warehouse)  
↓  
dbt Transformations  
↓  
Power BI Dashboard  


## Architecture Diagram

![Architecture](./architecture/pipeline_architecture.png)


## Design Principles

### Separation of Concerns

Each stage of the pipeline has a clear responsibility:

| Layer | Responsibility |
|------|---------------|
| Data Lake | raw data storage |
| Spark Processing | schema validation and cleaning |
| Data Warehouse | analytical storage |
| dbt | data modelling and transformations |
| BI | visualization and insights |


### ELT Architecture

The pipeline follows an ELT approach:

1. Extract raw data
2. Load into warehouse
3. Transform using dbt

This allows the data warehouse to perform scalable transformations.


### Scalability

Cloud-native components enable the pipeline to scale:

- Object storage for large datasets
- Distributed processing using Spark
- Analytical warehouse for transformations


# 4. Data Lake

## Bronze Layer (Raw Data)

Raw CSV files are ingested into Amazon S3 as the Bronze layer.

Characteristics:

- immutable storage
- full historical traceability
- schema preservation

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


## Silver Layer (Light Transformations)

Light transformations are applied using Spark before loading into the warehouse.

Transformations include:

- schema validation
- column normalization
- basic data cleaning
- format standardization

The cleaned datasets are stored in the Silver layer before loading to Redshift.


# 5. Data Warehouse

The analytics warehouse is implemented using Amazon Redshift.

Data modelling and transformations are managed using dbt.


## dbt Transformation Layers

The warehouse follows a layered dbt architecture:

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


# 6. Final Data Model (Star Schema)

The warehouse uses a star schema optimized for analytics.


## Star Schema Diagram

![Star Schema](./docs/dimensional-db-scdtype2-v1.png)


## Fact Tables

### fct_order_items

Primary sales dataset.

Grain:  
1 row per product in an order

Measures:

- price
- freight_value
- total_item_value


### fct_order_payments

Captures payment behaviour.

Grain:  
1 row per payment transaction

Handles multiple payments per order.


### fct_order_reviews

Stores customer feedback.

Grain:  
1 row per review

Supports analysis of customer satisfaction.


## Dimension Tables

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


# 7. Record Counts (Example)

| Table | Approx Rows |
|------|-------------|
| dim_customers | ~100k |
| dim_products | ~32k |
| dim_sellers | ~3k |
| dim_date | ~3000 |
| fct_order_items | ~112k |
| fct_order_payments | ~103k |
| fct_order_reviews | ~100k |


# 8. Project Structure

ecommerce-data-pipeline/

airflow/
   dags/

spark/
   processing scripts

data/
   raw datasets

dbt/
   ecommerce_data_pipeline/
      models/
         staging/
         intermediate/
         marts/
            dimensions/
            facts/

architecture/
   pipeline diagrams

docs/
   ERD diagrams

dashboards/
   Power BI reports


# 9. Tech Stack

| Tool | Responsibility |
|-----|---------------|
| Airflow | Pipeline orchestration |
| Amazon S3 | Data lake storage |
| Apache Spark | Data processing |
| Amazon Redshift | Data warehouse |
| dbt | Data modelling |
| Power BI | Business intelligence |


# 10. Business Use Cases

The dimensional model supports the following analytics:

- Revenue analysis by product or category
- Customer purchasing behaviour
- Seller performance tracking
- Payment method analysis
- Delivery performance vs customer reviews


# 11. Example Business Recommendations

### Improve Delivery Performance

Analyze how delivery delays influence review scores and customer satisfaction.

### Optimize Payment Installments

Evaluate whether installment plans increase order value and conversion rates.


# 12. Challenges and Lessons Learned

### Dimensional Modelling

Transforming normalized OLTP data into an analytical star schema required careful definition of fact grains.

### SCD Type 2 Implementation

Tracking historical customer changes required surrogate keys and effective date logic.

### Data Quality

Several fields contained missing timestamps or inconsistent values, requiring flexible modelling and data tests.


# 13. Work in Progress

The following components are currently being implemented:

- Airflow DAG orchestration
- Slack pipeline notifications
- Spark ingestion pipeline
- Power BI dashboards
- dbt lineage visualization

These components will complete the full end-to-end data engineering pipeline.
# Ecommerce Data Pipeline

## 1. Introduction

This project implements the design and modelling phase of an end-to-end data engineering pipeline using an ecommerce OLTP dataset.

The goal of the project is to transform raw transactional data into a scalable analytical data warehouse that enables business intelligence and analytics.

The pipeline demonstrates the architecture of a modern data stack including:

- Data Lake storage
- Distributed data processing
- Cloud Data Warehouse
- dbt-based transformations
- Dimensional modelling
- Business Intelligence dashboards

The dataset represents an ecommerce platform containing customer orders, payments, products, sellers, and reviews.

Key concepts demonstrated in this project include:

- OLTP → OLAP transformation
- Dimensional modelling
- Star schema design
- Slowly Changing Dimension (SCD Type 2)
- Modern ELT architecture
- Data pipeline design


# 2. Source Data (OLTP System)

The dataset consists of 8 relational tables representing an ecommerce transactional system.

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

The source schema follows a typical OLTP design:

- highly normalized
- optimized for transactions
- contains multiple one-to-many relationships

Example relationships:

- 1 customer → many orders  
- 1 order → many order items  
- 1 order → many payments  

While suitable for transactional workloads, this structure is not optimized for analytical queries because it requires heavy joins and can cause duplication in aggregations.


# 3. Architecture Design

The pipeline is designed following a modern data engineering architecture.

CSV Dataset  
↓  
Amazon S3 (Bronze Layer)  
↓  
Spark Processing  
↓  
Amazon S3 (Silver Layer)  
↓  
Amazon Redshift (Data Warehouse)  
↓  
dbt Transformations  
↓  
Power BI Dashboard  


## Architecture Diagram

![Architecture](./architecture/pipeline_architecture.png)


## Design Principles

### Separation of Concerns

Each stage of the pipeline has a clear responsibility:

| Layer | Responsibility |
|------|---------------|
| Data Lake | raw data storage |
| Spark Processing | schema validation and cleaning |
| Data Warehouse | analytical storage |
| dbt | data modelling and transformations |
| BI | visualization and insights |


### ELT Architecture

The pipeline follows an ELT approach:

1. Extract raw data
2. Load into warehouse
3. Transform using dbt

This allows the data warehouse to perform scalable transformations.


### Scalability

Cloud-native components enable the pipeline to scale:

- Object storage for large datasets
- Distributed processing using Spark
- Analytical warehouse for transformations


# 4. Data Lake

## Bronze Layer (Raw Data)

Raw CSV files are ingested into Amazon S3 as the Bronze layer.

Characteristics:

- immutable storage
- full historical traceability
- schema preservation

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


## Silver Layer (Light Transformations)

Light transformations are applied using Spark before loading into the warehouse.

Transformations include:

- schema validation
- column normalization
- basic data cleaning
- format standardization

The cleaned datasets are stored in the Silver layer before loading to Redshift.


# 5. Data Warehouse

The analytics warehouse is implemented using Amazon Redshift.

Data modelling and transformations are managed using dbt.


## dbt Transformation Layers

The warehouse follows a layered dbt architecture:

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


# 6. Final Data Model (Star Schema)

The warehouse uses a star schema optimized for analytics.


## Star Schema Diagram

![Star Schema](./docs/dimensional-db-scdtype2-v1.png)


## Fact Tables

### fct_order_items

Primary sales dataset.

Grain:  
1 row per product in an order

Measures:

- price
- freight_value
- total_item_value


### fct_order_payments

Captures payment behaviour.

Grain:  
1 row per payment transaction

Handles multiple payments per order.


### fct_order_reviews

Stores customer feedback.

Grain:  
1 row per review

Supports analysis of customer satisfaction.


## Dimension Tables

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


# 7. Record Counts (Example)

| Table | Approx Rows |
|------|-------------|
| dim_customers | ~100k |
| dim_products | ~32k |
| dim_sellers | ~3k |
| dim_date | ~3000 |
| fct_order_items | ~112k |
| fct_order_payments | ~103k |
| fct_order_reviews | ~100k |


# 8. Project Structure

ecommerce-data-pipeline/

airflow/
   dags/

spark/
   processing scripts

data/
   raw datasets

dbt/
   ecommerce_data_pipeline/
      models/
         staging/
         intermediate/
         marts/
            dimensions/
            facts/

architecture/
   pipeline diagrams

docs/
   ERD diagrams

dashboards/
   Power BI reports


# 9. Tech Stack

| Tool | Responsibility |
|-----|---------------|
| Airflow | Pipeline orchestration |
| Amazon S3 | Data lake storage |
| Apache Spark | Data processing |
| Amazon Redshift | Data warehouse |
| dbt | Data modelling |
| Power BI | Business intelligence |


# 10. Business Use Cases

The dimensional model supports the following analytics:

- Revenue analysis by product or category
- Customer purchasing behaviour
- Seller performance tracking
- Payment method analysis
- Delivery performance vs customer reviews


# 11. Example Business Recommendations

### Improve Delivery Performance

Analyze how delivery delays influence review scores and customer satisfaction.

### Optimize Payment Installments

Evaluate whether installment plans increase order value and conversion rates.


# 12. Challenges and Lessons Learned

### Dimensional Modelling

Transforming normalized OLTP data into an analytical star schema required careful definition of fact grains.

### SCD Type 2 Implementation

Tracking historical customer changes required surrogate keys and effective date logic.

### Data Quality

Several fields contained missing timestamps or inconsistent values, requiring flexible modelling and data tests.


# 13. Work in Progress

The following components are currently being implemented:

- Airflow DAG orchestration
- Slack pipeline notifications
- Spark ingestion pipeline
- Power BI dashboards
- dbt lineage visualization

These components will complete the full end-to-end data engineering pipeline.
