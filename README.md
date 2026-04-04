Ecommerce Data Pipeline
1. Introduction

This project implements the design and modelling phase of an end-to-end data engineering pipeline using an ecommerce OLTP dataset.

The objective is to transform raw transactional data into a scalable analytical data warehouse that enables reliable analytics and business intelligence.

The pipeline demonstrates the design of a modern data platform integrating:

Data lake storage
Distributed processing
Cloud data warehouse
dbt-based transformation
Dimensional modelling
Business intelligence dashboards

The dataset originates from an ecommerce OLTP system containing customer orders, payments, products, sellers and reviews.

This project demonstrates core data engineering concepts including:

OLTP → OLAP transformation
Dimensional modelling
Star schema design
Slowly Changing Dimensions (SCD Type 2)
Modern ELT architecture
Cloud data pipeline design

2. Source Data (OLTP System)

The source dataset contains 8 relational tables extracted from an ecommerce transactional system.

Tables
customers
orders
order_items
order_payments
order_reviews
products
sellers
product_category_translation

Source ERD
![Source ERD](./docs/ecomerce-data-pipeline-raw-erd.png)

Key Characteristics

The source schema follows a typical OLTP design:

Highly normalized
Optimized for transactional operations
Many one-to-many relationships

Example relationships:

customers
   │
   │
orders ── order_items ── products
   │            │
   │            └── sellers
   │
   ├── payments
   │
   └── reviews

While suitable for transactions, this schema is not optimized for analytics because:

heavy joins are required
duplication risk in aggregations
poor performance for analytical queries

3. Architecture Design

The pipeline follows a modern data engineering architecture.

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

Architecture Diagram

Design Principles
Separation of Concerns

Each stage of the pipeline performs a specific responsibility:

Layer	Responsibility
Data Lake	raw data storage
Processing	schema cleaning and preparation
Warehouse	analytical storage
dbt	transformation and modelling
BI	visualization and insights
ELT Architecture

This project follows the ELT paradigm:

Extract raw data
Load into warehouse
Transform using dbt

This approach allows transformations to leverage the scalability of the warehouse.

Scalability

The architecture uses cloud-native technologies including:

object storage
distributed compute
cloud warehouse
transformation orchestration

This design supports large-scale datasets and automated pipelines.

4. Data Lake
Bronze Layer (Raw Data)

The raw CSV dataset is stored in Amazon S3 as the Bronze layer.

Characteristics:

immutable raw storage
full historical traceability
minimal processing

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
Silver Layer (Light Transformations)

Light transformations are applied before loading data into the warehouse.

Spark is used to:

validate schemas
normalize column formats
clean inconsistent records
standardize datasets

The transformed output is stored in the Silver layer.

5. Data Warehouse

The analytics warehouse is implemented using Amazon Redshift.

Data modelling and transformations are managed using dbt.

dbt Transformation Layers

The warehouse uses a layered dbt architecture:

RAW
 ↓
STAGING
 ↓
INTERMEDIATE
 ↓
DIMENSIONS
 ↓
FACTS
Staging Layer

Standardizes raw source data.

Examples:

stg_orders
stg_customers
stg_order_items
stg_products

Responsibilities:

rename columns
type casting
basic cleaning
Intermediate Layer

Intermediate models prepare datasets for dimensional modelling.

Examples:

int_orders_enriched
int_order_items_enriched
int_products_enriched
int_customers_deduped

Responsibilities:

deduplication
enrichment
business logic

6. Final Data Model (Star Schema)

The warehouse uses a star schema optimized for analytics.

Star Schema Diagram
![Star Schema](./docs/dimensional-db-scdtype2-v1.png)

Fact Tables
fct_order_items

Primary sales dataset.

Grain:

1 row per product within an order

Measures:

price
freight_value
total_item_value
fct_order_payments

Captures payment behaviour.

Grain:

1 row per payment transaction

Handles multiple payments per order.

fct_order_reviews

Stores customer feedback.

Grain:

1 row per review

Supports analysis of customer satisfaction.

Dimension Tables
dim_customers (SCD Type 2)

Tracks historical changes in customer attributes.

Key columns:

customer_key
customer_id
effective_date
end_date
is_current
Other Dimensions
dim_products
dim_sellers
dim_order_status
dim_payment_type
dim_date

7. dbt Lineage Graph

dbt provides automated dependency management between models.

Example lineage flow:

stg_orders
      │
      ▼
int_orders_enriched
      │
      ▼
fct_order_items

This ensures transformations are:

reproducible
traceable
dependency-aware

(Insert dbt lineage graph screenshot here)

8. Record Counts (Example)
Table	Approx Rows
dim_customers	~100k
dim_products	~32k
dim_sellers	~3k
dim_date	~3000
fct_order_items	~112k
fct_order_payments	~103k
fct_order_reviews	~100k

9. Project Structure
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

10. Tech Stack
Tool	Role
Airflow	Pipeline orchestration
Amazon S3	Data lake storage
Apache Spark	Data processing
Amazon Redshift	Data warehouse
dbt	Data modelling
Power BI	Business intelligence

11. Business Insights

The data model enables analysis such as:

revenue trends
customer purchasing patterns
seller performance
payment behaviour
delivery impact on reviews

12. Example Business Recommendations
Improve Delivery Performance

Analyze how delivery delays influence review scores and customer satisfaction.

Optimize Payment Installments

Evaluate whether installment plans increase order value and sales conversion.

13. Challenges and Lessons Learned
Dimensional Modelling

Transforming normalized OLTP data into an analytical star schema required careful definition of fact grains.

SCD Type 2 Implementation

Tracking historical customer changes required surrogate keys and effective date logic.

Data Quality

Some timestamps and review fields contained missing values which required flexible modelling.

14. Work in Progress

The following components are currently being implemented:

Airflow DAG orchestration
Slack notification integration
Spark ingestion pipeline
Power BI dashboard
dbt lineage screenshots
architecture documentation

These components will complete the full end-to-end data engineering pipeline.