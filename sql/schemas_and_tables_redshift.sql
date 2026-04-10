create schema if not exists staging;
create schema if not exists analytics;

create table if not exists staging.stg_orders_raw (
    order_id varchar(50),
    customer_id varchar(50),
    order_status varchar(30),
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);


create table if not exists staging.stg_customers_raw (
    customer_id varchar(50),
    customer_unique_id varchar(50),
    customer_zip_code_prefix varchar(20),
    customer_city varchar(100),
    customer_state varchar(5),
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);


create table if not exists staging.stg_order_items_raw (
    order_id varchar(50),
    order_item_id integer,
    product_id varchar(50),
    seller_id varchar(50),
    shipping_limit_date timestamp,
    price decimal(12,2),
    freight_value decimal(12,2),
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);


create table if not exists staging.stg_order_payments_raw (
    order_id varchar(50),
    payment_sequential integer,
    payment_type varchar(50),
    payment_installments integer,
    payment_value decimal(12,2),
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);

create table if not exists staging.stg_order_reviews_raw (
    review_id varchar(50),
    order_id varchar(50),
    review_score integer,
    review_comment_title varchar(255),
    review_comment_message varchar(2000),
    review_creation_date timestamp,
    review_answer_timestamp timestamp,
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);

create table if not exists staging.stg_products_raw (
    product_id varchar(50),
    product_category_name varchar(100),
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);


create table if not exists staging.stg_sellers_raw (
    seller_id varchar(50),
    seller_zip_code_prefix varchar(20),
    seller_city varchar(100),
    seller_state varchar(5),
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);

create table if not exists staging.stg_product_category_translation_raw (
    product_category_name varchar(100),
    product_category_name_english varchar(100),
    ingestion_timestamp timestamp,
    source_file_name varchar(1000)
);
