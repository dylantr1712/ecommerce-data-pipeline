{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_orders_raw') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed