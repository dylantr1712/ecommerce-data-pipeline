{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_order_items_raw') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed