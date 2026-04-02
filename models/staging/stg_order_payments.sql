{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_order_payments_raw') }}

),

renamed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed