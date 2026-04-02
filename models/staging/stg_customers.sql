{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_customers_raw') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed