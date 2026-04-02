{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_sellers_raw') }}

),

renamed as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed