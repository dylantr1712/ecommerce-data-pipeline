{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_product_category_translation_raw') }}

),

renamed as (

    select
        product_category_name,
        product_category_name_english,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed