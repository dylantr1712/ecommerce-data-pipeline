{{ config(materialized='view') }}

with products as (

    select *
    from {{ ref('stg_products') }}

),

category_translation as (

    select *
    from {{ ref('stg_product_category_translation') }}

),

final as (

    select
        p.product_id,
        p.product_category_name,
        ct.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.ingestion_timestamp,
        p.source_file_name
    from products p
    left join category_translation ct
        on p.product_category_name = ct.product_category_name

)

select *
from final