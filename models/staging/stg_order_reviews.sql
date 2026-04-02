{{ config(materialized='view') }}

with source as (

    select * from {{ source('ecommerce', 'stg_order_reviews_raw') }}

),

renamed as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        ingestion_timestamp,
        source_file_name
    from source

)

select * from renamed