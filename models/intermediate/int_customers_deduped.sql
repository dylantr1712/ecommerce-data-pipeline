{{ config(materialized='view') }}

with source_customers as (

    select *
    from {{ ref('stg_customers') }}

),

deduplicated as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ingestion_timestamp,
        source_file_name,

        md5(
            coalesce(cast(customer_unique_id as varchar), '') || '|' ||
            coalesce(cast(customer_zip_code_prefix as varchar), '') || '|' ||
            coalesce(cast(customer_city as varchar), '') || '|' ||
            coalesce(cast(customer_state as varchar), '')
        ) as customer_attr_hash,

        row_number() over (
            partition by customer_id, ingestion_timestamp
            order by source_file_name desc
        ) as rn

    from source_customers

)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingestion_timestamp,
    source_file_name,
    customer_attr_hash
from deduplicated
where rn = 1