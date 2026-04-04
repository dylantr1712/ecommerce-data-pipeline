{{ config(materialized='table') }}

with distinct_status as (

    select distinct
        order_status
    from {{ ref('int_orders_enriched') }}
    where order_status is not null

)

select
    row_number() over (order by order_status) as order_status_key,
    order_status
from distinct_status