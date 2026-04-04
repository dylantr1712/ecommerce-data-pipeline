{{ config(materialized='table') }}

with distinct_payment_type as (

    select distinct
        payment_type
    from {{ ref('stg_order_payments') }}
    where payment_type is not null

)

select
    row_number() over (order by payment_type) as payment_type_key,
    payment_type
from distinct_payment_type