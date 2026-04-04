{{ config(materialized='table') }}

with reviews as (

    select *
    from {{ ref('stg_order_reviews') }}

),

orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

dim_customers as (

    select *
    from {{ ref('dim_customers') }}

),

dim_order_status as (

    select *
    from {{ ref('dim_order_status') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['r.review_id', 'r.order_id']) }} as order_review_fact_key,

        r.review_id,
        r.order_id,

        dc.customer_key,
        dos.order_status_key,

        cast(to_char(cast(o.order_purchase_timestamp as date), 'YYYYMMDD') as integer) as purchase_date_key,

        case
            when o.order_delivered_customer_date is not null
            then cast(to_char(cast(o.order_delivered_customer_date as date), 'YYYYMMDD') as integer)
            else null
        end as delivered_customer_date_key,

        cast(to_char(cast(r.review_creation_date as date), 'YYYYMMDD') as integer) as review_creation_date_key,
        cast(to_char(cast(r.review_answer_timestamp as date), 'YYYYMMDD') as integer) as review_answer_date_key,

        r.review_score,

        case
            when coalesce(r.review_comment_title, '') <> ''
              or coalesce(r.review_comment_message, '') <> ''
            then true
            else false
        end as has_review_comment

    from reviews r
    left join orders o
        on r.order_id = o.order_id
    left join dim_customers dc
        on o.customer_id = dc.customer_id
       and dc.is_current = true
    left join dim_order_status dos
        on o.order_status = dos.order_status

)

select *
from final