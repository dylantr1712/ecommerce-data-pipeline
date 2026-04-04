{{ config(materialized='table') }}

with order_items as (

    select *
    from {{ ref('int_order_items_enriched') }}

),

dim_products as (

    select *
    from {{ ref('dim_products') }}

),

dim_sellers as (

    select *
    from {{ ref('dim_sellers') }}

),

dim_order_status as (

    select *
    from {{ ref('dim_order_status') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} as order_item_fact_key,

        oi.order_id,
        oi.order_item_id,

        -- temporary natural key until dim_customers SCD2 is implemented
        oi.customer_id,

        dp.product_key,
        ds.seller_key,
        dos.order_status_key,

        cast(to_char(cast(oi.order_purchase_timestamp as date), 'YYYYMMDD') as integer) as purchase_date_key,

        case
            when oi.order_approved_at is not null
            then cast(to_char(cast(oi.order_approved_at as date), 'YYYYMMDD') as integer)
            else null
        end as approved_date_key,

        case
            when oi.shipping_limit_date is not null
            then cast(to_char(cast(oi.shipping_limit_date as date), 'YYYYMMDD') as integer)
            else null
        end as shipping_limit_date_key,

        case
            when oi.order_delivered_carrier_date is not null
            then cast(to_char(cast(oi.order_delivered_carrier_date as date), 'YYYYMMDD') as integer)
            else null
        end as delivered_carrier_date_key,

        case
            when oi.order_delivered_customer_date is not null
            then cast(to_char(cast(oi.order_delivered_customer_date as date), 'YYYYMMDD') as integer)
            else null
        end as delivered_customer_date_key,

        case
            when oi.order_estimated_delivery_date is not null
            then cast(to_char(cast(oi.order_estimated_delivery_date as date), 'YYYYMMDD') as integer)
            else null
        end as estimated_delivery_date_key,

        oi.price,
        oi.freight_value,
        oi.total_item_value,

        1 as item_count

    from order_items oi
    left join dim_products dp
        on oi.product_id = dp.product_id
    left join dim_sellers ds
        on oi.seller_id = ds.seller_id
    left join dim_order_status dos
        on oi.order_status = dos.order_status

)

select *
from final