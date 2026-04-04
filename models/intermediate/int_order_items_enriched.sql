{{ config(materialized='view') }}

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

products as (

    select *
    from {{ ref('int_products_enriched') }}

),

sellers as (

    select *
    from {{ ref('stg_sellers') }}

),

final as (

    select
        oi.order_id,
        oi.order_item_id,

        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        oi.product_id,
        p.product_category_name,
        p.product_category_name_english,

        oi.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,

        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,
        (oi.price + oi.freight_value) as total_item_value,

        oi.ingestion_timestamp,
        oi.source_file_name

    from order_items oi
    left join orders o
        on oi.order_id = o.order_id
    left join products p
        on oi.product_id = p.product_id
    left join sellers s
        on oi.seller_id = s.seller_id

)

select *
from final