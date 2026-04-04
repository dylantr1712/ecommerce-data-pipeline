{{ config(materialized='table') }}

with payments as (

    select *
    from {{ ref('stg_order_payments') }}

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

dim_payment_type as (

    select *
    from {{ ref('dim_payment_type') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['p.order_id', 'p.payment_sequential']) }} as order_payment_fact_key,

        p.order_id,
        p.payment_sequential,

        dc.customer_key,
        dos.order_status_key,
        dpt.payment_type_key,

        cast(to_char(cast(o.order_purchase_timestamp as date), 'YYYYMMDD') as integer) as purchase_date_key,

        case
            when o.order_approved_at is not null
            then cast(to_char(cast(o.order_approved_at as date), 'YYYYMMDD') as integer)
            else null
        end as approved_date_key,

        p.payment_installments,
        p.payment_value,

        1 as payment_count

    from payments p
    left join orders o
        on p.order_id = o.order_id
    left join dim_customers dc
        on o.customer_id = dc.customer_id
       and dc.is_current = true
    left join dim_order_status dos
        on o.order_status = dos.order_status
    left join dim_payment_type dpt
        on p.payment_type = dpt.payment_type

)

select *
from final