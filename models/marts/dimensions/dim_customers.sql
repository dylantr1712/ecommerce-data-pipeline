-- models/marts/dimensions/dim_customers.sql
{{ config(materialized='table') }}

with snapshot_source as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('customers_snapshot') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        dbt_valid_from as effective_date,
        dbt_valid_to as end_date,
        case
            when dbt_valid_to is null then true
            else false
        end as is_current
    from snapshot_source

)

select *
from final