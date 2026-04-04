{{ config(materialized='table') }}

with customer_versions as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        customer_attr_hash,
        record_effective_date
    from {{ ref('int_customers_deduped') }}

),

deduplicated_versions as (

    select distinct
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        customer_attr_hash,
        record_effective_date
    from customer_versions

),

versioned as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        customer_attr_hash,

        record_effective_date as effective_date,

        lead(record_effective_date) over (
            partition by customer_id
            order by record_effective_date
        ) as next_effective_date

    from deduplicated_versions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'effective_date']) }} as customer_key,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        effective_date,

        case
            when next_effective_date is not null
            then dateadd(day, -1, next_effective_date)
            else null
        end as end_date,

        case
            when next_effective_date is null then true
            else false
        end as is_current

    from versioned

)

select *
from final