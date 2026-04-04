{{ config(materialized='table') }}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2015-01-01' as date)",
            end_date="cast('2022-01-01' as date)"
        )
    }}

),

final as (

    select
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
        cast(date_day as date) as full_date,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week,
        trim(to_char(date_day, 'Day')) as day_name,
        extract(week from date_day) as week_of_year,
        extract(month from date_day) as month_number,
        trim(to_char(date_day, 'Month')) as month_name,
        extract(quarter from date_day) as quarter_number,
        extract(year from date_day) as year_number,
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end as is_weekend
    from date_spine

)

select *
from final