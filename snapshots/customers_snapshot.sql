{% snapshot customers_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=[
      'customer_unique_id',
      'customer_zip_code_prefix',
      'customer_city',
      'customer_state'
    ],
    invalidate_hard_deletes=True
  )
}}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingestion_timestamp,
    source_file_name
from {{ ref('stg_customers') }}

{% endsnapshot %}