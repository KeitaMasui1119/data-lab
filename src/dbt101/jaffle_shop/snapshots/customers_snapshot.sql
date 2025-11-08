{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['name'],
    )
}}

select
    *
from {{ source('jaffle_shop_raw', 'customers') }}

{% endsnapshot %}
