{{ config(tags=['silver', 'jepx']) }}

select
    delivery_datetime,
    selling_bid_volume,
    purchase_bid_volume,
    contracted_volume,
    system_price
from {{ ref('stg_jepx_spot_price') }}
