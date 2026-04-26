{{ config(tags=['silver', 'jepx']) }}

select
    delivery_datetime,
    block_selling_bid_volume,
    block_selling_contracted_volume,
    block_purchase_bid_volume,
    block_purchase_contracted_volume
from {{ ref('stg_jepx_spot_price') }}
