-- models/staging/stg_supplies.sql
-- サプライのステージングモデル

select
    id as supply_id,
    name as supply_name,
    cost / 100.0 as cost_dollars,
    perishable as is_perishable,
    sku as product_sku
from {{ source('jaffle_shop_raw', 'supplies') }}
