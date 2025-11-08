-- models/staging/stg_stores.sql
-- 店舗のステージングモデル

select
    id as store_id,
    name as store_name,
    opened_at,
    tax_rate
from {{ source('jaffle_shop_raw', 'stores') }}
