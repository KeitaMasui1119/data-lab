-- models/staging/stg_products.sql
-- 製品のステージングモデル

select
    sku as product_sku,
    name as product_name,
    type as product_type,
    price / 100.0 as price_dollars,
    description
from {{ source('jaffle_shop_raw', 'products') }}
