-- models/staging/stg_items.sql
-- 注文アイテムデータのステージングモデル

SELECT
    id AS item_id,
    order_id,
    sku AS product_sku
FROM
    {{ ref('raw_items') }}
