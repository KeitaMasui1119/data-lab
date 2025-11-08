-- models/staging/stg_orders.sql
-- 注文データのステージングモデル
SELECT
    id AS order_id,
    customer AS customer_id,
    ordered_at,
    store_id,
    subtotal / 100.0 AS subtotal_dollars,
    tax_paid / 100.0 AS tax_paid_dollars,
    order_total / 100.0 AS order_total_dollars
FROM
    {{ source('jaffle_shop_raw', 'orders') }}
