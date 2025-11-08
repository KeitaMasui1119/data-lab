-- models/marts/core/fact_orders.sql
-- 注文ファクトテーブル

WITH orders AS (
    SELECT
        *
    FROM
        {{ ref('stg_orders') }}
),

-- paymentsテーブルは今回のデータには含まれていないため、
-- ordersテーブルの金額情報を使用
order_items_agg AS (
    SELECT
        order_id,
        COUNT(DISTINCT product_sku) AS product_count,
        COUNT(*) AS item_count
    FROM
        {{ ref('stg_items') }}
    GROUP BY
        order_id
),

final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.store_id,
        orders.subtotal_dollars,
        orders.tax_paid_dollars,
        orders.order_total_dollars,
        COALESCE(order_items_agg.product_count, 0) AS product_count,
        COALESCE(order_items_agg.item_count, 0) AS item_count
    FROM
        orders
    LEFT JOIN
        order_items_agg
    USING
        (order_id)
)

SELECT
    *
FROM
    final
