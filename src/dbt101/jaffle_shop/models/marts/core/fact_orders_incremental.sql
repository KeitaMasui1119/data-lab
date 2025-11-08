-- models/marts/core/fact_orders_incremental.sql
-- インクリメンタルモデルの例

{{
    config(
        materialized="incremental",
        unique_key = "order_id",
        on_schema_change = "fail"
    )
}}

WITH orders AS (
    SELECT
        *
    FROM
        {{ ref('stg_orders') }}
    {% if is_incremental() %}
    -- 増分更新実行時は新しいデータのみを処理
    WHERE
        ordered_at > (
            SELECT
                MAX(ordered_at)
            FROM
                {{ this }}
        )
    {% endif %}
),

--paymentsテーブルは今回のデータには含まれないため、ordersテーブルの金額情報を利用

order_items_agg AS (
    SELECT
        order_id,
        COUNT(DISTINCT product_sku) AS product_count,
        COUNT(*) AS item_count
    FROM
        {{ ref("stg_items") }}
    GROUP BY
        order_id
),

final AS (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.store_id,
        orders.subtotal_dollars,
        orders.tax_paid_dollars,
        orders.order_total_dollars,
        coalesce(order_items_agg.product_count, 0) as product_count,
        coalesce(order_items_agg.item_count, 0) as item_count
    from
        orders
    left join
        order_items_agg
    using
        (order_id)
)

select * from final
