-- すべての注文に少なくとも1つのアイテムが含まれることを確認するテスト
with orders as (
    select
        order_id
    from
        {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        count(*) as item_count
    from
        {{ ref('stg_items') }}
    group by
        order_id
),

-- アイテムのない注文を検出
orders_without_items as (
    select
        o.order_id
    from
        orders o
    left join
        order_items oi
    using
        (order_id)
    where
        oi.order_id is null
)

select * from orders_without_items
