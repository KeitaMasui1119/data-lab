-- models/marts/core/dim_customers.sql
-- 顧客ディメンションテーブル

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('fact_orders') }}
),

customer_orders as (
    select
        customer_id,
        count(*) as number_of_orders,
        sum(order_total_dollars) as lifetime_value
    from orders
    group by customer_id
),

final as (
    select
        customers.customer_id,
        customers.customer_name,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value
    from customers
    left join customer_orders using (customer_id)
)

select * from final
