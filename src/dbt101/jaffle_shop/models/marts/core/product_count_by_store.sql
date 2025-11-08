{{
    config(
        materialized='table'
    )
}}

select
    store_id,
    {% for product_type in get_product_types() %}
    count(distinct case when p.type = '{{ product_type }}' then i.sku end) as {{ product_type }}_product_count
    {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_orders') }} o
join {{ ref('stg_items') }} i on o.order_id = i.order_id
join {{ ref('stg_products') }} p on i.product_sku = p.product_sku
group by store_id

/* コンパイル後
select
    store_id,

    count(distinct case when p.type = 'jaffle' then i.sku end) as jaffle_product_count
    ,

    count(distinct case when p.type = 'beverage' then i.sku end) as beverage_product_count


from "dev"."main"."stg_orders" o
join "dev"."main"."stg_items" i on o.order_id = i.order_id
join "dev"."main"."stg_products" p on i.product_sku = p.product_sku
group by store_id
*/
