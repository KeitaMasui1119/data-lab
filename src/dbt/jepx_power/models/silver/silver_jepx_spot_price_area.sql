{{ config(tags=['silver', 'jepx']) }}

with area_unpivoted as (
    select
        delivery_datetime,
        area_price_column,
        area_price
    from {{ ref('stg_jepx_spot_price') }}
    unpivot (
        area_price for area_price_column in (
            area_price_hokkaido,
            area_price_tohoku,
            area_price_tokyo,
            area_price_chubu,
            area_price_hokuriku,
            area_price_kansai,
            area_price_chugoku,
            area_price_shikoku,
            area_price_kyushu
        )
    )
)
select
    delivery_datetime,
    split_part(area_price_column, '_', 3) as area_name,
    area_price
from area_unpivoted
