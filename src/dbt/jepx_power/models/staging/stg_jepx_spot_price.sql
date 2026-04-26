{{ config(
    tags=['staging', 'jepx'],
    pre_hook=["SET unsafe_enable_version_guessing = true"]
) }}

with bronze_raw as (
    select *
    from iceberg_scan('s3://jp-power-grid-dev/bronze/jepx_spot_price')
),
typed as (
    select
        coalesce(
            try_strptime(delivery_date, '%Y-%m-%d'),
            try_strptime(delivery_date, '%Y/%m/%d')
        ) as delivery_date_d,
        try_cast(time_code as integer) as time_code_i,
        try_cast(replace(selling_bid_volume, ',', '') as bigint) as selling_bid_volume,
        try_cast(replace(purchase_bid_volume, ',', '') as bigint) as purchase_bid_volume,
        try_cast(replace(contracted_volume, ',', '') as bigint) as contracted_volume,
        try_cast(replace(system_price, ',', '') as bigint) as system_price,
        try_cast(replace(area_price_hokkaido, ',', '') as bigint) as area_price_hokkaido,
        try_cast(replace(area_price_tohoku, ',', '') as bigint) as area_price_tohoku,
        try_cast(replace(area_price_tokyo, ',', '') as bigint) as area_price_tokyo,
        try_cast(replace(area_price_chubu, ',', '') as bigint) as area_price_chubu,
        try_cast(replace(area_price_hokuriku, ',', '') as bigint) as area_price_hokuriku,
        try_cast(replace(area_price_kansai, ',', '') as bigint) as area_price_kansai,
        try_cast(replace(area_price_chugoku, ',', '') as bigint) as area_price_chugoku,
        try_cast(replace(area_price_shikoku, ',', '') as bigint) as area_price_shikoku,
        try_cast(replace(area_price_kyushu, ',', '') as bigint) as area_price_kyushu,
        try_cast(replace(block_selling_bid_volume, ',', '') as bigint) as block_selling_bid_volume,
        try_cast(replace(block_selling_contracted_volume, ',', '') as bigint) as block_selling_contracted_volume,
        try_cast(replace(block_purchase_bid_volume, ',', '') as bigint) as block_purchase_bid_volume,
        try_cast(replace(block_purchase_contracted_volume, ',', '') as bigint) as block_purchase_contracted_volume,
        source_data,
        status,
        ingestion_time,
        ingestion_date,
        execution_id
    from bronze_raw
),
normalized as (
    select
        cast(delivery_date_d as date) as delivery_date,
        time_code_i as time_code,
        cast(delivery_date_d + ((time_code_i - 1) * interval 30 minute) as timestamptz) as delivery_datetime,
        selling_bid_volume,
        purchase_bid_volume,
        contracted_volume,
        system_price,
        area_price_hokkaido,
        area_price_tohoku,
        area_price_tokyo,
        area_price_chubu,
        area_price_hokuriku,
        area_price_kansai,
        area_price_chugoku,
        area_price_shikoku,
        area_price_kyushu,
        block_selling_bid_volume,
        block_selling_contracted_volume,
        block_purchase_bid_volume,
        block_purchase_contracted_volume,
        source_data,
        status,
        ingestion_time,
        ingestion_date,
        execution_id
    from typed
    where delivery_date_d is not null
      and time_code_i between 1 and 48
),
deduplicated as (
    select *
    from normalized
    qualify row_number() over (
        partition by delivery_date, time_code
        order by ingestion_time desc nulls last, ingestion_date desc nulls last, execution_id desc nulls last
    ) = 1
)
select *
from deduplicated
