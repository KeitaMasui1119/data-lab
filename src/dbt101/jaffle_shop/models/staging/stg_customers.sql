-- models/staging/stg_customers.sql
-- 顧客データのステージングモデル

SELECT
    id as customer_id,
    name as customer_name
FROM
    {{ ref('raw_customers') }}
