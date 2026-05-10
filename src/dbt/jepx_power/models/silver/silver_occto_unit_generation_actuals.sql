{{ config(tags=['silver', 'occto']) }}

select *
from {{ ref('stg_occto_unit_generation_actuals') }}
