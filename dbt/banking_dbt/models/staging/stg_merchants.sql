{{ config(materialized='view') }}

select
    merchant_id::varchar as merchant_id,
    merchant_name::varchar as merchant_name,
    merchant_category::varchar as merchant_category,
    state::varchar as state,
    city::varchar as city,
    merchant_risk_level::varchar as merchant_risk_level,
    merchant_rating::number(3, 1) as merchant_rating,
    merchant_status::varchar as merchant_status,
    merchant_since::date as merchant_since
from {{ source('raw_banking', 'RAW_MERCHANTS') }}
