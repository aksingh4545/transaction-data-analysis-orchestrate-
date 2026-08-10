{{ config(materialized='view') }}

select
    transaction_id::varchar as transaction_id,
    customer_id::varchar as customer_id,
    card_id::varchar as card_id,
    merchant_id::varchar as merchant_id,
    transaction_date::date as transaction_date,
    transaction_time::time as transaction_time,
    transaction_amount::number(18, 2) as transaction_amount,
    payment_method::varchar as payment_method,
    transaction_channel::varchar as transaction_channel,
    device_type::varchar as device_type,
    transaction_status::varchar as transaction_status,
    is_international::integer as is_international,
    fraud_flag::integer as fraud_flag,
    fraud_reason::varchar as fraud_reason,
    merchant_risk_level::varchar as merchant_risk_level,
    merchant_category::varchar as merchant_category,
    customer_state::varchar as customer_state,
    customer_city::varchar as customer_city,
    merchant_state::varchar as merchant_state,
    merchant_city::varchar as merchant_city
from {{ source('raw_banking', 'RAW_TRANSACTIONS') }}
