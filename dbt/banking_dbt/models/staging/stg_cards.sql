{{ config(materialized='view') }}

select
    card_id::varchar as card_id,
    customer_id::varchar as customer_id,
    card_type::varchar as card_type,
    card_network::varchar as card_network,
    credit_limit::number(18, 2) as credit_limit,
    card_status::varchar as card_status,
    contactless::varchar as contactless,
    card_mode::varchar as card_mode,
    issue_date::date as issue_date,
    expiry_date::date as expiry_date
from {{ source('raw_banking', 'RAW_CARDS') }}
