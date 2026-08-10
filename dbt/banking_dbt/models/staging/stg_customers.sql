{{ config(materialized='view') }}

select
    customer_id::varchar as customer_id,
    customer_name::varchar as customer_name,
    gender::varchar as gender,
    age::integer as age,
    marital_status::varchar as marital_status,
    occupation::varchar as occupation,
    annual_income::number(18, 2) as annual_income,
    customer_segment::varchar as customer_segment,
    state::varchar as state,
    city::varchar as city,
    account_type::varchar as account_type,
    customer_since::date as customer_since
from {{ source('raw_banking', 'RAW_CUSTOMERS') }}
