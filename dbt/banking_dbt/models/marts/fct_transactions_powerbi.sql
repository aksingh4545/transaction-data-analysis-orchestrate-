"""
Defined in dbt_project
    marts:
      +materialized: table
      +schema: CURATED

"""


{{ config(materialized='table') }}

with transactions as (
    select *
    from {{ ref('stg_transactions') }}
),
customers as (
    select *
    from {{ ref('stg_customers') }}
),
cards as (
    select *
    from {{ ref('stg_cards') }}
),
merchants as (
    select *
    from {{ ref('stg_merchants') }}
)

select
    t.transaction_id,
    t.transaction_date,
    t.transaction_time,
    t.transaction_amount,
    t.payment_method,
    t.transaction_channel,
    t.device_type,
    t.transaction_status,
    t.is_international,
    t.fraud_flag,
    t.fraud_reason,
    t.customer_id,
    c.customer_name,
    c.gender,
    c.age,
    c.marital_status,
    c.occupation,
    c.annual_income,
    c.customer_segment,
    c.state as customer_state,
    c.city as customer_city,
    c.account_type,
    c.customer_since,
    t.card_id,
    ca.card_type,
    ca.card_network,
    ca.credit_limit,
    ca.card_status,
    ca.contactless,
    ca.card_mode,
    ca.issue_date,
    ca.expiry_date,
    t.merchant_id,
    m.merchant_name,
    t.merchant_category,
    t.merchant_risk_level,
    m.merchant_rating,
    m.merchant_status,
    m.merchant_since,
    m.state as merchant_state,
    m.city as merchant_city
from transactions as t
left join customers as c
    on t.customer_id = c.customer_id
left join cards as ca
    on t.card_id = ca.card_id
left join merchants as m
    on t.merchant_id = m.merchant_id
