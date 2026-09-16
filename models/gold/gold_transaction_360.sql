{{ config(
    materialized='table',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['customer_id', 'merchant_id']
) }}

with transactions as (
    select * from {{ ref('silver_transactions') }}
),

cards as (
    select
        card_id,
        customer_id,
        card_type_id,
        credit_limit,
        card_status,
        cardholder_name
    from {{ ref('silver_credit_cards') }}
),

merchants as (
    select
        merchant_id,
        merchant_name,
        merchant_city,
        merchant_country,
        mcc_code,
        is_online     as merchant_is_online
    from {{ ref('silver_merchants') }}
),

txn_types as (
    select
        txn_type_id,
        type_name   as txn_type_name,
        is_credit
    from {{ ref('silver_dim_transaction_types') }}
),

mcc_lookup as (
    select
        mcc_code,
        category_name,
        category_group
    from {{ ref('silver_dim_merchant_categories') }}
),

fraud_flag as (
    select
        transaction_id,
        true                    as has_fraud_alert,
        max(risk_score)         as max_risk_score,
        countif(is_genuine_fraud) as confirmed_fraud_count
    from {{ ref('silver_fraud_alerts') }}
    group by transaction_id
),

dispute_flag as (
    select
        transaction_id,
        true                    as has_dispute,
        max(outcome)            as dispute_outcome,
        max(disputed_amount)    as disputed_amount
    from {{ ref('silver_disputes') }}
    group by transaction_id
),

velocity_flag as (
    select
        transaction_id,
        count(hit_id)           as velocity_hit_count,
        max(action_taken)       as velocity_action
    from {{ ref('silver_velocity_hits') }}
    group by transaction_id
)

select
    -- Transaction keys
    t.transaction_id,
    t.card_id,
    c.customer_id,
    t.merchant_id,
    t.txn_type_id,

    -- Transaction facts
    t.transaction_date,
    t.posting_date,
    t.amount,
    t.currency,
    t.original_amount,
    t.original_currency,
    t.fx_rate,
    t.description,
    t.status               as transaction_status,
    t.is_online,
    t.is_international,
    t.mcc_code             as transaction_mcc_code,
    t.auth_code,
    t.is_flagged,

    -- Card context
    c.cardholder_name,
    c.credit_limit,
    c.card_status,

    -- Transaction type
    tt.txn_type_name,
    tt.is_credit,

    -- Merchant enrichment
    m.merchant_name,
    m.merchant_city,
    m.merchant_country,
    m.merchant_is_online,

    -- MCC enrichment (from transaction's mcc_code)
    mc.category_name,
    mc.category_group,

    -- Fraud context
    coalesce(fa.has_fraud_alert, false)         as has_fraud_alert,
    coalesce(fa.max_risk_score, 0.0)            as max_fraud_risk_score,
    coalesce(fa.confirmed_fraud_count, 0) > 0   as is_confirmed_fraud,

    -- Dispute context
    coalesce(d.has_dispute, false)              as has_dispute,
    d.dispute_outcome,
    coalesce(d.disputed_amount, 0.0)            as disputed_amount,

    -- Velocity context
    coalesce(v.velocity_hit_count, 0)           as velocity_hit_count,
    v.velocity_action,

    -- Gold audit
    'transaction_360'       as gold_entity_name,
    current_timestamp()     as gold_created_ts,
    to_hex(sha256(to_json_string(struct(t.transaction_id, t.card_id, t.merchant_id, t.amount)))) as gold_record_hash
from transactions t
join cards c                        using (card_id)
left join merchants m               using (merchant_id)
left join txn_types tt              using (txn_type_id)
left join mcc_lookup mc             on t.mcc_code = mc.mcc_code
left join fraud_flag fa             using (transaction_id)
left join dispute_flag d            using (transaction_id)
left join velocity_flag v           using (transaction_id)
