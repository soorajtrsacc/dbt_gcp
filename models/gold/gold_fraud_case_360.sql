{{ config(materialized='table') }}

with fraud_alerts as (
    select * from {{ ref('silver_fraud_alerts') }}
),

transactions as (
    select
        transaction_id,
        card_id,
        merchant_id,
        txn_type_id,
        transaction_date,
        posting_date,
        amount,
        currency,
        status          as transaction_status,
        is_online,
        is_international,
        mcc_code,
        is_flagged
    from {{ ref('silver_transactions') }}
),

cards as (
    select
        card_id,
        customer_id,
        card_type_id,
        credit_limit,
        card_status
    from {{ ref('silver_credit_cards') }}
),

customers as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        credit_score,
        customer_status
    from {{ ref('silver_customers') }}
),

merchants as (
    select
        merchant_id,
        merchant_name,
        merchant_city,
        merchant_country
    from {{ ref('silver_merchants') }}
),

dispute_flag as (
    select
        transaction_id,
        true                            as has_dispute,
        max(dispute_id)                 as dispute_id,
        max(dispute_reason)             as dispute_reason,
        max(outcome)                    as dispute_outcome,
        max(disputed_amount)            as disputed_amount,
        max(refund_amount)              as refund_amount,
        max(status)                     as dispute_status
    from {{ ref('silver_disputes') }}
    group by transaction_id
),

chargeback_flag as (
    select
        d.transaction_id,
        true                                as has_chargeback,
        max(cb.chargeback_id)               as chargeback_id,
        max(cb.chargeback_amount)           as chargeback_amount,
        max(cb.current_stage)               as chargeback_stage,
        max(cb.acquirer_response)           as acquirer_response,
        coalesce(max(cb.bank_liability), false) as bank_liability,
        max(cb.recovery_amount)             as recovery_amount
    from {{ ref('silver_chargeback_cases') }} cb
    join {{ ref('silver_disputes') }} d     using (dispute_id)
    group by d.transaction_id
),

velocity_agg as (
    select
        transaction_id,
        count(hit_id)                       as velocity_hit_count,
        string_agg(distinct action_taken order by action_taken) as velocity_actions
    from {{ ref('silver_velocity_hits') }}
    group by transaction_id
)

select
    -- Alert keys
    fa.alert_id,
    fa.transaction_id,
    fa.card_id,
    cc.customer_id,
    t.merchant_id,

    -- Alert attributes
    fa.alert_type,
    fa.status           as alert_status,
    fa.alert_timestamp,
    fa.risk_score,
    fa.is_genuine_fraud,
    fa.resolved_at,
    fa.analyst_notes,

    -- Transaction context
    t.transaction_date,
    t.posting_date,
    t.amount,
    t.currency,
    t.transaction_status,
    t.is_online,
    t.is_international,
    t.mcc_code,
    t.is_flagged,

    -- Card context
    cc.credit_limit     as card_credit_limit,
    cc.card_status,

    -- Customer context
    c.first_name,
    c.last_name,
    c.email,
    c.credit_score,
    c.customer_status,

    -- Merchant context
    m.merchant_name,
    m.merchant_city,
    m.merchant_country,

    -- Dispute context
    coalesce(d.has_dispute, false)          as has_dispute,
    d.dispute_id,
    d.dispute_reason,
    d.dispute_outcome,
    coalesce(d.disputed_amount, 0)          as disputed_amount,
    coalesce(d.refund_amount, 0)            as refund_amount,
    d.dispute_status,

    -- Chargeback context
    coalesce(cb.has_chargeback, false)      as has_chargeback,
    cb.chargeback_id,
    coalesce(cb.chargeback_amount, 0)       as chargeback_amount,
    cb.chargeback_stage,
    cb.acquirer_response,
    coalesce(cb.bank_liability, false)      as bank_liability,
    coalesce(cb.recovery_amount, 0)         as recovery_amount,

    -- Velocity context
    coalesce(v.velocity_hit_count, 0)       as velocity_hit_count,
    v.velocity_actions,

    -- Resolution flag
    case
        when fa.is_genuine_fraud and coalesce(cb.has_chargeback, false) then 'CHARGEBACK_RAISED'
        when fa.is_genuine_fraud and coalesce(d.has_dispute, false)     then 'DISPUTED'
        when fa.is_genuine_fraud                                         then 'FRAUD_CONFIRMED_NO_ACTION'
        when fa.status = 'False Positive'                                then 'FALSE_POSITIVE'
        when fa.status = 'Resolved'                                      then 'RESOLVED'
        else 'OPEN'
    end                                     as case_resolution_status,

    -- Gold audit
    'fraud_case_360'    as gold_entity_name,
    current_timestamp() as gold_created_ts,
    to_hex(sha256(to_json_string(struct(fa.alert_id, fa.card_id, fa.alert_type)))) as gold_record_hash
from fraud_alerts fa
left join transactions t    on t.transaction_id = fa.transaction_id
left join cards cc           on cc.card_id = fa.card_id
left join customers c        on c.customer_id = cc.customer_id
left join merchants m        on m.merchant_id = t.merchant_id
left join dispute_flag d     on d.transaction_id = fa.transaction_id
left join chargeback_flag cb on cb.transaction_id = fa.transaction_id
left join velocity_agg v     on v.transaction_id = fa.transaction_id
