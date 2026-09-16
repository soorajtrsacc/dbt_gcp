{{ config(materialized='table') }}

with merchants as (
    select * from {{ ref('silver_merchants') }}
),

mcc_lookup as (
    select
        mcc_code,
        category_name,
        category_group
    from {{ ref('silver_dim_merchant_categories') }}
),

txn_agg as (
    select
        merchant_id,
        count(transaction_id)                   as total_transactions,
        count(distinct card_id)                 as unique_cards,
        round(sum(amount), 2)                   as total_volume,
        round(avg(amount), 2)                   as avg_txn_amount,
        round(min(amount), 2)                   as min_txn_amount,
        round(max(amount), 2)                   as max_txn_amount,
        countif(is_international)               as international_txn_count,
        countif(is_online)                      as online_txn_count,
        countif(status = 'Posted')              as posted_txn_count,
        countif(status = 'Declined')            as declined_txn_count,
        countif(is_flagged)                     as flagged_txn_count,
        min(transaction_date)                   as first_transaction_date,
        max(transaction_date)                   as last_transaction_date
    from {{ ref('silver_transactions') }}
    group by merchant_id
),

dispute_agg as (
    select
        t.merchant_id,
        count(distinct d.dispute_id)            as dispute_count,
        round(sum(d.disputed_amount), 2)        as total_disputed_amount,
        round(sum(d.refund_amount), 2)          as total_refunded_amount,
        countif(d.outcome = 'Upheld')           as disputes_upheld,
        countif(d.outcome = 'Rejected')         as disputes_rejected
    from {{ ref('silver_disputes') }} d
    join {{ ref('silver_transactions') }} t using (transaction_id)
    group by t.merchant_id
),

chargeback_agg as (
    select
        t.merchant_id,
        count(distinct cb.chargeback_id)        as chargeback_count,
        round(sum(cb.chargeback_amount), 2)     as total_chargeback_amount,
        round(sum(cb.recovery_amount), 2)       as total_recovered_amount
    from {{ ref('silver_chargeback_cases') }} cb
    join {{ ref('silver_disputes') }} d         using (dispute_id)
    join {{ ref('silver_transactions') }} t     using (transaction_id)
    group by t.merchant_id
),

blacklist as (
    select
        merchant_id,
        true            as is_blacklisted,
        blacklist_reason,
        is_active       as blacklist_is_active
    from {{ ref('silver_merchant_blacklist') }}
    qualify row_number() over (partition by merchant_id order by added_date desc) = 1
)

select
    m.merchant_id,
    m.merchant_name,
    m.merchant_city,
    m.merchant_country,
    m.merchant_postcode,
    m.mcc_code,
    mc.category_name,
    mc.category_group,
    m.acquirer_bank,
    m.is_online,
    m.registered_date,

    -- Transaction performance
    coalesce(ta.total_transactions, 0)          as total_transactions,
    coalesce(ta.unique_cards, 0)                as unique_cards,
    coalesce(ta.total_volume, 0)                as total_volume,
    coalesce(ta.avg_txn_amount, 0)              as avg_txn_amount,
    coalesce(ta.min_txn_amount, 0)              as min_txn_amount,
    coalesce(ta.max_txn_amount, 0)              as max_txn_amount,
    coalesce(ta.international_txn_count, 0)     as international_txn_count,
    coalesce(ta.online_txn_count, 0)            as online_txn_count,
    coalesce(ta.posted_txn_count, 0)            as posted_txn_count,
    coalesce(ta.declined_txn_count, 0)          as declined_txn_count,
    coalesce(ta.flagged_txn_count, 0)           as flagged_txn_count,
    ta.first_transaction_date,
    ta.last_transaction_date,
    round(
        safe_divide(coalesce(ta.declined_txn_count, 0), nullif(coalesce(ta.total_transactions, 0), 0)), 4
    )                                           as decline_rate,

    -- Dispute performance
    coalesce(da.dispute_count, 0)               as dispute_count,
    coalesce(da.total_disputed_amount, 0)       as total_disputed_amount,
    coalesce(da.total_refunded_amount, 0)       as total_refunded_amount,
    round(
        safe_divide(coalesce(da.dispute_count, 0), nullif(coalesce(ta.total_transactions, 0), 0)), 4
    )                                           as dispute_rate,

    -- Chargeback performance
    coalesce(cb.chargeback_count, 0)            as chargeback_count,
    coalesce(cb.total_chargeback_amount, 0)     as total_chargeback_amount,
    coalesce(cb.total_recovered_amount, 0)      as total_recovered_amount,
    round(
        safe_divide(coalesce(cb.total_recovered_amount, 0), nullif(coalesce(cb.total_chargeback_amount, 0), 0)), 4
    )                                           as chargeback_recovery_rate,

    -- Blacklist status
    coalesce(bl.is_blacklisted, false)          as is_blacklisted,
    bl.blacklist_reason,
    coalesce(bl.blacklist_is_active, false)     as blacklist_is_active,

    -- Risk tier
    case
        when coalesce(bl.blacklist_is_active, false) then 'BLACKLISTED'
        when round(safe_divide(coalesce(da.dispute_count, 0), nullif(coalesce(ta.total_transactions, 0), 0)), 4) > 0.05 then 'HIGH_RISK'
        when round(safe_divide(coalesce(da.dispute_count, 0), nullif(coalesce(ta.total_transactions, 0), 0)), 4) > 0.02 then 'MEDIUM_RISK'
        else 'LOW_RISK'
    end                                         as merchant_risk_tier,

    -- Gold audit
    'merchant_performance'  as gold_entity_name,
    current_timestamp()     as gold_created_ts,
    to_hex(sha256(to_json_string(struct(m.merchant_id, m.merchant_name, m.mcc_code)))) as gold_record_hash
from merchants m
left join mcc_lookup mc         using (mcc_code)
left join txn_agg ta            using (merchant_id)
left join dispute_agg da        using (merchant_id)
left join chargeback_agg cb     using (merchant_id)
left join blacklist bl          using (merchant_id)
