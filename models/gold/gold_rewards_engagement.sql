{{ config(materialized='table') }}

with rewards as (
    select * from {{ ref('silver_rewards') }}
),

cards as (
    select
        card_id,
        customer_id,
        card_type_id,
        credit_limit,
        card_status,
        issued_date
    from {{ ref('silver_credit_cards') }}
),

card_types as (
    select
        card_type_id,
        card_name,
        network,
        tier,
        reward_rate
    from {{ ref('silver_dim_card_types') }}
),

customers as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        customer_status,
        annual_income,
        credit_score
    from {{ ref('silver_customers') }}
),

redemption_agg as (
    select
        reward_id,
        count(redemption_id)                        as redemption_count,
        round(sum(points_used), 0)                  as total_points_redeemed,
        round(sum(cash_equivalent), 2)              as total_cash_redeemed,
        max(redeemed_date)                          as last_redemption_date,
        countif(status = 'Processed')               as successful_redemptions,
        countif(status = 'Failed')                  as failed_redemptions,
        string_agg(distinct redemption_type order by redemption_type) as redemption_types_used
    from {{ ref('silver_reward_redemptions') }}
    group by reward_id
),

spend_agg as (
    select
        cc.card_id,
        round(sum(if(t.status = 'Posted', t.amount, 0)), 2)         as total_spend,
        count(if(t.status = 'Posted', t.transaction_id, null))       as posted_txn_count,
        max(t.transaction_date)                                       as last_transaction_date,
        min(t.transaction_date)                                       as first_transaction_date
    from {{ ref('silver_transactions') }} t
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    group by cc.card_id
)

select
    r.reward_id,
    r.card_id,
    r.customer_id,
    r.program_type,
    r.tier          as reward_tier,
    r.enrolled_date,
    r.expiry_date,
    date_diff(r.expiry_date, current_date(), day) as days_to_expiry,
    r.points_balance,
    r.points_earned_ytd,
    r.points_redeemed_ytd,
    r.points_expiring,

    -- Card context
    ct.card_name,
    ct.network,
    ct.tier         as card_tier,
    ct.reward_rate  as card_reward_rate,
    c.card_status,
    c.credit_limit,
    c.issued_date   as card_issued_date,

    -- Customer context
    cu.first_name,
    cu.last_name,
    cu.email,
    cu.customer_status,
    cu.annual_income,
    cu.credit_score,

    -- Redemption summary
    coalesce(rd.redemption_count, 0)            as redemption_count,
    coalesce(rd.total_points_redeemed, 0)       as total_points_redeemed,
    coalesce(rd.total_cash_redeemed, 0)         as total_cash_redeemed,
    rd.last_redemption_date,
    coalesce(rd.successful_redemptions, 0)      as successful_redemptions,
    coalesce(rd.failed_redemptions, 0)          as failed_redemptions,
    rd.redemption_types_used,

    -- Spend context
    coalesce(s.total_spend, 0)                  as total_spend_on_card,
    coalesce(s.posted_txn_count, 0)             as posted_txn_count,
    s.first_transaction_date,
    s.last_transaction_date,

    -- Calculated metrics
    round(
        safe_divide(coalesce(rd.total_cash_redeemed, 0), nullif(coalesce(s.total_spend, 0), 0)) * 100, 4
    )                                           as effective_reward_cost_pct,
    round(
        safe_divide(coalesce(r.points_redeemed_ytd, 0), nullif(coalesce(r.points_earned_ytd, 0), 0)), 4
    )                                           as redemption_ratio_ytd,

    -- Engagement tier
    case
        when coalesce(rd.redemption_count, 0) >= 4 and coalesce(s.total_spend, 0) > 10000 then 'HIGHLY_ENGAGED'
        when coalesce(rd.redemption_count, 0) >= 2 and coalesce(s.total_spend, 0) > 5000  then 'ENGAGED'
        when coalesce(rd.redemption_count, 0) >= 1                                          then 'ACTIVE'
        when coalesce(s.total_spend, 0) > 0                                                 then 'SPENDING_NOT_REDEEMING'
        else 'INACTIVE'
    end                                         as engagement_tier,

    -- Gold audit
    'rewards_engagement'    as gold_entity_name,
    current_timestamp()     as gold_created_ts,
    to_hex(sha256(to_json_string(struct(r.reward_id, r.card_id, r.customer_id, r.program_type)))) as gold_record_hash
from rewards r
left join cards c                   using (card_id)
left join card_types ct             using (card_type_id)
left join customers cu              using (customer_id)
left join redemption_agg rd         using (reward_id)
left join spend_agg s               using (card_id)
