{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('silver_customers') }}
),

primary_address as (
    select
        customer_id,
        address_line1,
        address_line2,
        city,
        county,
        postcode,
        country
    from {{ ref('silver_addresses') }}
    where is_primary = true
    qualify row_number() over (partition by customer_id order by valid_from desc) = 1
),

accounts_agg as (
    select
        customer_id,
        count(distinct account_id)              as account_count,
        sum(safe_cast(balance as float64))      as total_balance,
        countif(account_status = 'Active')      as active_account_count
    from {{ ref('silver_accounts') }}
    group by customer_id
),

cards_agg as (
    select
        customer_id,
        count(distinct card_id)                         as card_count,
        sum(credit_limit)                               as total_credit_limit,
        sum(available_credit)                           as total_available_credit,
        countif(card_status = 'Active')                 as active_card_count,
        round(
            1 - safe_divide(sum(available_credit), nullif(sum(credit_limit), 0)), 4
        )                                               as overall_utilisation_rate
    from {{ ref('silver_credit_cards') }}
    group by customer_id
),

rewards_agg as (
    select
        customer_id,
        count(distinct reward_id)           as rewards_programme_count,
        sum(points_balance)                 as total_points_balance,
        sum(points_earned_ytd)              as total_points_earned_ytd,
        sum(points_redeemed_ytd)            as total_points_redeemed_ytd
    from {{ ref('silver_rewards') }}
    group by customer_id
),

kyc_status as (
    select
        customer_id,
        max(status)                         as kyc_latest_status,
        countif(status = 'Verified')        as kyc_verified_doc_count
    from {{ ref('silver_kyc_documents') }}
    group by customer_id
),

contact_agg as (
    select
        customer_id,
        count(contact_id)                               as total_contact_count,
        round(avg(satisfaction_score), 2)               as avg_satisfaction_score,
        max(contact_date)                               as last_contact_date
    from {{ ref('silver_contact_history') }}
    group by customer_id
),

missed_payments as (
    select
        cc.customer_id,
        countif(s.payment_status = 'Missed')   as missed_payment_count,
        countif(s.payment_status = 'Paid')     as on_time_payment_count
    from {{ ref('silver_statements') }} s
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    group by cc.customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    concat(c.first_name, ' ', c.last_name)              as full_name,
    c.email,
    c.phone_mobile,
    c.phone_home,
    c.date_of_birth,
    c.gender,
    c.nationality,
    c.occupation,
    c.annual_income,
    c.credit_score,
    c.customer_status,
    c.created_at                                        as customer_since,

    -- Address
    a.address_line1,
    a.address_line2,
    a.city,
    a.county,
    a.postcode,
    a.country,

    -- Account summary
    coalesce(ac.account_count, 0)                       as account_count,
    coalesce(ac.total_balance, 0)                       as total_account_balance,
    coalesce(ac.active_account_count, 0)                as active_account_count,

    -- Card summary
    coalesce(cr.card_count, 0)                          as card_count,
    coalesce(cr.total_credit_limit, 0)                  as total_credit_limit,
    coalesce(cr.total_available_credit, 0)              as total_available_credit,
    coalesce(cr.active_card_count, 0)                   as active_card_count,
    coalesce(cr.overall_utilisation_rate, 0)            as overall_utilisation_rate,

    -- Rewards
    coalesce(r.rewards_programme_count, 0)              as rewards_programme_count,
    coalesce(r.total_points_balance, 0)                 as total_points_balance,
    coalesce(r.total_points_earned_ytd, 0)              as total_points_earned_ytd,
    coalesce(r.total_points_redeemed_ytd, 0)            as total_points_redeemed_ytd,

    -- KYC
    k.kyc_latest_status,
    coalesce(k.kyc_verified_doc_count, 0)               as kyc_verified_doc_count,

    -- Contact
    coalesce(ct.total_contact_count, 0)                 as total_contact_count,
    ct.avg_satisfaction_score,
    ct.last_contact_date,

    -- Payment behaviour
    coalesce(mp.missed_payment_count, 0)                as missed_payment_count,
    coalesce(mp.on_time_payment_count, 0)               as on_time_payment_count,

    -- Gold audit
    'customer_360'          as gold_entity_name,
    current_timestamp()     as gold_created_ts,
    to_hex(sha256(to_json_string(struct(c.customer_id, c.first_name, c.last_name, c.email)))) as gold_record_hash
from customers c
left join primary_address a     using (customer_id)
left join accounts_agg ac       using (customer_id)
left join cards_agg cr          using (customer_id)
left join rewards_agg r         using (customer_id)
left join kyc_status k          using (customer_id)
left join contact_agg ct        using (customer_id)
left join missed_payments mp    using (customer_id)
