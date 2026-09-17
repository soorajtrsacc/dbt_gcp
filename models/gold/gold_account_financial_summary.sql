{{ config(materialized='table') }}

with accounts as (
    select * from {{ ref('silver_accounts') }}
),

cards_agg as (
    select
        account_id,
        count(distinct card_id)             as card_count,
        sum(credit_limit)                   as total_credit_limit,
        sum(available_credit)               as total_available_credit,
        countif(card_status = 'Active')     as active_card_count,
        round(
            1 - safe_divide(sum(available_credit), nullif(sum(credit_limit), 0)), 4
        )                                   as utilisation_rate
    from {{ ref('silver_credit_cards') }}
    group by account_id
),

stmt_agg as (
    select
        cc.account_id,
        count(distinct s.statement_id)              as statement_count,
        round(sum(s.total_purchases), 2)            as total_purchases,
        round(sum(s.total_payments), 2)             as total_payments,
        round(sum(s.total_fees), 2)                 as total_fees,
        round(sum(s.total_interest), 2)             as total_interest,
        round(sum(s.closing_balance), 2)            as total_outstanding_balance,
        countif(s.payment_status = 'Missed')        as missed_payment_count,
        countif(s.payment_status = 'Paid')          as on_time_payment_count,
        countif(s.payment_status = 'Partial')       as partial_payment_count,
        max(s.statement_date)                       as latest_statement_date
    from {{ ref('silver_statements') }} s
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    group by cc.account_id
),

fees_agg as (
    select
        cc.account_id,
        count(distinct f.fee_id)        as fee_event_count,
        round(sum(f.fee_amount), 2)     as total_fees_charged,
        round(sum(if(f.waived, f.fee_amount, 0)), 2) as total_fees_waived,
        round(sum(if(not f.waived, f.fee_amount, 0)), 2) as total_fees_paid
    from {{ ref('silver_fees') }} f
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    group by cc.account_id
),

current_rate as (
    select
        cc.account_id,
        round(avg(if(ir.rate_type = 'Purchase' and ir.is_current, ir.apr, null)), 2) as purchase_apr,
        round(avg(if(ir.rate_type = 'Cash Advance' and ir.is_current, ir.apr, null)), 2) as cash_advance_apr
    from {{ ref('silver_interest_rates') }} ir
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    group by cc.account_id
),

current_limit as (
    select
        cc.account_id,
        sum(cl.new_limit)                   as current_total_limit
    from {{ ref('silver_credit_limit_history') }} cl
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    where cl.is_current = true
    group by cc.account_id
),

emi_agg as (
    select
        cc.account_id,
        count(distinct e.emi_plan_id)           as emi_plan_count,
        round(sum(e.original_amount), 2)        as total_emi_original_amount,
        round(sum(e.total_payable), 2)          as total_emi_payable,
        countif(e.status = 'Active')            as active_emi_plans
    from {{ ref('silver_emi_plans') }} e
    join {{ ref('silver_credit_cards') }} cc using (card_id)
    group by cc.account_id
),

payments_agg as (
    select
        cc.account_id,
        count(distinct p.payment_id)                            as payment_event_count,
        round(sum(p.amount), 2)                                 as total_amount_paid,
        round(min(p.amount), 2)                                 as min_payment_amount,
        round(max(p.amount), 2)                                 as max_payment_amount,
        round(avg(p.amount), 2)                                 as avg_payment_amount,
        max(p.payment_date)                                     as last_payment_date,
        countif(p.is_minimum_only)                              as minimum_only_payment_count,
        countif(p.status = 'Completed')                         as completed_payment_count,
        countif(p.status = 'Failed')                            as failed_payment_count
    from {{ ref('silver_payments') }} p
    join {{ ref('silver_credit_cards') }} cc on cc.card_id = p.card_id
    group by cc.account_id
)

select
    a.account_id,
    a.customer_id,
    a.account_type,
    a.account_number,
    a.sort_code,
    a.currency,
    a.balance                                           as current_balance,
    a.overdraft_limit,
    a.account_status,
    a.opened_date,
    a.closed_date,

    -- Card summary
    coalesce(ca.card_count, 0)                          as card_count,
    coalesce(ca.total_credit_limit, 0)                  as total_credit_limit,
    coalesce(ca.total_available_credit, 0)              as total_available_credit,
    coalesce(ca.active_card_count, 0)                   as active_card_count,
    coalesce(ca.utilisation_rate, 0)                    as utilisation_rate,

    -- Statement summary
    coalesce(sa.statement_count, 0)                     as statement_count,
    coalesce(sa.total_purchases, 0)                     as total_purchases,
    coalesce(sa.total_payments, 0)                      as total_payments,
    coalesce(sa.total_fees, 0)                          as total_statement_fees,
    coalesce(sa.total_interest, 0)                      as total_interest_charged,
    coalesce(sa.total_outstanding_balance, 0)           as total_outstanding_balance,
    coalesce(sa.missed_payment_count, 0)                as missed_payment_count,
    coalesce(sa.on_time_payment_count, 0)               as on_time_payment_count,
    coalesce(sa.partial_payment_count, 0)               as partial_payment_count,
    sa.latest_statement_date,
    round(
        safe_divide(coalesce(sa.missed_payment_count, 0),
            nullif(coalesce(sa.statement_count, 0), 0)), 4
    )                                                   as missed_payment_rate,

    -- Fee summary
    coalesce(fa.fee_event_count, 0)                     as fee_event_count,
    coalesce(fa.total_fees_charged, 0)                  as total_fees_charged,
    coalesce(fa.total_fees_waived, 0)                   as total_fees_waived,
    coalesce(fa.total_fees_paid, 0)                     as total_fees_paid,

    -- Rate
    cr.purchase_apr,
    cr.cash_advance_apr,

    -- Current credit limit
    coalesce(cl.current_total_limit, 0)                 as current_total_credit_limit,

    -- EMI
    coalesce(em.emi_plan_count, 0)                      as emi_plan_count,
    coalesce(em.total_emi_original_amount, 0)           as total_emi_original_amount,
    coalesce(em.total_emi_payable, 0)                   as total_emi_payable,
    coalesce(em.active_emi_plans, 0)                    as active_emi_plans,

    -- Payment events (granular payment records)
    coalesce(pa.payment_event_count, 0)                 as payment_event_count,
    coalesce(pa.total_amount_paid, 0)                   as total_amount_paid,
    coalesce(pa.min_payment_amount, 0)                  as min_payment_amount,
    coalesce(pa.max_payment_amount, 0)                  as max_payment_amount,
    coalesce(pa.avg_payment_amount, 0)                  as avg_payment_amount,
    pa.last_payment_date,
    coalesce(pa.minimum_only_payment_count, 0)          as minimum_only_payment_count,
    coalesce(pa.completed_payment_count, 0)             as completed_payment_count,
    coalesce(pa.failed_payment_count, 0)                as failed_payment_count,

    -- Delinquency bucket
    case
        when coalesce(sa.missed_payment_count, 0) = 0              then 'Current'
        when coalesce(sa.missed_payment_count, 0) between 1 and 2  then '30-60 DPD'
        when coalesce(sa.missed_payment_count, 0) between 3 and 5  then '60-90 DPD'
        else '90+ DPD'
    end                                                 as delinquency_bucket,

    -- Gold audit
    'account_financial_summary' as gold_entity_name,
    current_timestamp()         as gold_created_ts,
    to_hex(sha256(to_json_string(struct(a.account_id, a.customer_id, a.account_number)))) as gold_record_hash
from accounts a
left join cards_agg ca      using (account_id)
left join stmt_agg sa       using (account_id)
left join fees_agg fa       using (account_id)
left join current_rate cr   using (account_id)
left join current_limit cl  using (account_id)
left join emi_agg em        using (account_id)
left join payments_agg pa   using (account_id)
