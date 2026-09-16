-- Queryable DQ report for all six Gold entities.
-- Runs PK uniqueness, required-NOT-NULL, and FK referential integrity checks.
-- Mirrors the pattern in schema.yml dbt tests but produces a BI-browsable table.
{{ config(materialized='table') }}

with
-- ── gold_customer_360 ────────────────────────────────────────────────────────
c360_pk as (
    {{ dq_check_pk('gold_customer_360', 'customer_id') }}
),
c360_nn_full_name as (
    {{ dq_check_not_null('gold_customer_360', 'full_name') }}
),
c360_nn_status as (
    {{ dq_check_not_null('gold_customer_360', 'customer_status') }}
),

-- ── gold_transaction_360 ─────────────────────────────────────────────────────
t360_pk as (
    {{ dq_check_pk('gold_transaction_360', 'transaction_id') }}
),
t360_nn_customer as (
    {{ dq_check_not_null('gold_transaction_360', 'customer_id') }}
),
t360_nn_amount as (
    {{ dq_check_not_null('gold_transaction_360', 'amount') }}
),
t360_fk_customer as (
    {{ dq_check_fk('gold_transaction_360', 'customer_id', 'gold_customer_360', 'customer_id') }}
),
t360_fk_merchant as (
    {{ dq_check_fk('gold_transaction_360', 'merchant_id', 'gold_merchant_performance', 'merchant_id') }}
),

-- ── gold_merchant_performance ────────────────────────────────────────────────
mp_pk as (
    {{ dq_check_pk('gold_merchant_performance', 'merchant_id') }}
),
mp_nn_name as (
    {{ dq_check_not_null('gold_merchant_performance', 'merchant_name') }}
),
mp_nn_mcc as (
    {{ dq_check_not_null('gold_merchant_performance', 'mcc_code') }}
),

-- ── gold_account_financial_summary ───────────────────────────────────────────
afs_pk as (
    {{ dq_check_pk('gold_account_financial_summary', 'account_id') }}
),
afs_nn_status as (
    {{ dq_check_not_null('gold_account_financial_summary', 'account_status') }}
),
afs_fk_customer as (
    {{ dq_check_fk('gold_account_financial_summary', 'customer_id', 'gold_customer_360', 'customer_id') }}
),

-- ── gold_fraud_case_360 ──────────────────────────────────────────────────────
fc_pk as (
    {{ dq_check_pk('gold_fraud_case_360', 'alert_id') }}
),
fc_nn_card as (
    {{ dq_check_not_null('gold_fraud_case_360', 'card_id') }}
),
fc_nn_type as (
    {{ dq_check_not_null('gold_fraud_case_360', 'alert_type') }}
),

-- ── gold_rewards_engagement ──────────────────────────────────────────────────
re_pk as (
    {{ dq_check_pk('gold_rewards_engagement', 'reward_id') }}
),
re_nn_program as (
    {{ dq_check_not_null('gold_rewards_engagement', 'program_type') }}
),
re_fk_customer as (
    {{ dq_check_fk('gold_rewards_engagement', 'customer_id', 'gold_customer_360', 'customer_id') }}
),

all_checks as (
    select * from c360_pk
    union all select * from c360_nn_full_name
    union all select * from c360_nn_status
    union all select * from t360_pk
    union all select * from t360_nn_customer
    union all select * from t360_nn_amount
    union all select * from t360_fk_customer
    union all select * from t360_fk_merchant
    union all select * from mp_pk
    union all select * from mp_nn_name
    union all select * from mp_nn_mcc
    union all select * from afs_pk
    union all select * from afs_nn_status
    union all select * from afs_fk_customer
    union all select * from fc_pk
    union all select * from fc_nn_card
    union all select * from fc_nn_type
    union all select * from re_pk
    union all select * from re_nn_program
    union all select * from re_fk_customer
)

select
    check_id,
    table_name,
    check_type,
    checked_columns,
    total_rows,
    passed_rows,
    failed_rows,
    pass_percentage,
    check_status,
    severity,
    current_timestamp() as report_generated_at
from all_checks
order by
    case severity when 'CRITICAL' then 1 when 'HIGH' then 2 else 3 end,
    check_status desc,
    table_name,
    check_id
