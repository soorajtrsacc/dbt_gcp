-- Row-count and key-integrity reconciliation across all three medallion layers.
-- Bronze → Silver reconciliation for all 26 source tables.
-- Silver → Gold reconciliation for all 6 canonical entities.
-- reconciliation_status = PASS when source_count = intermediate_count (or target_count for S2G)
-- and missing/duplicate counts are all zero.
{{ config(materialized='table') }}

with
-- ── Bronze → Silver ───────────────────────────────────────────────────────────
b2s_customers as (
    {{ reconcile_bronze_to_silver('customers', 'customer_id') }}
),
b2s_addresses as (
    {{ reconcile_bronze_to_silver('addresses', 'address_id') }}
),
b2s_accounts as (
    {{ reconcile_bronze_to_silver('accounts', 'account_id') }}
),
b2s_credit_cards as (
    {{ reconcile_bronze_to_silver('credit_cards', 'card_id') }}
),
b2s_merchants as (
    {{ reconcile_bronze_to_silver('merchants', 'merchant_id') }}
),
b2s_transactions as (
    {{ reconcile_bronze_to_silver('transactions', 'transaction_id') }}
),
b2s_statements as (
    {{ reconcile_bronze_to_silver('statements', 'statement_id') }}
),
b2s_payments as (
    {{ reconcile_bronze_to_silver('payments', 'payment_id') }}
),
b2s_fees as (
    {{ reconcile_bronze_to_silver('fees', 'fee_id') }}
),
b2s_fraud_alerts as (
    {{ reconcile_bronze_to_silver('fraud_alerts', 'alert_id') }}
),
b2s_disputes as (
    {{ reconcile_bronze_to_silver('disputes', 'dispute_id') }}
),
b2s_chargeback_cases as (
    {{ reconcile_bronze_to_silver('chargeback_cases', 'chargeback_id') }}
),
b2s_velocity_hits as (
    {{ reconcile_bronze_to_silver('velocity_hits', 'hit_id') }}
),
b2s_merchant_blacklist as (
    {{ reconcile_bronze_to_silver('merchant_blacklist', 'blacklist_id') }}
),
b2s_rewards as (
    {{ reconcile_bronze_to_silver('rewards', 'reward_id') }}
),
b2s_reward_redemptions as (
    {{ reconcile_bronze_to_silver('reward_redemptions', 'redemption_id') }}
),
b2s_kyc_documents as (
    {{ reconcile_bronze_to_silver('kyc_documents', 'kyc_id') }}
),
b2s_contact_history as (
    {{ reconcile_bronze_to_silver('contact_history', 'contact_id') }}
),
b2s_emi_plans as (
    {{ reconcile_bronze_to_silver('emi_plans', 'emi_plan_id') }}
),
b2s_notifications as (
    {{ reconcile_bronze_to_silver('notifications', 'notification_id') }}
),
b2s_credit_limit_history as (
    {{ reconcile_bronze_to_silver('credit_limit_history', 'limit_history_id') }}
),
b2s_interest_rates as (
    {{ reconcile_bronze_to_silver('interest_rates', 'rate_id') }}
),
b2s_dim_card_types as (
    {{ reconcile_bronze_to_silver('dim_card_types', 'card_type_id') }}
),
b2s_dim_merchant_categories as (
    {{ reconcile_bronze_to_silver('dim_merchant_categories', 'mcc_code') }}
),
b2s_dim_transaction_types as (
    {{ reconcile_bronze_to_silver('dim_transaction_types', 'txn_type_id') }}
),
b2s_dim_velocity_rules as (
    {{ reconcile_bronze_to_silver('dim_velocity_rules', 'rule_id') }}
),

-- ── Silver → Gold ─────────────────────────────────────────────────────────────
s2g_customer_360 as (
    {{ reconcile_silver_to_gold('customers', 'customer_id', 'silver_customers', 'customer_id', 'gold_customer_360', 'customer_id') }}
),
s2g_transaction_360 as (
    {{ reconcile_silver_to_gold('transactions', 'transaction_id', 'silver_transactions', 'transaction_id', 'gold_transaction_360', 'transaction_id') }}
),
s2g_merchant_performance as (
    {{ reconcile_silver_to_gold('merchants', 'merchant_id', 'silver_merchants', 'merchant_id', 'gold_merchant_performance', 'merchant_id') }}
),
s2g_account_financial_summary as (
    {{ reconcile_silver_to_gold('accounts', 'account_id', 'silver_accounts', 'account_id', 'gold_account_financial_summary', 'account_id') }}
),
s2g_fraud_case_360 as (
    {{ reconcile_silver_to_gold('fraud_alerts', 'alert_id', 'silver_fraud_alerts', 'alert_id', 'gold_fraud_case_360', 'alert_id') }}
),
s2g_rewards_engagement as (
    {{ reconcile_silver_to_gold('rewards', 'reward_id', 'silver_rewards', 'reward_id', 'gold_rewards_engagement', 'reward_id') }}
),

all_reconciliations as (
    select * from b2s_customers
    union all select * from b2s_addresses
    union all select * from b2s_accounts
    union all select * from b2s_credit_cards
    union all select * from b2s_merchants
    union all select * from b2s_transactions
    union all select * from b2s_statements
    union all select * from b2s_payments
    union all select * from b2s_fees
    union all select * from b2s_fraud_alerts
    union all select * from b2s_disputes
    union all select * from b2s_chargeback_cases
    union all select * from b2s_velocity_hits
    union all select * from b2s_merchant_blacklist
    union all select * from b2s_rewards
    union all select * from b2s_reward_redemptions
    union all select * from b2s_kyc_documents
    union all select * from b2s_contact_history
    union all select * from b2s_emi_plans
    union all select * from b2s_notifications
    union all select * from b2s_credit_limit_history
    union all select * from b2s_interest_rates
    union all select * from b2s_dim_card_types
    union all select * from b2s_dim_merchant_categories
    union all select * from b2s_dim_transaction_types
    union all select * from b2s_dim_velocity_rules
    union all select * from s2g_customer_360
    union all select * from s2g_transaction_360
    union all select * from s2g_merchant_performance
    union all select * from s2g_account_financial_summary
    union all select * from s2g_fraud_case_360
    union all select * from s2g_rewards_engagement
)

select
    reconciliation_id,
    reconciliation_scope,
    source_table,
    target_table,
    source_count,
    intermediate_count,
    target_count,
    coalesce(missing_in_intermediate, 0)        as missing_in_intermediate,
    coalesce(missing_in_target, 0)              as missing_in_target,
    coalesce(duplicate_intermediate_keys, 0)    as duplicate_intermediate_keys,
    coalesce(duplicate_target_keys, 0)          as duplicate_target_keys,
    case
        when reconciliation_scope = 'BRONZE_TO_SILVER' then
            if(
                source_count = intermediate_count
                and coalesce(missing_in_intermediate, 0) = 0
                and coalesce(duplicate_intermediate_keys, 0) = 0,
                'PASS', 'FAIL'
            )
        when reconciliation_scope = 'SILVER_TO_GOLD' then
            if(
                source_count = target_count
                and coalesce(missing_in_target, 0) = 0
                and coalesce(duplicate_target_keys, 0) = 0,
                'PASS', 'FAIL'
            )
        else 'UNKNOWN'
    end                                         as reconciliation_status,
    reconciliation_ts
from all_reconciliations
order by
    reconciliation_scope,
    reconciliation_status desc,
    source_table
