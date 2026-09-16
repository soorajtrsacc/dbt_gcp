-- FK orphan checks across all declared parent-child relationships.
-- Each CTE identifies rows in the child table whose FK value has no match in the parent.
{{ config(materialized='table') }}

with
addr_orphans as (
    select 'silver_addresses' as child_table, 'customer_id' as fk_column,
           'silver_customers' as parent_table, 'customer_id' as pk_column,
           count(*) as orphaned_count, current_timestamp() as checked_at
    from {{ ref('silver_addresses') }} c
    left join {{ ref('silver_customers') }} p using (customer_id)
    where p.customer_id is null
),

acct_orphans as (
    select 'silver_accounts', 'customer_id', 'silver_customers', 'customer_id',
           count(*), current_timestamp()
    from {{ ref('silver_accounts') }} c
    left join {{ ref('silver_customers') }} p using (customer_id)
    where p.customer_id is null
),

cards_cust_orphans as (
    select 'silver_credit_cards', 'customer_id', 'silver_customers', 'customer_id',
           count(*), current_timestamp()
    from {{ ref('silver_credit_cards') }} c
    left join {{ ref('silver_customers') }} p using (customer_id)
    where p.customer_id is null
),

cards_acct_orphans as (
    select 'silver_credit_cards', 'account_id', 'silver_accounts', 'account_id',
           count(*), current_timestamp()
    from {{ ref('silver_credit_cards') }} c
    left join {{ ref('silver_accounts') }} p using (account_id)
    where p.account_id is null
),

cards_type_orphans as (
    select 'silver_credit_cards', 'card_type_id', 'silver_dim_card_types', 'card_type_id',
           count(*), current_timestamp()
    from {{ ref('silver_credit_cards') }} c
    left join {{ ref('silver_dim_card_types') }} p using (card_type_id)
    where p.card_type_id is null
),

merch_mcc_orphans as (
    select 'silver_merchants', 'mcc_code', 'silver_dim_merchant_categories', 'mcc_code',
           count(*), current_timestamp()
    from {{ ref('silver_merchants') }} c
    left join {{ ref('silver_dim_merchant_categories') }} p using (mcc_code)
    where p.mcc_code is null
),

txn_card_orphans as (
    select 'silver_transactions', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_transactions') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

txn_merch_orphans as (
    select 'silver_transactions', 'merchant_id', 'silver_merchants', 'merchant_id',
           count(*), current_timestamp()
    from {{ ref('silver_transactions') }} c
    left join {{ ref('silver_merchants') }} p using (merchant_id)
    where p.merchant_id is null
),

stmt_card_orphans as (
    select 'silver_statements', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_statements') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

pmt_card_orphans as (
    select 'silver_payments', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_payments') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

fraud_txn_orphans as (
    select 'silver_fraud_alerts', 'transaction_id', 'silver_transactions', 'transaction_id',
           count(*), current_timestamp()
    from {{ ref('silver_fraud_alerts') }} c
    left join {{ ref('silver_transactions') }} p using (transaction_id)
    where c.transaction_id is not null and p.transaction_id is null
),

disp_txn_orphans as (
    select 'silver_disputes', 'transaction_id', 'silver_transactions', 'transaction_id',
           count(*), current_timestamp()
    from {{ ref('silver_disputes') }} c
    left join {{ ref('silver_transactions') }} p using (transaction_id)
    where c.transaction_id is not null and p.transaction_id is null
),

cb_disp_orphans as (
    select 'silver_chargeback_cases', 'dispute_id', 'silver_disputes', 'dispute_id',
           count(*), current_timestamp()
    from {{ ref('silver_chargeback_cases') }} c
    left join {{ ref('silver_disputes') }} p using (dispute_id)
    where c.dispute_id is not null and p.dispute_id is null
),

rewards_card_orphans as (
    select 'silver_rewards', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_rewards') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

rdem_reward_orphans as (
    select 'silver_reward_redemptions', 'reward_id', 'silver_rewards', 'reward_id',
           count(*), current_timestamp()
    from {{ ref('silver_reward_redemptions') }} c
    left join {{ ref('silver_rewards') }} p using (reward_id)
    where p.reward_id is null
),

kyc_cust_orphans as (
    select 'silver_kyc_documents', 'customer_id', 'silver_customers', 'customer_id',
           count(*), current_timestamp()
    from {{ ref('silver_kyc_documents') }} c
    left join {{ ref('silver_customers') }} p using (customer_id)
    where p.customer_id is null
),

contact_cust_orphans as (
    select 'silver_contact_history', 'customer_id', 'silver_customers', 'customer_id',
           count(*), current_timestamp()
    from {{ ref('silver_contact_history') }} c
    left join {{ ref('silver_customers') }} p using (customer_id)
    where p.customer_id is null
),

emi_card_orphans as (
    select 'silver_emi_plans', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_emi_plans') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

notif_cust_orphans as (
    select 'silver_notifications', 'customer_id', 'silver_customers', 'customer_id',
           count(*), current_timestamp()
    from {{ ref('silver_notifications') }} c
    left join {{ ref('silver_customers') }} p using (customer_id)
    where p.customer_id is null
),

clim_card_orphans as (
    select 'silver_credit_limit_history', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_credit_limit_history') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

rate_card_orphans as (
    select 'silver_interest_rates', 'card_id', 'silver_credit_cards', 'card_id',
           count(*), current_timestamp()
    from {{ ref('silver_interest_rates') }} c
    left join {{ ref('silver_credit_cards') }} p using (card_id)
    where p.card_id is null
),

all_checks as (
    select * from addr_orphans
    union all select * from acct_orphans
    union all select * from cards_cust_orphans
    union all select * from cards_acct_orphans
    union all select * from cards_type_orphans
    union all select * from merch_mcc_orphans
    union all select * from txn_card_orphans
    union all select * from txn_merch_orphans
    union all select * from stmt_card_orphans
    union all select * from pmt_card_orphans
    union all select * from fraud_txn_orphans
    union all select * from disp_txn_orphans
    union all select * from cb_disp_orphans
    union all select * from rewards_card_orphans
    union all select * from rdem_reward_orphans
    union all select * from kyc_cust_orphans
    union all select * from contact_cust_orphans
    union all select * from emi_card_orphans
    union all select * from notif_cust_orphans
    union all select * from clim_card_orphans
    union all select * from rate_card_orphans
)

select
    child_table,
    fk_column,
    parent_table,
    pk_column,
    orphaned_count,
    if(orphaned_count = 0, 'PASS', 'FAIL') as fk_status,
    checked_at
from all_checks
order by fk_status desc, orphaned_count desc
