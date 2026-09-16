{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'accounts') }}
),

typed as (
    select
        account_id,
        customer_id,
        account_type,
        account_number,
        sort_code,
        currency,
        safe_cast(balance as float64)          as balance,
        safe_cast(overdraft_limit as int64)    as overdraft_limit,
        account_status,
        safe_cast(opened_date as date)         as opened_date,
        safe_cast(closed_date as date)         as closed_date,
        (balance is not null and safe_cast(balance as float64) is null)             as dq_cast_failed_balance,
        (overdraft_limit is not null and safe_cast(overdraft_limit as int64) is null) as dq_cast_failed_overdraft_limit,
        (opened_date is not null and safe_cast(opened_date as date) is null)        as dq_cast_failed_opened_date,
        (closed_date is not null and safe_cast(closed_date as date) is null)        as dq_cast_failed_closed_date
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_balance, dq_cast_failed_overdraft_limit,
            dq_cast_failed_opened_date, dq_cast_failed_closed_date
        ),
        array(
            select x from unnest([
                if(account_id is null,     'REQUIRED_NULL:account_id',     null),
                if(customer_id is null,    'REQUIRED_NULL:customer_id',    null),
                if(account_status is null, 'REQUIRED_NULL:account_status', null),
                if(currency is null,       'REQUIRED_NULL:currency',       null),
                if(dq_cast_failed_balance,          'CAST_FAILED:balance:FLOAT64',          null),
                if(dq_cast_failed_overdraft_limit,  'CAST_FAILED:overdraft_limit:INT64',    null),
                if(dq_cast_failed_opened_date,      'CAST_FAILED:opened_date:DATE',         null),
                if(dq_cast_failed_closed_date,      'CAST_FAILED:closed_date:DATE',         null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'accounts'                  as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(account_id, customer_id, account_number)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
