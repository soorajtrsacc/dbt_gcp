{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'transactions') }}
),

typed as (
    select
        transaction_id,
        card_id,
        merchant_id,
        txn_type_id,
        safe_cast(transaction_date as date)    as transaction_date,
        safe_cast(posting_date as date)        as posting_date,
        safe_cast(amount as float64)           as amount,
        currency,
        safe_cast(original_amount as float64)  as original_amount,
        original_currency,
        safe_cast(fx_rate as float64)          as fx_rate,
        description,
        status,
        is_online,
        is_international,
        mcc_code,
        auth_code,
        is_flagged,
        (transaction_date is not null and safe_cast(transaction_date as date) is null)  as dq_cast_failed_transaction_date,
        (posting_date is not null and safe_cast(posting_date as date) is null)          as dq_cast_failed_posting_date,
        (amount is not null and safe_cast(amount as float64) is null)                   as dq_cast_failed_amount,
        (original_amount is not null and safe_cast(original_amount as float64) is null) as dq_cast_failed_original_amount,
        (fx_rate is not null and safe_cast(fx_rate as float64) is null)                 as dq_cast_failed_fx_rate
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_transaction_date, dq_cast_failed_posting_date,
            dq_cast_failed_amount, dq_cast_failed_original_amount, dq_cast_failed_fx_rate
        ),
        array(
            select x from unnest([
                if(transaction_id is null,   'REQUIRED_NULL:transaction_id',   null),
                if(card_id is null,          'REQUIRED_NULL:card_id',          null),
                if(amount is null,           'REQUIRED_NULL:amount',           null),
                if(currency is null,         'REQUIRED_NULL:currency',         null),
                if(status is null,           'REQUIRED_NULL:status',           null),
                if(dq_cast_failed_transaction_date, 'CAST_FAILED:transaction_date:DATE',     null),
                if(dq_cast_failed_posting_date,     'CAST_FAILED:posting_date:DATE',         null),
                if(dq_cast_failed_amount,           'CAST_FAILED:amount:FLOAT64',            null),
                if(dq_cast_failed_original_amount,  'CAST_FAILED:original_amount:FLOAT64',   null),
                if(dq_cast_failed_fx_rate,          'CAST_FAILED:fx_rate:FLOAT64',           null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'transactions'              as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(transaction_id, card_id, merchant_id, amount)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
