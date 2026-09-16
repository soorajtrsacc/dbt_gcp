{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'payments') }}
),

typed as (
    select
        payment_id,
        statement_id,
        card_id,
        safe_cast(payment_date as date)  as payment_date,
        safe_cast(amount as float64)     as amount,
        payment_method,
        reference,
        status,
        is_minimum_only,
        (payment_date is not null and safe_cast(payment_date as date) is null)  as dq_cast_failed_payment_date,
        (amount is not null and safe_cast(amount as float64) is null)           as dq_cast_failed_amount
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_payment_date, dq_cast_failed_amount),
        array(
            select x from unnest([
                if(payment_id is null,   'REQUIRED_NULL:payment_id',   null),
                if(card_id is null,      'REQUIRED_NULL:card_id',      null),
                if(amount is null,       'REQUIRED_NULL:amount',       null),
                if(status is null,       'REQUIRED_NULL:status',       null),
                if(dq_cast_failed_payment_date, 'CAST_FAILED:payment_date:DATE',   null),
                if(dq_cast_failed_amount,       'CAST_FAILED:amount:FLOAT64',      null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'payments'                  as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(payment_id, card_id, payment_date, amount)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
