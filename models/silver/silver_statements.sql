{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'statements') }}
),

typed as (
    select
        statement_id,
        card_id,
        safe_cast(statement_date as date)     as statement_date,
        safe_cast(due_date as date)           as due_date,
        safe_cast(opening_balance as float64) as opening_balance,
        safe_cast(total_purchases as float64) as total_purchases,
        safe_cast(total_payments as float64)  as total_payments,
        safe_cast(total_fees as float64)      as total_fees,
        safe_cast(total_interest as float64)  as total_interest,
        safe_cast(closing_balance as float64) as closing_balance,
        safe_cast(minimum_payment as float64) as minimum_payment,
        payment_status,
        is_paperless,
        (statement_date is not null and safe_cast(statement_date as date) is null)     as dq_cast_failed_statement_date,
        (due_date is not null and safe_cast(due_date as date) is null)                 as dq_cast_failed_due_date,
        (opening_balance is not null and safe_cast(opening_balance as float64) is null) as dq_cast_failed_opening_balance,
        (closing_balance is not null and safe_cast(closing_balance as float64) is null) as dq_cast_failed_closing_balance
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_statement_date, dq_cast_failed_due_date,
            dq_cast_failed_opening_balance, dq_cast_failed_closing_balance
        ),
        array(
            select x from unnest([
                if(statement_id is null,   'REQUIRED_NULL:statement_id',   null),
                if(card_id is null,        'REQUIRED_NULL:card_id',        null),
                if(payment_status is null, 'REQUIRED_NULL:payment_status', null),
                if(dq_cast_failed_statement_date,  'CAST_FAILED:statement_date:DATE',      null),
                if(dq_cast_failed_due_date,        'CAST_FAILED:due_date:DATE',            null),
                if(dq_cast_failed_opening_balance, 'CAST_FAILED:opening_balance:FLOAT64',  null),
                if(dq_cast_failed_closing_balance, 'CAST_FAILED:closing_balance:FLOAT64',  null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'statements'                as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(statement_id, card_id, statement_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
