{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'chargeback_cases') }}
),

typed as (
    select
        chargeback_id,
        dispute_id,
        card_id,
        safe_cast(chargeback_amount as float64) as chargeback_amount,
        safe_cast(filed_date as date)           as filed_date,
        current_stage,
        acquirer_response,
        network_case_ref,
        safe_cast(resolution_date as date)      as resolution_date,
        bank_liability,
        safe_cast(recovery_amount as float64)   as recovery_amount,
        (chargeback_amount is not null and safe_cast(chargeback_amount as float64) is null) as dq_cast_failed_chargeback_amount,
        (filed_date is not null and safe_cast(filed_date as date) is null)                  as dq_cast_failed_filed_date,
        (resolution_date is not null and safe_cast(resolution_date as date) is null)        as dq_cast_failed_resolution_date,
        (recovery_amount is not null and safe_cast(recovery_amount as float64) is null)     as dq_cast_failed_recovery_amount
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_chargeback_amount, dq_cast_failed_filed_date,
            dq_cast_failed_resolution_date, dq_cast_failed_recovery_amount
        ),
        array(
            select x from unnest([
                if(chargeback_id is null,   'REQUIRED_NULL:chargeback_id',   null),
                if(card_id is null,         'REQUIRED_NULL:card_id',         null),
                if(current_stage is null,   'REQUIRED_NULL:current_stage',   null),
                if(dq_cast_failed_chargeback_amount, 'CAST_FAILED:chargeback_amount:FLOAT64', null),
                if(dq_cast_failed_filed_date,        'CAST_FAILED:filed_date:DATE',           null),
                if(dq_cast_failed_resolution_date,   'CAST_FAILED:resolution_date:DATE',      null),
                if(dq_cast_failed_recovery_amount,   'CAST_FAILED:recovery_amount:FLOAT64',   null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'chargeback_cases'          as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(chargeback_id, card_id, filed_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
