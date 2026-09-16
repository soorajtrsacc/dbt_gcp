{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'fees') }}
),

typed as (
    select
        fee_id,
        card_id,
        transaction_id,
        fee_type,
        safe_cast(fee_amount as float64)  as fee_amount,
        safe_cast(fee_date as date)       as fee_date,
        waived,
        waiver_reason,
        statement_cycle,
        (fee_amount is not null and safe_cast(fee_amount as float64) is null) as dq_cast_failed_fee_amount,
        (fee_date is not null and safe_cast(fee_date as date) is null)        as dq_cast_failed_fee_date
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_fee_amount, dq_cast_failed_fee_date),
        array(
            select x from unnest([
                if(fee_id is null,   'REQUIRED_NULL:fee_id',   null),
                if(card_id is null,  'REQUIRED_NULL:card_id',  null),
                if(fee_type is null, 'REQUIRED_NULL:fee_type', null),
                if(dq_cast_failed_fee_amount, 'CAST_FAILED:fee_amount:FLOAT64', null),
                if(dq_cast_failed_fee_date,   'CAST_FAILED:fee_date:DATE',      null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'fees'                      as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(fee_id, card_id, fee_type, fee_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
