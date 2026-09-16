{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'disputes') }}
),

typed as (
    select
        dispute_id,
        transaction_id,
        card_id,
        dispute_reason,
        safe_cast(filed_date as date)         as filed_date,
        safe_cast(disputed_amount as float64) as disputed_amount,
        outcome,
        safe_cast(outcome_date as date)       as outcome_date,
        safe_cast(refund_amount as float64)   as refund_amount,
        case_reference,
        status,
        (filed_date is not null and safe_cast(filed_date as date) is null)             as dq_cast_failed_filed_date,
        (disputed_amount is not null and safe_cast(disputed_amount as float64) is null) as dq_cast_failed_disputed_amount,
        (outcome_date is not null and safe_cast(outcome_date as date) is null)          as dq_cast_failed_outcome_date,
        (refund_amount is not null and safe_cast(refund_amount as float64) is null)     as dq_cast_failed_refund_amount
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_filed_date, dq_cast_failed_disputed_amount,
            dq_cast_failed_outcome_date, dq_cast_failed_refund_amount
        ),
        array(
            select x from unnest([
                if(dispute_id is null,     'REQUIRED_NULL:dispute_id',     null),
                if(card_id is null,        'REQUIRED_NULL:card_id',        null),
                if(dispute_reason is null, 'REQUIRED_NULL:dispute_reason', null),
                if(status is null,         'REQUIRED_NULL:status',         null),
                if(dq_cast_failed_filed_date,      'CAST_FAILED:filed_date:DATE',          null),
                if(dq_cast_failed_disputed_amount, 'CAST_FAILED:disputed_amount:FLOAT64',  null),
                if(dq_cast_failed_outcome_date,    'CAST_FAILED:outcome_date:DATE',        null),
                if(dq_cast_failed_refund_amount,   'CAST_FAILED:refund_amount:FLOAT64',    null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'disputes'                  as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(dispute_id, card_id, dispute_reason, filed_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
