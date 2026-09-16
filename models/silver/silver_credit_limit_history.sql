{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'credit_limit_history') }}
),

typed as (
    select
        limit_history_id,
        card_id,
        safe_cast(effective_date as date)   as effective_date,
        safe_cast(previous_limit as int64)  as previous_limit,
        safe_cast(new_limit as int64)       as new_limit,
        change_reason,
        approved_by,
        is_current,
        (effective_date is not null and safe_cast(effective_date as date) is null)  as dq_cast_failed_effective_date,
        (previous_limit is not null and safe_cast(previous_limit as int64) is null) as dq_cast_failed_previous_limit,
        (new_limit is not null and safe_cast(new_limit as int64) is null)           as dq_cast_failed_new_limit
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_effective_date, dq_cast_failed_previous_limit, dq_cast_failed_new_limit),
        array(
            select x from unnest([
                if(limit_history_id is null, 'REQUIRED_NULL:limit_history_id', null),
                if(card_id is null,          'REQUIRED_NULL:card_id',          null),
                if(new_limit is null,        'REQUIRED_NULL:new_limit',        null),
                if(dq_cast_failed_effective_date, 'CAST_FAILED:effective_date:DATE',  null),
                if(dq_cast_failed_previous_limit, 'CAST_FAILED:previous_limit:INT64', null),
                if(dq_cast_failed_new_limit,      'CAST_FAILED:new_limit:INT64',      null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'credit_limit_history'      as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(limit_history_id, card_id, effective_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
