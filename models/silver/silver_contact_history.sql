{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'contact_history') }}
),

typed as (
    select
        contact_id,
        customer_id,
        safe_cast(contact_date as date)          as contact_date,
        channel,
        reason,
        outcome,
        safe_cast(duration_seconds as int64)     as duration_seconds,
        agent_id,
        safe_cast(satisfaction_score as int64)   as satisfaction_score,
        notes,
        (contact_date is not null and safe_cast(contact_date as date) is null)                 as dq_cast_failed_contact_date,
        (duration_seconds is not null and safe_cast(duration_seconds as int64) is null)        as dq_cast_failed_duration_seconds,
        (satisfaction_score is not null and safe_cast(satisfaction_score as int64) is null)    as dq_cast_failed_satisfaction_score
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_contact_date, dq_cast_failed_duration_seconds, dq_cast_failed_satisfaction_score),
        array(
            select x from unnest([
                if(contact_id is null,   'REQUIRED_NULL:contact_id',   null),
                if(customer_id is null,  'REQUIRED_NULL:customer_id',  null),
                if(channel is null,      'REQUIRED_NULL:channel',      null),
                if(reason is null,       'REQUIRED_NULL:reason',       null),
                if(dq_cast_failed_contact_date,      'CAST_FAILED:contact_date:DATE',         null),
                if(dq_cast_failed_duration_seconds,  'CAST_FAILED:duration_seconds:INT64',    null),
                if(dq_cast_failed_satisfaction_score,'CAST_FAILED:satisfaction_score:INT64',  null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'contact_history'           as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(contact_id, customer_id, contact_date, channel)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
