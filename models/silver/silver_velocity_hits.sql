{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'velocity_hits') }}
),

typed as (
    select
        hit_id,
        rule_id,
        transaction_id,
        card_id,
        safe_cast(triggered_at as date)       as triggered_at,
        safe_cast(measured_value as float64)  as measured_value,
        action_taken,
        override_by,
        (triggered_at is not null and safe_cast(triggered_at as date) is null)      as dq_cast_failed_triggered_at,
        (measured_value is not null and safe_cast(measured_value as float64) is null) as dq_cast_failed_measured_value
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_triggered_at, dq_cast_failed_measured_value),
        array(
            select x from unnest([
                if(hit_id is null,      'REQUIRED_NULL:hit_id',      null),
                if(rule_id is null,     'REQUIRED_NULL:rule_id',     null),
                if(card_id is null,     'REQUIRED_NULL:card_id',     null),
                if(action_taken is null,'REQUIRED_NULL:action_taken',null),
                if(dq_cast_failed_triggered_at,   'CAST_FAILED:triggered_at:DATE',     null),
                if(dq_cast_failed_measured_value, 'CAST_FAILED:measured_value:FLOAT64', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'velocity_hits'             as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(hit_id, rule_id, card_id)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
