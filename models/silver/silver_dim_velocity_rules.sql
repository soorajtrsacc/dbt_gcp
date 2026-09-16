{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'dim_velocity_rules') }}
),

typed as (
    select
        rule_id,
        rule_name,
        rule_type,
        safe_cast(threshold as float64)     as threshold,
        safe_cast(window_hours as int64)    as window_hours,
        action,
        (threshold is not null and safe_cast(threshold as float64) is null)     as dq_cast_failed_threshold,
        (window_hours is not null and safe_cast(window_hours as int64) is null) as dq_cast_failed_window_hours
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_threshold, dq_cast_failed_window_hours),
        array(
            select x from unnest([
                if(rule_id is null,   'REQUIRED_NULL:rule_id',   null),
                if(rule_name is null, 'REQUIRED_NULL:rule_name', null),
                if(rule_type is null, 'REQUIRED_NULL:rule_type', null),
                if(action is null,    'REQUIRED_NULL:action',    null),
                if(dq_cast_failed_threshold,   'CAST_FAILED:threshold:FLOAT64',  null),
                if(dq_cast_failed_window_hours,'CAST_FAILED:window_hours:INT64', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'       as audit_source_system,
    'dim_velocity_rules'            as audit_source_table,
    current_timestamp()             as audit_insert_ts,
    current_timestamp()             as audit_update_ts,
    to_hex(sha256(to_json_string(struct(rule_id, rule_name, rule_type)))) as audit_record_hash,
    current_timestamp()             as dq_checked_at,
    array_length(dq_error_array)    as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
