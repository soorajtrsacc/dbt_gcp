{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'fraud_alerts') }}
),

typed as (
    select
        alert_id,
        transaction_id,
        card_id,
        alert_type,
        safe_cast(alert_timestamp as timestamp)  as alert_timestamp,
        safe_cast(risk_score as float64)         as risk_score,
        status,
        safe_cast(resolved_at as timestamp)      as resolved_at,
        analyst_notes,
        is_genuine_fraud,
        (alert_timestamp is not null and safe_cast(alert_timestamp as timestamp) is null) as dq_cast_failed_alert_timestamp,
        (risk_score is not null and safe_cast(risk_score as float64) is null)             as dq_cast_failed_risk_score,
        (resolved_at is not null and safe_cast(resolved_at as timestamp) is null)         as dq_cast_failed_resolved_at
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_alert_timestamp, dq_cast_failed_risk_score, dq_cast_failed_resolved_at),
        array(
            select x from unnest([
                if(alert_id is null,   'REQUIRED_NULL:alert_id',   null),
                if(card_id is null,    'REQUIRED_NULL:card_id',    null),
                if(alert_type is null, 'REQUIRED_NULL:alert_type', null),
                if(status is null,     'REQUIRED_NULL:status',     null),
                if(dq_cast_failed_alert_timestamp, 'CAST_FAILED:alert_timestamp:TIMESTAMP', null),
                if(dq_cast_failed_risk_score,      'CAST_FAILED:risk_score:FLOAT64',        null),
                if(dq_cast_failed_resolved_at,     'CAST_FAILED:resolved_at:TIMESTAMP',     null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'fraud_alerts'              as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(alert_id, card_id, alert_type)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
