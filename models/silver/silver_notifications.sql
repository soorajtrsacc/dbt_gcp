{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'notifications') }}
),

typed as (
    select
        notification_id,
        customer_id,
        notification_type,
        channel,
        safe_cast(sent_at as date)  as sent_at,
        delivered,
        `read`,
        message_preview,
        action_taken,
        (sent_at is not null and safe_cast(sent_at as date) is null) as dq_cast_failed_sent_at
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_sent_at),
        array(
            select x from unnest([
                if(notification_id is null,   'REQUIRED_NULL:notification_id',   null),
                if(customer_id is null,       'REQUIRED_NULL:customer_id',       null),
                if(notification_type is null, 'REQUIRED_NULL:notification_type', null),
                if(channel is null,           'REQUIRED_NULL:channel',           null),
                if(dq_cast_failed_sent_at, 'CAST_FAILED:sent_at:DATE', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'notifications'             as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(notification_id, customer_id, notification_type, sent_at)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
