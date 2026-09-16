{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'merchant_blacklist') }}
),

typed as (
    select
        blacklist_id,
        merchant_id,
        merchant_name,
        blacklist_reason,
        safe_cast(added_date as date)   as added_date,
        added_by,
        safe_cast(review_date as date)  as review_date,
        is_active,
        notes,
        (added_date is not null and safe_cast(added_date as date) is null)   as dq_cast_failed_added_date,
        (review_date is not null and safe_cast(review_date as date) is null) as dq_cast_failed_review_date
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_added_date, dq_cast_failed_review_date),
        array(
            select x from unnest([
                if(blacklist_id is null,     'REQUIRED_NULL:blacklist_id',     null),
                if(merchant_id is null,      'REQUIRED_NULL:merchant_id',      null),
                if(blacklist_reason is null, 'REQUIRED_NULL:blacklist_reason', null),
                if(dq_cast_failed_added_date,  'CAST_FAILED:added_date:DATE',  null),
                if(dq_cast_failed_review_date, 'CAST_FAILED:review_date:DATE', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'merchant_blacklist'        as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(blacklist_id, merchant_id, blacklist_reason)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
