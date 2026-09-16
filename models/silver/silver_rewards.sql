{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'rewards') }}
),

typed as (
    select
        reward_id,
        card_id,
        customer_id,
        program_type,
        safe_cast(points_balance as int64)        as points_balance,
        safe_cast(points_earned_ytd as int64)     as points_earned_ytd,
        safe_cast(points_redeemed_ytd as int64)   as points_redeemed_ytd,
        safe_cast(points_expiring as int64)       as points_expiring,
        safe_cast(expiry_date as date)            as expiry_date,
        tier,
        safe_cast(enrolled_date as date)          as enrolled_date,
        (points_balance is not null and safe_cast(points_balance as int64) is null)           as dq_cast_failed_points_balance,
        (expiry_date is not null and safe_cast(expiry_date as date) is null)                  as dq_cast_failed_expiry_date,
        (enrolled_date is not null and safe_cast(enrolled_date as date) is null)              as dq_cast_failed_enrolled_date
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_points_balance, dq_cast_failed_expiry_date, dq_cast_failed_enrolled_date),
        array(
            select x from unnest([
                if(reward_id is null,    'REQUIRED_NULL:reward_id',    null),
                if(card_id is null,      'REQUIRED_NULL:card_id',      null),
                if(customer_id is null,  'REQUIRED_NULL:customer_id',  null),
                if(program_type is null, 'REQUIRED_NULL:program_type', null),
                if(dq_cast_failed_points_balance, 'CAST_FAILED:points_balance:INT64', null),
                if(dq_cast_failed_expiry_date,    'CAST_FAILED:expiry_date:DATE',     null),
                if(dq_cast_failed_enrolled_date,  'CAST_FAILED:enrolled_date:DATE',   null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'rewards'                   as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(reward_id, card_id, customer_id, program_type)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
