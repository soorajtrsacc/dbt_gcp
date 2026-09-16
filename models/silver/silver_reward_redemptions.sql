{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'reward_redemptions') }}
),

typed as (
    select
        redemption_id,
        reward_id,
        card_id,
        customer_id,
        redemption_type,
        safe_cast(points_used as int64)           as points_used,
        safe_cast(cash_equivalent as float64)     as cash_equivalent,
        safe_cast(redeemed_date as date)          as redeemed_date,
        reference,
        status,
        (points_used is not null and safe_cast(points_used as int64) is null)             as dq_cast_failed_points_used,
        (cash_equivalent is not null and safe_cast(cash_equivalent as float64) is null)   as dq_cast_failed_cash_equivalent,
        (redeemed_date is not null and safe_cast(redeemed_date as date) is null)          as dq_cast_failed_redeemed_date
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_points_used, dq_cast_failed_cash_equivalent, dq_cast_failed_redeemed_date),
        array(
            select x from unnest([
                if(redemption_id is null,   'REQUIRED_NULL:redemption_id',   null),
                if(reward_id is null,       'REQUIRED_NULL:reward_id',       null),
                if(card_id is null,         'REQUIRED_NULL:card_id',         null),
                if(customer_id is null,     'REQUIRED_NULL:customer_id',     null),
                if(status is null,          'REQUIRED_NULL:status',          null),
                if(dq_cast_failed_points_used,      'CAST_FAILED:points_used:INT64',        null),
                if(dq_cast_failed_cash_equivalent,  'CAST_FAILED:cash_equivalent:FLOAT64',  null),
                if(dq_cast_failed_redeemed_date,    'CAST_FAILED:redeemed_date:DATE',       null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'reward_redemptions'        as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(redemption_id, reward_id, card_id, redeemed_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
