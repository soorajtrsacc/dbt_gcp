{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'interest_rates') }}
),

typed as (
    select
        rate_id,
        card_id,
        rate_type,
        safe_cast(apr as float64)               as apr,
        safe_cast(daily_rate as float64)        as daily_rate,
        safe_cast(promotional_rate as float64)  as promotional_rate,
        safe_cast(promo_end_date as date)       as promo_end_date,
        safe_cast(effective_date as date)       as effective_date,
        is_current,
        (apr is not null and safe_cast(apr as float64) is null)                             as dq_cast_failed_apr,
        (daily_rate is not null and safe_cast(daily_rate as float64) is null)               as dq_cast_failed_daily_rate,
        (promotional_rate is not null and safe_cast(promotional_rate as float64) is null)   as dq_cast_failed_promotional_rate,
        (promo_end_date is not null and safe_cast(promo_end_date as date) is null)          as dq_cast_failed_promo_end_date,
        (effective_date is not null and safe_cast(effective_date as date) is null)          as dq_cast_failed_effective_date
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_apr, dq_cast_failed_daily_rate, dq_cast_failed_promotional_rate,
            dq_cast_failed_promo_end_date, dq_cast_failed_effective_date
        ),
        array(
            select x from unnest([
                if(rate_id is null,   'REQUIRED_NULL:rate_id',   null),
                if(card_id is null,   'REQUIRED_NULL:card_id',   null),
                if(rate_type is null, 'REQUIRED_NULL:rate_type', null),
                if(apr is null,       'REQUIRED_NULL:apr',       null),
                if(dq_cast_failed_apr,             'CAST_FAILED:apr:FLOAT64',             null),
                if(dq_cast_failed_daily_rate,      'CAST_FAILED:daily_rate:FLOAT64',      null),
                if(dq_cast_failed_promotional_rate,'CAST_FAILED:promotional_rate:FLOAT64', null),
                if(dq_cast_failed_promo_end_date,  'CAST_FAILED:promo_end_date:DATE',     null),
                if(dq_cast_failed_effective_date,  'CAST_FAILED:effective_date:DATE',     null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'interest_rates'            as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(rate_id, card_id, rate_type, effective_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
