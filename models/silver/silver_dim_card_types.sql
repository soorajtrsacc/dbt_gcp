{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'dim_card_types') }}
),

typed as (
    select
        card_type_id,
        card_name,
        network,
        tier,
        safe_cast(annual_fee as float64)    as annual_fee,
        safe_cast(reward_rate as float64)   as reward_rate,
        lounge_access,
        travel_insurance,
        (annual_fee is not null and safe_cast(annual_fee as float64) is null)   as dq_cast_failed_annual_fee,
        (reward_rate is not null and safe_cast(reward_rate as float64) is null) as dq_cast_failed_reward_rate
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_annual_fee, dq_cast_failed_reward_rate),
        array(
            select x from unnest([
                if(card_type_id is null, 'REQUIRED_NULL:card_type_id', null),
                if(card_name is null,    'REQUIRED_NULL:card_name',    null),
                if(network is null,      'REQUIRED_NULL:network',      null),
                if(tier is null,         'REQUIRED_NULL:tier',         null),
                if(dq_cast_failed_annual_fee,  'CAST_FAILED:annual_fee:FLOAT64',  null),
                if(dq_cast_failed_reward_rate, 'CAST_FAILED:reward_rate:FLOAT64', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'dim_card_types'            as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(card_type_id, card_name, network, tier)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
