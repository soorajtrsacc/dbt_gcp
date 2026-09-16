{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'merchants') }}
),

typed as (
    select
        merchant_id,
        merchant_name,
        mcc_code,
        merchant_city,
        merchant_country,
        merchant_postcode,
        terminal_id,
        acquirer_bank,
        is_online,
        safe_cast(registered_date as date) as registered_date,
        (registered_date is not null and safe_cast(registered_date as date) is null) as dq_cast_failed_registered_date
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_registered_date),
        array(
            select x from unnest([
                if(merchant_id is null,   'REQUIRED_NULL:merchant_id',   null),
                if(merchant_name is null, 'REQUIRED_NULL:merchant_name', null),
                if(mcc_code is null,      'REQUIRED_NULL:mcc_code',      null),
                if(dq_cast_failed_registered_date, 'CAST_FAILED:registered_date:DATE', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'merchants'                 as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(merchant_id, merchant_name, mcc_code)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
