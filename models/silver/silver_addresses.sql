{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'addresses') }}
),

typed as (
    select
        address_id,
        customer_id,
        address_type,
        address_line1,
        address_line2,
        city,
        county,
        postcode,
        country,
        is_primary,
        safe_cast(valid_from as date) as valid_from,
        safe_cast(valid_to as date)   as valid_to,
        (valid_from is not null and safe_cast(valid_from as date) is null) as dq_cast_failed_valid_from,
        (valid_to is not null and safe_cast(valid_to as date) is null)     as dq_cast_failed_valid_to
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_valid_from, dq_cast_failed_valid_to),
        array(
            select x from unnest([
                if(address_id is null,  'REQUIRED_NULL:address_id',  null),
                if(customer_id is null, 'REQUIRED_NULL:customer_id', null),
                if(city is null,        'REQUIRED_NULL:city',        null),
                if(postcode is null,    'REQUIRED_NULL:postcode',    null),
                if(dq_cast_failed_valid_from, 'CAST_FAILED:valid_from:DATE', null),
                if(dq_cast_failed_valid_to,   'CAST_FAILED:valid_to:DATE',   null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'addresses'                 as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(address_id, customer_id, postcode)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
