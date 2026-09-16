{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'customers') }}
),

typed as (
    select
        customer_id,
        first_name,
        last_name,
        safe_cast(date_of_birth as date)    as date_of_birth,
        gender,
        email,
        phone_mobile,
        phone_home,
        nationality,
        occupation,
        safe_cast(annual_income as float64) as annual_income,
        safe_cast(credit_score as int64)    as credit_score,
        customer_status,
        safe_cast(created_at as date)       as created_at,
        safe_cast(updated_at as date)       as updated_at,
        -- cast failure flags (null-on-failure → detected below)
        (date_of_birth is not null and safe_cast(date_of_birth as date) is null)       as dq_cast_failed_date_of_birth,
        (annual_income is not null and safe_cast(annual_income as float64) is null)    as dq_cast_failed_annual_income,
        (credit_score is not null and safe_cast(credit_score as int64) is null)        as dq_cast_failed_credit_score,
        (created_at is not null and safe_cast(created_at as date) is null)             as dq_cast_failed_created_at,
        (updated_at is not null and safe_cast(updated_at as date) is null)             as dq_cast_failed_updated_at
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_date_of_birth, dq_cast_failed_annual_income,
            dq_cast_failed_credit_score, dq_cast_failed_created_at, dq_cast_failed_updated_at
        ),
        array(
            select x from unnest([
                if(customer_id is null,     'REQUIRED_NULL:customer_id',    null),
                if(first_name is null,      'REQUIRED_NULL:first_name',     null),
                if(last_name is null,       'REQUIRED_NULL:last_name',      null),
                if(customer_status is null, 'REQUIRED_NULL:customer_status',null),
                if(dq_cast_failed_date_of_birth,  'CAST_FAILED:date_of_birth:DATE',    null),
                if(dq_cast_failed_annual_income,  'CAST_FAILED:annual_income:FLOAT64', null),
                if(dq_cast_failed_credit_score,   'CAST_FAILED:credit_score:INT64',    null),
                if(dq_cast_failed_created_at,     'CAST_FAILED:created_at:DATE',       null),
                if(dq_cast_failed_updated_at,     'CAST_FAILED:updated_at:DATE',       null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'customers'                 as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(customer_id, first_name, last_name, email)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
