{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'credit_cards') }}
),

typed as (
    select
        card_id,
        customer_id,
        account_id,
        card_type_id,
        card_number_masked,
        card_number_hash,
        expiry_date,
        cardholder_name,
        safe_cast(credit_limit as int64)         as credit_limit,
        safe_cast(available_credit as float64)   as available_credit,
        card_status,
        safe_cast(issued_date as date)           as issued_date,
        safe_cast(activation_date as date)       as activation_date,
        is_contactless,
        is_virtual,
        safe_cast(pin_failed_attempts as int64)  as pin_failed_attempts,
        (credit_limit is not null and safe_cast(credit_limit as int64) is null)           as dq_cast_failed_credit_limit,
        (available_credit is not null and safe_cast(available_credit as float64) is null) as dq_cast_failed_available_credit,
        (issued_date is not null and safe_cast(issued_date as date) is null)              as dq_cast_failed_issued_date,
        (activation_date is not null and safe_cast(activation_date as date) is null)      as dq_cast_failed_activation_date,
        (pin_failed_attempts is not null and safe_cast(pin_failed_attempts as int64) is null) as dq_cast_failed_pin_failed_attempts
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_credit_limit, dq_cast_failed_available_credit,
            dq_cast_failed_issued_date, dq_cast_failed_activation_date,
            dq_cast_failed_pin_failed_attempts
        ),
        array(
            select x from unnest([
                if(card_id is null,      'REQUIRED_NULL:card_id',      null),
                if(customer_id is null,  'REQUIRED_NULL:customer_id',  null),
                if(account_id is null,   'REQUIRED_NULL:account_id',   null),
                if(card_type_id is null, 'REQUIRED_NULL:card_type_id', null),
                if(card_status is null,  'REQUIRED_NULL:card_status',  null),
                if(dq_cast_failed_credit_limit,        'CAST_FAILED:credit_limit:INT64',           null),
                if(dq_cast_failed_available_credit,    'CAST_FAILED:available_credit:FLOAT64',     null),
                if(dq_cast_failed_issued_date,         'CAST_FAILED:issued_date:DATE',             null),
                if(dq_cast_failed_activation_date,     'CAST_FAILED:activation_date:DATE',         null),
                if(dq_cast_failed_pin_failed_attempts, 'CAST_FAILED:pin_failed_attempts:INT64',    null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'credit_cards'              as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(card_id, customer_id, card_number_hash)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
