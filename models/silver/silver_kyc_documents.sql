{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'kyc_documents') }}
),

typed as (
    select
        kyc_id,
        customer_id,
        document_type,
        document_number,
        issued_country,
        safe_cast(issue_date as date)     as issue_date,
        safe_cast(expiry_date as date)    as expiry_date,
        safe_cast(verified_date as date)  as verified_date,
        verification_method,
        status,
        reviewer_id,
        (issue_date is not null and safe_cast(issue_date as date) is null)       as dq_cast_failed_issue_date,
        (expiry_date is not null and safe_cast(expiry_date as date) is null)     as dq_cast_failed_expiry_date,
        (verified_date is not null and safe_cast(verified_date as date) is null) as dq_cast_failed_verified_date
    from bronze
),

quality as (
    select
        * except (dq_cast_failed_issue_date, dq_cast_failed_expiry_date, dq_cast_failed_verified_date),
        array(
            select x from unnest([
                if(kyc_id is null,         'REQUIRED_NULL:kyc_id',         null),
                if(customer_id is null,    'REQUIRED_NULL:customer_id',    null),
                if(document_type is null,  'REQUIRED_NULL:document_type',  null),
                if(status is null,         'REQUIRED_NULL:status',         null),
                if(dq_cast_failed_issue_date,    'CAST_FAILED:issue_date:DATE',    null),
                if(dq_cast_failed_expiry_date,   'CAST_FAILED:expiry_date:DATE',   null),
                if(dq_cast_failed_verified_date, 'CAST_FAILED:verified_date:DATE', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'kyc_documents'             as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(kyc_id, customer_id, document_type, document_number)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
