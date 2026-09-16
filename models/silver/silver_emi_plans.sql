{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'emi_plans') }}
),

typed as (
    select
        emi_plan_id,
        transaction_id,
        card_id,
        safe_cast(original_amount as float64)          as original_amount,
        safe_cast(tenure_months as int64)              as tenure_months,
        safe_cast(monthly_interest_rate as float64)    as monthly_interest_rate,
        safe_cast(emi_amount as float64)               as emi_amount,
        safe_cast(total_payable as float64)            as total_payable,
        safe_cast(start_date as date)                  as start_date,
        safe_cast(end_date as date)                    as end_date,
        safe_cast(installments_paid as int64)          as installments_paid,
        status,
        (original_amount is not null and safe_cast(original_amount as float64) is null)         as dq_cast_failed_original_amount,
        (tenure_months is not null and safe_cast(tenure_months as int64) is null)               as dq_cast_failed_tenure_months,
        (emi_amount is not null and safe_cast(emi_amount as float64) is null)                   as dq_cast_failed_emi_amount,
        (start_date is not null and safe_cast(start_date as date) is null)                      as dq_cast_failed_start_date,
        (end_date is not null and safe_cast(end_date as date) is null)                          as dq_cast_failed_end_date,
        (installments_paid is not null and safe_cast(installments_paid as int64) is null)       as dq_cast_failed_installments_paid
    from bronze
),

quality as (
    select
        * except (
            dq_cast_failed_original_amount, dq_cast_failed_tenure_months, dq_cast_failed_emi_amount,
            dq_cast_failed_start_date, dq_cast_failed_end_date, dq_cast_failed_installments_paid
        ),
        array(
            select x from unnest([
                if(emi_plan_id is null, 'REQUIRED_NULL:emi_plan_id', null),
                if(card_id is null,     'REQUIRED_NULL:card_id',     null),
                if(status is null,      'REQUIRED_NULL:status',      null),
                if(dq_cast_failed_original_amount,  'CAST_FAILED:original_amount:FLOAT64',      null),
                if(dq_cast_failed_tenure_months,    'CAST_FAILED:tenure_months:INT64',          null),
                if(dq_cast_failed_emi_amount,       'CAST_FAILED:emi_amount:FLOAT64',           null),
                if(dq_cast_failed_start_date,       'CAST_FAILED:start_date:DATE',              null),
                if(dq_cast_failed_end_date,         'CAST_FAILED:end_date:DATE',                null),
                if(dq_cast_failed_installments_paid,'CAST_FAILED:installments_paid:INT64',      null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'   as audit_source_system,
    'emi_plans'                 as audit_source_table,
    current_timestamp()         as audit_insert_ts,
    current_timestamp()         as audit_update_ts,
    to_hex(sha256(to_json_string(struct(emi_plan_id, card_id, start_date)))) as audit_record_hash,
    current_timestamp()         as dq_checked_at,
    array_length(dq_error_array) as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
