{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'dim_transaction_types') }}
),

typed as (
    select
        txn_type_id,
        type_name,
        is_credit
    from bronze
),

quality as (
    select
        *,
        array(
            select x from unnest([
                if(txn_type_id is null, 'REQUIRED_NULL:txn_type_id', null),
                if(type_name is null,   'REQUIRED_NULL:type_name',   null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'       as audit_source_system,
    'dim_transaction_types'         as audit_source_table,
    current_timestamp()             as audit_insert_ts,
    current_timestamp()             as audit_update_ts,
    to_hex(sha256(to_json_string(struct(txn_type_id, type_name)))) as audit_record_hash,
    current_timestamp()             as dq_checked_at,
    array_length(dq_error_array)    as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
