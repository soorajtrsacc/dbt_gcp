{{ config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'dim_merchant_categories') }}
),

typed as (
    select
        mcc_code,
        category_name,
        category_group
    from bronze
),

quality as (
    select
        *,
        array(
            select x from unnest([
                if(mcc_code is null,       'REQUIRED_NULL:mcc_code',       null),
                if(category_name is null,  'REQUIRED_NULL:category_name',  null),
                if(category_group is null, 'REQUIRED_NULL:category_group', null)
            ]) as x where x is not null
        ) as dq_error_array
    from typed
)

select
    * except (dq_error_array),
    'SYNTHETIC_CARD_PLATFORM'       as audit_source_system,
    'dim_merchant_categories'       as audit_source_table,
    current_timestamp()             as audit_insert_ts,
    current_timestamp()             as audit_update_ts,
    to_hex(sha256(to_json_string(struct(mcc_code, category_name)))) as audit_record_hash,
    current_timestamp()             as dq_checked_at,
    array_length(dq_error_array)    as dq_error_count,
    array_to_string(dq_error_array, '|') as dq_errors,
    if(array_length(dq_error_array) = 0, 'PASS', 'FAIL') as dq_status
from quality
