-- Reshapes the mapping_input seed for queryable reporting.
-- Shows every Bronze→Silver and Silver→Gold mapping with metadata.
{{ config(materialized='table') }}

with raw as (
    select * from {{ ref('mapping_input') }}
)

select
    source_layer,
    source_object,
    source_columns,
    target_layer,
    target_object,
    target_columns,
    mapping_category,
    primary_key,
    foreign_keys,
    cast_rules,
    dq_rules,
    audit_rules,
    business_purpose,
    output_dataset,
    dbt_model,
    implementation,
    -- Derived flags for monitoring
    case target_layer
        when 'silver' then 1
        when 'gold'   then 2
        else 0
    end                                     as layer_order,
    (implementation = 'Complete')           as is_implemented,
    current_timestamp()                     as report_generated_at
from raw
order by layer_order, source_layer, source_object
