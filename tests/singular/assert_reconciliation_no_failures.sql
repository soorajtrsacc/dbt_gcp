-- Singular test: fails if any reconciliation check reports FAIL status.
-- Both Bronze→Silver and Silver→Gold rows are covered.
select
    reconciliation_id,
    reconciliation_scope,
    source_table,
    target_table,
    source_count,
    intermediate_count,
    target_count,
    missing_in_intermediate,
    missing_in_target,
    duplicate_intermediate_keys,
    duplicate_target_keys,
    reconciliation_status
from {{ ref('medallion_reconciliation_report') }}
where reconciliation_status = 'FAIL'
