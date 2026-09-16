-- Singular test: fails if any CRITICAL check in gold_data_quality_report has status = FAIL.
-- Run as part of `dbt test` — a result row means the assertion failed.
select
    check_id,
    table_name,
    check_type,
    failed_rows,
    severity,
    check_status
from {{ ref('gold_data_quality_report') }}
where severity = 'CRITICAL'
  and check_status = 'FAIL'
