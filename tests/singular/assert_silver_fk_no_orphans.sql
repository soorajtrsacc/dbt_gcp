-- Singular test: fails if any FK relationship in silver_relationship_dq_results has orphaned rows.
select
    child_table,
    fk_column,
    parent_table,
    pk_column,
    orphaned_count,
    fk_status
from {{ ref('silver_relationship_dq_results') }}
where fk_status = 'FAIL'
