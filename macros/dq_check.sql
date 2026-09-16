-- Reusable macro that runs PK uniqueness, NOT NULL, and FK referential integrity
-- checks against a given model and returns a one-row summary per check.
-- Usage: {{ dq_check_pk('gold_customer_360', 'customer_id') }}

{% macro dq_check_pk(model_name, pk_column) %}
select
    '{{ model_name }}_PK_UNIQUE'                    as check_id,
    '{{ model_name }}'                              as table_name,
    'PK_UNIQUE'                                     as check_type,
    '{{ pk_column }}'                               as checked_columns,
    count(*)                                        as total_rows,
    count(distinct {{ pk_column }})                 as passed_rows,
    count(*) - count(distinct {{ pk_column }})      as failed_rows,
    round(
        safe_divide(
            count(distinct {{ pk_column }}),
            count(*)
        ) * 100, 4
    )                                               as pass_percentage,
    if(count(*) = count(distinct {{ pk_column }}), 'PASS', 'FAIL') as check_status,
    'CRITICAL'                                      as severity
from {{ ref(model_name) }}
{% endmacro %}


{% macro dq_check_not_null(model_name, column_name) %}
select
    '{{ model_name }}_NN_{{ column_name }}'                         as check_id,
    '{{ model_name }}'                                              as table_name,
    'NOT_NULL'                                                      as check_type,
    '{{ column_name }}'                                             as checked_columns,
    count(*)                                                        as total_rows,
    countif({{ column_name }} is not null)                          as passed_rows,
    countif({{ column_name }} is null)                              as failed_rows,
    round(
        safe_divide(countif({{ column_name }} is not null), count(*)) * 100, 4
    )                                                               as pass_percentage,
    if(countif({{ column_name }} is null) = 0, 'PASS', 'FAIL')     as check_status,
    'HIGH'                                                          as severity
from {{ ref(model_name) }}
{% endmacro %}


{% macro dq_check_fk(child_model, fk_column, parent_model, pk_column) %}
select
    '{{ child_model }}_FK_{{ fk_column }}'                          as check_id,
    '{{ child_model }}'                                             as table_name,
    'FK_REFERENTIAL_INTEGRITY'                                      as check_type,
    '{{ fk_column }} → {{ parent_model }}.{{ pk_column }}'          as checked_columns,
    count(*)                                                        as total_rows,
    countif(p.{{ pk_column }} is not null)                          as passed_rows,
    countif(p.{{ pk_column }} is null)                              as failed_rows,
    round(
        safe_divide(countif(p.{{ pk_column }} is not null), count(*)) * 100, 4
    )                                                               as pass_percentage,
    if(countif(p.{{ pk_column }} is null) = 0, 'PASS', 'FAIL')     as check_status,
    'HIGH'                                                          as severity
from {{ ref(child_model) }} c
left join {{ ref(parent_model) }} p
    on c.{{ fk_column }} = p.{{ pk_column }}
{% endmacro %}
