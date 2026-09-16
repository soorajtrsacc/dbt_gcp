-- Reconciles row counts and key integrity from Bronze to Silver for one table.
{% macro reconcile_bronze_to_silver(table_name, key_column) %}
select
    'B2S::{{ table_name }}'                                         as reconciliation_id,
    'BRONZE_TO_SILVER'                                              as reconciliation_scope,
    '{{ table_name }}'                                              as source_table,
    'silver_{{ table_name }}'                                       as target_table,
    (select count(*) from {{ source('bronze', table_name) }})       as source_count,
    (select count(*) from {{ ref('silver_' ~ table_name) }})        as intermediate_count,
    cast(null as int64)                                             as target_count,
    (
        select count(*)
        from {{ source('bronze', table_name) }} b
        left join {{ ref('silver_' ~ table_name) }} s
            on b.{{ key_column }} = s.{{ key_column }}
        where s.{{ key_column }} is null
    )                                                               as missing_in_intermediate,
    cast(null as int64)                                             as missing_in_target,
    (
        select count(*) - count(distinct {{ key_column }})
        from {{ ref('silver_' ~ table_name) }}
    )                                                               as duplicate_intermediate_keys,
    cast(null as int64)                                             as duplicate_target_keys,
    current_timestamp()                                             as reconciliation_ts
{% endmacro %}


-- Reconciles Bronze → Silver → Gold for one gold entity.
{% macro reconcile_silver_to_gold(bronze_table, bronze_key, silver_table, silver_key, gold_table, gold_key) %}
select
    'S2G::{{ gold_table }}'                                         as reconciliation_id,
    'SILVER_TO_GOLD'                                                as reconciliation_scope,
    '{{ silver_table }}'                                            as source_table,
    '{{ gold_table }}'                                              as target_table,
    (select count(*) from {{ source('bronze', bronze_table) }})     as source_count,
    (select count(*) from {{ ref(silver_table) }})                  as intermediate_count,
    (select count(*) from {{ ref(gold_table) }})                    as target_count,
    cast(null as int64)                                             as missing_in_intermediate,
    (
        select count(*)
        from {{ ref(silver_table) }} s
        left join {{ ref(gold_table) }} g
            on s.{{ silver_key }} = g.{{ gold_key }}
        where g.{{ gold_key }} is null
    )                                                               as missing_in_target,
    (
        select count(*) - count(distinct {{ silver_key }})
        from {{ ref(silver_table) }}
    )                                                               as duplicate_intermediate_keys,
    (
        select count(*) - count(distinct {{ gold_key }})
        from {{ ref(gold_table) }}
    )                                                               as duplicate_target_keys,
    current_timestamp()                                             as reconciliation_ts
{% endmacro %}
