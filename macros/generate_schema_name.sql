-- Routes each model to its BigQuery dataset based on the folder it lives in
-- (bronze/ → bronze dataset, silver/ → silver dataset, gold/ → gold dataset, ops/ → ops dataset).
-- Seeds with an explicit custom_schema_name (set in dbt_project.yml) also land in that schema.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ node.fqn[1] | trim }}
    {%- endif -%}
{%- endmacro %}
