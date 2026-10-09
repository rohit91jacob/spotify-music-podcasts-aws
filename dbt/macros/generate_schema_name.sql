{#- Use the configured schema verbatim (staging, intermediate, marts) instead of prefixing it
    with the target schema. On Athena the Glue databases are project-prefixed
    (musicpods_staging, ...), matching infra/modules/catalog. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}
    {%- elif target.type == 'athena' -%}{{ project_name }}_{{ custom_schema_name | trim }}
    {%- else -%}{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
