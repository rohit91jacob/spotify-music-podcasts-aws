{#- Small cross-database helpers: every model compiles for DuckDB (local) and Athena (Trino). -#}

{% macro percentile(expr, p) -%}
    {{ return(adapter.dispatch('percentile', 'musicpods')(expr, p)) }}
{%- endmacro %}

{% macro default__percentile(expr, p) -%}
    quantile_cont({{ expr }}, {{ p }})
{%- endmacro %}

{% macro athena__percentile(expr, p) -%}
    approx_percentile({{ expr }}, {{ p }})
{%- endmacro %}

{#- ISO-8601 'YYYY-MM-DD' strings to DATE; partial MusicBrainz dates ('2019', '2019-05') give NULL. -#}
{% macro iso_date(expr) -%}
    case when length({{ expr }}) = 10 then {{ dbt.safe_cast(expr, api.Column.translate_type('date')) }} end
{%- endmacro %}

{% macro ratio(numerator, denominator) -%}
    cast({{ numerator }} as double) / nullif(cast({{ denominator }} as double), 0)
{%- endmacro %}

{% macro seconds_between(start_ts, end_ts) -%}
    {{ dbt.datediff(start_ts, end_ts, 'second') }}
{%- endmacro %}

{% macro days_between(start_date, end_date) -%}
    {{ dbt.datediff(start_date, end_date, 'day') }}
{%- endmacro %}
