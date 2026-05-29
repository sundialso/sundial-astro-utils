{# ------------------------------------------------------------------ #}
{#  BigQuery implementations of the dbt_completions primitives.        #}
{#  Resolved via ``adapter.dispatch`` when ``target.type == 'bigquery'``.#}
{#  See ``../dbt_completions.sql`` for the dispatchers + orchestration. #}
{# ------------------------------------------------------------------ #}

{% macro bigquery__dbt_completions_table() %}
  `{{ var('target_project') }}.{{ var('target_dataset') }}.dbt_completions`
{% endmacro %}

{# Wraps the tenant-defined ``execution_ts()`` (a BigQuery datetime/date
   expression) as a YYYY-MM-DD string — the table's execution_ts key. #}
{% macro bigquery__execution_ts_as_datestr() %}
  cast(cast(({{ execution_ts() }}) as date) as string)
{% endmacro %}

{% macro bigquery__completions_col_type(kind) %}
  {%- if kind == 'string' -%}STRING
  {%- elif kind == 'timestamp' -%}TIMESTAMP
  {%- else -%}{{ exceptions.raise_compiler_error("unknown completions col type: " ~ kind) }}
  {%- endif -%}
{% endmacro %}
