{# ------------------------------------------------------------------ #}
{#  Snowflake implementations of the dbt_completions primitives.       #}
{#  Resolved via ``adapter.dispatch`` when ``target.type == 'snowflake'``.#}
{#  See ``../dbt_completions.sql`` for the dispatchers + orchestration. #}
{# ------------------------------------------------------------------ #}

{# database.schema.table. The ``target_database``/``target_schema`` vars win;
   otherwise the database/schema from the active connection/profile
   (``target.database`` / ``target.schema``). #}
{% macro snowflake__dbt_completions_table() %}
  {{ var('target_database', target.database) }}.{{ var('target_schema', target.schema) }}.dbt_completions_raw
{% endmacro %}

{% macro snowflake__dbt_completions_view() %}
  {{ var('target_database', target.database) }}.{{ var('target_schema', target.schema) }}.dbt_completions
{% endmacro %}

{# Wraps the tenant-defined ``execution_ts()`` (a Snowflake timestamp/date
   expression) as a YYYY-MM-DD string — the table's execution_ts key. #}
{% macro snowflake__execution_ts_as_datestr() %}
  to_varchar(cast(({{ execution_ts() }}) as date))
{% endmacro %}

{% macro snowflake__completions_col_type(kind) %}
  {%- if kind == 'string' -%}VARCHAR
  {%- elif kind == 'timestamp' -%}TIMESTAMP_NTZ
  {%- else -%}{{ exceptions.raise_compiler_error("unknown completions col type: " ~ kind) }}
  {%- endif -%}
{% endmacro %}

{# Snowflake serialises concurrent DML via row/table locks (statements queue
   and block rather than aborting), so the MERGE needs no retry wrapper. #}
{% macro snowflake__with_merge_retry(merge_sql) %}
{{ merge_sql }}
{% endmacro %}
