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

{# Structured Relation for dbt_completions_raw. Used by
   create_dbt_completions_table() to read the existing columns at runtime. #}
{% macro snowflake__completions_relation() %}
  {{ return(api.Relation.create(
       database=var('target_database', target.database),
       schema=var('target_schema', target.schema),
       identifier='dbt_completions_raw')) }}
{% endmacro %}

{# Subtract ``minutes`` from a Snowflake timestamp expression (run-lock TTL). #}
{% macro snowflake__lock_ts_sub(ts_expr, minutes) %}
  DATEADD(minute, -{{ minutes }}, {{ ts_expr }})
{% endmacro %}

{# Coerce any TIMESTAMP_* / DATE / string expression to TIMESTAMP_NTZ — the type
   of the window_*_ts watermark columns. Lets a tenant pass a tz-aware start_ts()/
   end_ts() without a manual wrap. A tz-aware TIMESTAMP_TZ/LTZ has its offset
   dropped (its wall-clock is kept, NOT converted) — pre-wrap the source with
   CONVERT_TIMEZONE('UTC', expr) if it isn't already UTC, so it matches
   BigQuery's UTC interpretation. NTZ inputs cast through unchanged. Inner parens
   guard against operator precedence in ``ts_expr``. #}
{% macro snowflake__to_completions_datetime(ts_expr) %}
  CAST(({{ ts_expr }}) AS TIMESTAMP_NTZ)
{% endmacro %}

{# "today minus ``days``" as a 'YYYY-MM-DD' string, for the view's execution_ts
   window bound. #}
{% macro snowflake__execution_ts_days_ago(days) %}
  TO_VARCHAR(DATEADD(day, -{{ days }}, CURRENT_DATE), 'YYYY-MM-DD')
{% endmacro %}

{# Wraps the tenant-defined ``execution_ts()`` (a Snowflake timestamp/date
   expression) as a YYYY-MM-DD string — the table's execution_ts key. #}
{% macro snowflake__execution_ts_as_datestr() %}
  to_varchar(cast(({{ execution_ts() }}) as date))
{% endmacro %}

{% macro snowflake__completions_col_type(kind) %}
  {%- if kind == 'string' -%}VARCHAR
  {%- elif kind == 'timestamp' -%}TIMESTAMP_NTZ
  {%- elif kind == 'datetime' -%}TIMESTAMP_NTZ
  {%- else -%}{{ exceptions.raise_compiler_error("unknown completions col type: " ~ kind) }}
  {%- endif -%}
{% endmacro %}

{# Snowflake serialises concurrent DML via row/table locks (statements queue
   and block rather than aborting), so the MERGE needs no retry wrapper. #}
{% macro snowflake__with_merge_retry(merge_sql) %}
{{ merge_sql }}
{% endmacro %}
