{# ------------------------------------------------------------------ #}
{#  BigQuery implementations of the dbt_completions primitives.        #}
{#  Resolved via ``adapter.dispatch`` when ``target.type == 'bigquery'``.#}
{#  See ``../dbt_completions.sql`` for the dispatchers + orchestration. #}
{# ------------------------------------------------------------------ #}

{% macro bigquery__dbt_completions_table() %}
  `{{ var('target_project') }}.{{ var('target_dataset') }}.dbt_completions_raw`
{% endmacro %}

{% macro bigquery__dbt_completions_view() %}
  `{{ var('target_project') }}.{{ var('target_dataset') }}.dbt_completions`
{% endmacro %}

{# Subtract ``minutes`` from a BigQuery TIMESTAMP expression (run-lock TTL). #}
{% macro bigquery__lock_ts_sub(ts_expr, minutes) %}
  TIMESTAMP_SUB({{ ts_expr }}, INTERVAL {{ minutes }} MINUTE)
{% endmacro %}

{# Wraps the tenant-defined ``execution_ts()`` (a BigQuery datetime/date
   expression) as a YYYY-MM-DD string — the table's execution_ts key. #}
{% macro bigquery__execution_ts_as_datestr() %}
  cast(cast(({{ execution_ts() }}) as date) as string)
{% endmacro %}

{% macro bigquery__completions_col_type(kind) %}
  {%- if kind == 'string' -%}STRING
  {%- elif kind == 'timestamp' -%}TIMESTAMP
  {%- elif kind == 'datetime' -%}DATETIME
  {%- else -%}{{ exceptions.raise_compiler_error("unknown completions col type: " ~ kind) }}
  {%- endif -%}
{% endmacro %}

{# Retry the wrapped statement (MERGE, or the CREATE OR REPLACE VIEW DDL) on
   BigQuery's serialization / concurrent-update error. BigQuery scripting runs
   the whole block as one statement; each attempt re-issues the statement against
   a fresh snapshot, so by the time the conflicting txn's error surfaces it has
   already committed and the retry succeeds. Non-concurrency errors and the
   exhausted-attempts case re-raise the original error unchanged. BigQuery has
   no SLEEP, so retries are immediate (no backoff). The '%concurrent%' match
   covers both "concurrent update" (MERGE) and concurrent-DDL view replaces. #}
{% macro bigquery__with_merge_retry(merge_sql) %}
BEGIN
  DECLARE _attempt INT64 DEFAULT 0;
  retry_merge: LOOP
    BEGIN
      {{ merge_sql }};
      LEAVE retry_merge;
    EXCEPTION WHEN ERROR THEN
      IF _attempt < 5 AND (@@error.message LIKE '%serialize%' OR @@error.message LIKE '%concurrent%') THEN
        SET _attempt = _attempt + 1;
      ELSE
        RAISE;
      END IF;
    END;
  END LOOP;
END
{% endmacro %}
