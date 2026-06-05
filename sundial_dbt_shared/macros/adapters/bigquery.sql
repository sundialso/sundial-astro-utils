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
  {%- else -%}{{ exceptions.raise_compiler_error("unknown completions col type: " ~ kind) }}
  {%- endif -%}
{% endmacro %}

{# Retry the MERGE on ANY error, up to a fixed cap, then re-raise the original.
   BigQuery scripting runs the whole block as one statement; each attempt
   re-issues the MERGE against a fresh snapshot, so a conflicting txn's error
   surfaces only after it has committed and the retry then succeeds. The retry is
   a blanket catch (no error-message filtering): transient failures (serialization
   conflicts, the per-table mutation rate limit / 429, backend/internal errors)
   all get retried. Deterministic errors (syntax, missing column, permission) are
   also retried but simply fail the same way each time and re-raise once the cap
   is hit. The MERGE is idempotent by key, so re-running it is safe.

   CAVEAT: BigQuery scripting has no SLEEP, so retries are immediate (no backoff).
   That is weak against the 10s rate-limit window — pair with reduced concurrency
   (dbt threads / Airflow max_active_tasks) and/or Airflow task retry_delay. #}
{% macro bigquery__with_merge_retry(merge_sql) %}
BEGIN
  DECLARE _attempt INT64 DEFAULT 0;
  retry_merge: LOOP
    BEGIN
      {{ merge_sql }};
      LEAVE retry_merge;
    EXCEPTION WHEN ERROR THEN
      IF _attempt < 5 THEN
        SET _attempt = _attempt + 1;
      ELSE
        RAISE;
      END IF;
    END;
  END LOOP;
END
{% endmacro %}
