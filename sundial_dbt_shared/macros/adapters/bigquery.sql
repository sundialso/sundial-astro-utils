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

{# Structured Relation for dbt_completions_raw — BigQuery maps database->project,
   schema->dataset. Used by create_dbt_completions_table() to read the existing
   columns at runtime. #}
{% macro bigquery__completions_relation() %}
  {{ return(api.Relation.create(
       database=var('target_project'),
       schema=var('target_dataset'),
       identifier='dbt_completions_raw')) }}
{% endmacro %}

{# Subtract ``minutes`` from a BigQuery TIMESTAMP expression (run-lock TTL). #}
{% macro bigquery__lock_ts_sub(ts_expr, minutes) %}
  TIMESTAMP_SUB({{ ts_expr }}, INTERVAL {{ minutes }} MINUTE)
{% endmacro %}

{# Coerce any TIMESTAMP / DATETIME / DATE / STRING expression to DATETIME — the
   type of the window_*_ts watermark columns. BigQuery does NOT implicitly
   convert TIMESTAMP->DATETIME, so a tenant's TIMESTAMP-typed start_ts()/end_ts()
   would otherwise fail to assign. CAST(timestamp AS DATETIME) interprets the
   instant at UTC; DATETIME / DATE / valid STRING inputs cast through unchanged.
   Inner parens guard against operator precedence in ``ts_expr``. #}
{% macro bigquery__to_completions_datetime(ts_expr) %}
  CAST(({{ ts_expr }}) AS DATETIME)
{% endmacro %}

{# "today minus ``days``" as a 'YYYY-MM-DD' string, for the view's execution_ts
   window bound. #}
{% macro bigquery__execution_ts_days_ago(days) %}
  FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL {{ days }} DAY))
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

{# Retry the wrapped statement (a MERGE, or the CREATE OR REPLACE VIEW DDL) on
   ANY error, up to a fixed cap, then re-raise the original. BigQuery scripting
   runs the whole block as one statement; each attempt re-issues the statement
   against a fresh snapshot, so a conflicting txn's error surfaces only after it
   has committed and the retry then succeeds. The retry is a blanket catch (no
   error-message filtering): transient failures (serialization conflicts,
   concurrent-DDL on the view, the per-table mutation rate limit / 429,
   backend/internal errors) all get retried. Deterministic errors (syntax,
   missing column, permission) are also retried but simply fail the same way
   each time and re-raise once the cap is hit. The MERGE/DDL is idempotent by
   key, so re-running it is safe.

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
