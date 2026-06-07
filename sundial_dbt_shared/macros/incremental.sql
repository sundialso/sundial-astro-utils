{# ------------------------------------------------------------------ #}
{#  Common incremental-window macros — start_ts / end_ts / execution_ts #}
{#  shared across ALL tenants, BigQuery and Snowflake.                  #}
{#                                                                      #}
{#  Why this exists: each tenant used to carry its own copy of these    #}
{#  macros (BigQuery tenants inline in sundial_macros.sql, Snowflake    #}
{#  tenants as individual start_ts.sql / end_ts.sql / execution_ts.sql),#}
{#  which drifted (var names, missing fallbacks, the -1s, tz handling). #}
{#  The window LOGIC now lives here ONCE; only the dialect differences  #}
{#  (DATETIME vs TIMESTAMP_NTZ, DATETIME_ADD vs DATEADD, UTC "now") are  #}
{#  isolated in the adapter-dispatched incr_* primitives.               #}
{#                                                                      #}
{#  Semantics (canonical):                                              #}
{#    - LOWER bound (start_ts) = the dbt_completions watermark           #}
{#      (read_watermark = MAX(end_ts) of the model's last COMPLETE run). #}
{#      First-run fallback (no watermark yet) = MAX(timestamp_column)    #}
{#      already in the target, so a fresh watermark never full-loads;    #}
{#      final fallback = first_timestamp. Resume from watermark + 1s     #}
{#      (that second was already processed), then back off `lookback`    #}
{#      days for late data, floored at first_timestamp.                  #}
{#    - UPPER bound (end_ts) = execution_ts - (lag days) - 1s, and it is #}
{#      recorded via record_window_end → becomes the next run's          #}
{#      watermark. (start_ts does NOT record — end-only, to minimise     #}
{#      writes to the completions table.)                               #}
{#    - backfill overrides: vars `backfill_start_ts` / `backfill_end_ts`.#}
{#                                                                      #}
{#  Tenant requirement: the dbt_completions lock/watermark wiring must   #}
{#  be in place (create_dbt_completions_table, acquire_run_lock pre-hook,#}
{#  record_window_end) before a model uses these — read_watermark reads  #}
{#  dbt_completions_raw. On the very first run the watermark is empty and #}
{#  the MAX(timestamp_column) fallback resumes from existing data.       #}
{# ------------------------------------------------------------------ #}

{# ---- dialect primitives (adapter-dispatched) ---- #}
{% macro incr_now_ts() %}
  {{ return(adapter.dispatch('incr_now_ts', 'sundial_dbt_shared')()) }}
{% endmacro %}

{% macro incr_cast_ts(literal) %}
  {{ return(adapter.dispatch('incr_cast_ts', 'sundial_dbt_shared')(literal)) }}
{% endmacro %}

{% macro incr_shift_days(ts_expr, days) %}
  {{ return(adapter.dispatch('incr_shift_days', 'sundial_dbt_shared')(ts_expr, days)) }}
{% endmacro %}

{% macro incr_shift_seconds(ts_expr, seconds) %}
  {{ return(adapter.dispatch('incr_shift_seconds', 'sundial_dbt_shared')(ts_expr, seconds)) }}
{% endmacro %}

{# ---- shared window macros ---- #}

{% macro execution_ts() %}
  {%- if var('execution_ts', none) is not none -%}
    {{ sundial_dbt_shared.incr_cast_ts(var('execution_ts')) }}
  {%- else -%}
    {{ sundial_dbt_shared.incr_now_ts() }}
  {%- endif -%}
{% endmacro %}

{# start_ts resolves the watermark lower bound to a LITERAL at compile time via
   run_query, then injects it as a constant. This is required for BigQuery tables
   with require_partition_filter: BigQuery only does partition elimination on
   constants / scripting vars / CURRENT_* — NOT on a subquery. The watermark
   (read_watermark) and the MAX(timestamp_column) fallback are subqueries, so
   embedding them inline makes the partition filter non-prunable and the warehouse
   rejects the query ("Cannot query ... without a filter ... for partition
   elimination"). Resolving to a literal keeps the window prunable on both
   warehouses. The result is memoised on the model node so the N start_ts() calls
   in one model render issue ONE resolving query, not N. #}
{% macro start_ts(timestamp_column, lookback_value, first_timestamp) %}
  {%- if var('backfill_start_ts', none) is not none -%}
    {# Validate backfill bounds ONLY on a partial-backfill run (this branch is
       reached only when backfill_start_ts is set), so normal runs do no extra
       work at all. validate_partial_backfill itself no-ops unless BOTH backfill
       vars are set, and it raises on an invalid range / one beyond the watermark.
       Memoised per model so the N start_ts() calls validate once. #}
    {%- if execute and (model is none or model.get('__sundial_backfill_validated__') is none) -%}
      {%- if model is not none -%}{%- do model.update({'__sundial_backfill_validated__': true}) -%}{%- endif -%}
      {%- do sundial_dbt_shared.validate_partial_backfill(this.name, first_timestamp) -%}
    {%- endif -%}
    {{ sundial_dbt_shared.incr_cast_ts(var('backfill_start_ts')) }}
  {%- elif is_incremental() -%}
    {{ sundial_dbt_shared.incr_cast_ts(sundial_dbt_shared._resolve_start_ts(timestamp_column, lookback_value, first_timestamp)) }}
  {%- else -%}
    {{ sundial_dbt_shared.incr_cast_ts(first_timestamp) }}
  {%- endif -%}
{% endmacro %}

{# Resolve the incremental lower bound to a scalar literal (string). Runs the
   watermark/MAX/first COALESCE + (+1s, -lookback, GREATEST(first)) as ONE query
   and returns the value, memoised per (model, timestamp_column, lookback) on the
   model node. During parse (execute is false) there is no warehouse, so it falls
   back to first_timestamp — the real value is resolved when the model runs. #}
{% macro _resolve_start_ts(timestamp_column, lookback_value, first_timestamp) %}
  {%- if not execute -%}
    {{- return(first_timestamp) -}}
  {%- endif -%}
  {%- set memo_key = '__sundial_start_ts__' ~ timestamp_column ~ '__' ~ (lookback_value | int) -%}
  {%- set cached = model.get(memo_key) if model is not none else none -%}
  {%- if cached is none -%}
    {%- set lower_bound -%}
      COALESCE(
        ({{ sundial_dbt_shared.read_watermark(this.name) }}),
        (SELECT MAX({{ timestamp_column }}) FROM {{ this }}),
        {{ sundial_dbt_shared.incr_cast_ts(first_timestamp) }}
      )
    {%- endset -%}
    {%- set resolve_sql -%}
      SELECT GREATEST(
        {{ sundial_dbt_shared.incr_shift_days(
             sundial_dbt_shared.incr_shift_seconds(lower_bound, 1),
             -1 * (lookback_value | int)) }},
        {{ sundial_dbt_shared.incr_cast_ts(first_timestamp) }}
      ) AS s
    {%- endset -%}
    {%- set res = run_query(resolve_sql) -%}
    {%- set cached = (res.rows[0][0] | string) if (res is not none and res.rows | length > 0 and res.rows[0][0] is not none) else first_timestamp -%}
    {%- if model is not none -%}{%- do model.update({memo_key: cached}) -%}{%- endif -%}
    {# Log the resolved lower bound into the start_ts column (one MERGE per model,
       since this block runs once per (model, args) under the memo guard). #}
    {%- do sundial_dbt_shared.record_window_start_value(cached) -%}
  {%- endif -%}
  {{- return(cached) -}}
{% endmacro %}

{% macro end_ts(lag_value) %}
  {%- set expr -%}
    {%- if var('backfill_end_ts', none) is not none -%}
      {{ sundial_dbt_shared.incr_cast_ts(var('backfill_end_ts')) }}
    {%- elif (lag_value | int) > 0 -%}
      {{ sundial_dbt_shared.incr_shift_seconds(
           sundial_dbt_shared.incr_shift_days(sundial_dbt_shared.execution_ts(), -1 * (lag_value | int)),
           -1) }}
    {%- else -%}
      {{ sundial_dbt_shared.incr_shift_seconds(sundial_dbt_shared.execution_ts(), -1) }}
    {%- endif -%}
  {%- endset -%}
  {%- do sundial_dbt_shared.record_window_end(expr) -%}
  {{ expr }}
{% endmacro %}
