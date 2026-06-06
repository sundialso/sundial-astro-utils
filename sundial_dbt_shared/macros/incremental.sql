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

{% macro start_ts(timestamp_column, lookback_value, first_timestamp) %}
  {%- if var('backfill_start_ts', none) is not none -%}
    {{ sundial_dbt_shared.incr_cast_ts(var('backfill_start_ts')) }}
  {%- elif is_incremental() -%}
    {%- set lower_bound -%}
      COALESCE(
        ({{ sundial_dbt_shared.read_watermark(this.name) }}),
        (SELECT MAX({{ timestamp_column }}) FROM {{ this }}),
        {{ sundial_dbt_shared.incr_cast_ts(first_timestamp) }}
      )
    {%- endset -%}
    GREATEST(
      {{ sundial_dbt_shared.incr_shift_days(
           sundial_dbt_shared.incr_shift_seconds(lower_bound, 1),
           -1 * (lookback_value | int)) }},
      {{ sundial_dbt_shared.incr_cast_ts(first_timestamp) }}
    )
  {%- else -%}
    {{ sundial_dbt_shared.incr_cast_ts(first_timestamp) }}
  {%- endif -%}
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
