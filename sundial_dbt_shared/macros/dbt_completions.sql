{# ------------------------------------------------------------------ #}
{#  Tracks per-model, per-run completion status in the warehouse.      #}
{#                                                                    #}
{#  Warehouse support                                                 #}
{#  -----------------                                                 #}
{#  The orchestration macros below (create / log_model_status /       #}
{#  log_run_results) are warehouse-agnostic. The three pieces that    #}
{#  actually differ per warehouse are isolated behind dispatched      #}
{#  primitives, implemented once per adapter under                    #}
{#  ``macros/adapters/<warehouse>.sql``:                              #}
{#                                                                    #}
{#    - dbt_completions_table()      table FQN + identifier quoting   #}
{#    - execution_ts_as_datestr()    date-string cast of the run ts   #}
{#    - completions_col_type(kind)   DDL column types                 #}
{#                                                                    #}
{#  Adding a warehouse = add ``macros/adapters/<warehouse>.sql`` with  #}
{#  the ``<warehouse>__*`` implementations of those three. Nothing in  #}
{#  this file changes. (Mirrors the Python ``WarehouseAdapter``.)      #}
{#                                                                    #}
{#  Tenant requirements:                                              #}
{#    - BigQuery: vars `target_project` and `target_dataset` must be  #}
{#      set. Snowflake: var `target_schema` (and optionally           #}
{#      `target_database`; otherwise `target.database` is used).      #}
{#    - The tenant must define an `execution_ts()` macro returning a  #}
{#      timestamp/date expression VALID FOR THEIR OWN WAREHOUSE (each  #}
{#      tenant targets a single warehouse). Sundial only wraps it     #}
{#      with the per-warehouse date-string cast. Unqualified calls    #}
{#      below resolve to the tenant's definition via dbt's lookup.    #}
{#                                                                    #}
{#  Storage layout:                                                   #}
{#    - dbt_completions_raw : table the hooks MERGE into; holds every  #}
{#      status transition (several rows per model per run).            #}
{#    - dbt_completions     : view created on-run-start; one row per   #}
{#      (model, execution_ts) showing that run's FINAL status.         #}
{#                                                                    #}
{#  Wire-up in tenant dbt_project.yml:                                #}
{#    on-run-start:                                                   #}
{#      - "{{ sundial_dbt_shared.create_dbt_completions_table() }}"   #}
{#      - "{{ sundial_dbt_shared.create_dbt_completions_view() }}"    #}
{#    on-run-end:                                                     #}
{#      - "{{ sundial_dbt_shared.log_run_results() }}"                #}
{#    models:                                                         #}
{#      <project>:                                                    #}
{#        +pre-hook:  ["{{ sundial_dbt_shared.log_model_status('started') }}"]   #}
{#        +post-hook: ["{{ sundial_dbt_shared.log_model_status('succeeded') }}"] #}
{# ------------------------------------------------------------------ #}

{# ------------------------------------------------------------------ #}
{#  Dispatched primitives — resolve to ``<adapter>__<name>`` in        #}
{#  ``macros/adapters/`` based on ``target.type`` (bigquery/snowflake).#}
{# ------------------------------------------------------------------ #}
{% macro dbt_completions_table() %}
  {{ return(adapter.dispatch('dbt_completions_table', 'sundial_dbt_shared')()) }}
{% endmacro %}

{% macro dbt_completions_view() %}
  {{ return(adapter.dispatch('dbt_completions_view', 'sundial_dbt_shared')()) }}
{% endmacro %}

{% macro execution_ts_as_datestr() %}
  {{ return(adapter.dispatch('execution_ts_as_datestr', 'sundial_dbt_shared')()) }}
{% endmacro %}

{% macro completions_col_type(kind) %}
  {{ return(adapter.dispatch('completions_col_type', 'sundial_dbt_shared')(kind)) }}
{% endmacro %}

{# Wraps a MERGE statement in warehouse-specific retry logic. BigQuery uses
   snapshot isolation: concurrent MERGEs into the shared dbt_completions table
   abort with "Could not serialize access ... due to concurrent update" once
   parallelism exceeds what it will queue. The BigQuery impl re-runs the MERGE
   (a fresh snapshot each attempt) on that error; Snowflake serialises DML via
   locks, so its impl emits the MERGE unchanged. #}
{% macro with_merge_retry(merge_sql) %}
  {{ return(adapter.dispatch('with_merge_retry', 'sundial_dbt_shared')(merge_sql)) }}
{% endmacro %}

{# Subtract ``minutes`` from a warehouse timestamp expression — the run-lock
   TTL/heartbeat staleness check. #}
{% macro lock_ts_sub(ts_expr, minutes) %}
  {{ return(adapter.dispatch('lock_ts_sub', 'sundial_dbt_shared')(ts_expr, minutes)) }}
{% endmacro %}

{% macro create_dbt_completions_table() %}
  CREATE TABLE IF NOT EXISTS {{ sundial_dbt_shared.dbt_completions_table() }} (
    model_name          {{ sundial_dbt_shared.completions_col_type('string') }},
    execution_ts        {{ sundial_dbt_shared.completions_col_type('string') }},
    status              {{ sundial_dbt_shared.completions_col_type('string') }},
    updated_at          {{ sundial_dbt_shared.completions_col_type('timestamp') }},
    run_group_id        {{ sundial_dbt_shared.completions_col_type('string') }},
    chunk_key           {{ sundial_dbt_shared.completions_col_type('string') }},
    heartbeat_at        {{ sundial_dbt_shared.completions_col_type('timestamp') }},
    window_start_ts     {{ sundial_dbt_shared.completions_col_type('datetime') }},
    window_end_ts       {{ sundial_dbt_shared.completions_col_type('datetime') }},
    window_committed_at {{ sundial_dbt_shared.completions_col_type('timestamp') }}
  )
{% endmacro %}

{# Additive column add for tables created before run grouping / the lock /
   the incremental watermark. Wire into on-run-start AFTER
   create_dbt_completions_table(). BigQuery and Snowflake both support
   ADD COLUMN IF NOT EXISTS. #}
{% macro ensure_completions_columns() %}
  ALTER TABLE {{ sundial_dbt_shared.dbt_completions_table() }}
    ADD COLUMN IF NOT EXISTS run_group_id        {{ sundial_dbt_shared.completions_col_type('string') }},
    ADD COLUMN IF NOT EXISTS chunk_key           {{ sundial_dbt_shared.completions_col_type('string') }},
    ADD COLUMN IF NOT EXISTS heartbeat_at        {{ sundial_dbt_shared.completions_col_type('timestamp') }},
    ADD COLUMN IF NOT EXISTS window_start_ts     {{ sundial_dbt_shared.completions_col_type('datetime') }},
    ADD COLUMN IF NOT EXISTS window_end_ts       {{ sundial_dbt_shared.completions_col_type('datetime') }},
    ADD COLUMN IF NOT EXISTS window_committed_at {{ sundial_dbt_shared.completions_col_type('timestamp') }}
{% endmacro %}

{#
  create_dbt_completions_view — on-run-start companion to the raw table.

  The hooks MERGE every status transition (started -> succeeded/failed) into
  dbt_completions_raw, so that table holds several rows per model per run. This
  view collapses each run to its FINAL state: one row per (model_name,
  execution_ts), picking the most-recently-written status (the terminal row, as
  on-run-end always writes last). Run history is preserved across execution_ts;
  query the latest run by ordering on execution_ts.

  Standard SQL — the ROW_NUMBER subquery runs unchanged on BigQuery and
  Snowflake; only the object names come from the dispatched FQN primitives.

  Created with IF NOT EXISTS (not OR REPLACE) so the parallel on-run-start
  invocations don't issue conflicting concurrent DDL on the same view —
  mirroring create_dbt_completions_table. A change to the view definition
  therefore requires dropping the view once so the next run recreates it.
#}
{% macro create_dbt_completions_view() %}
  CREATE VIEW IF NOT EXISTS {{ sundial_dbt_shared.dbt_completions_view() }} AS
  SELECT model_name, execution_ts, status, updated_at
  FROM (
    SELECT
      model_name,
      execution_ts,
      status,
      updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY model_name, execution_ts
        ORDER BY updated_at DESC
      ) AS _rn
    FROM {{ sundial_dbt_shared.dbt_completions_table() }}
    -- 'locked_out' (a run that backed off) and 'crashed' (a dead run reclaimed
    -- after TTL) are "this run didn't produce data" markers — exclude them so
    -- the concurrent data-producing run's terminal row is what sundial reads.
    WHERE status NOT IN ('locked_out', 'crashed')
  )
  WHERE _rn = 1
{% endmacro %}

{% macro model_has_tests(model_unique_id) %}
  {% if execute %}
    {% for _, node in graph.nodes.items() %}
      {% if node.resource_type == 'test' and model_unique_id in node.depends_on.nodes %}
        {{ return(true) }}
      {% endif %}
    {% endfor %}
  {% endif %}
  {{ return(false) }}
{% endmacro %}

{% macro log_model_status(status) %}
  {% if status == 'succeeded' and sundial_dbt_shared.model_has_tests(model.unique_id) %}
    SELECT 1 AS deferred_to_on_run_end
  {% else %}
    {%- set rg = var('run_group_id', invocation_id) -%}
    {%- set ck = var('chunk_key', 'full') -%}
    {%- set merge_sql -%}
    MERGE INTO {{ sundial_dbt_shared.dbt_completions_table() }} T
    USING (
      SELECT
        '{{ this.name }}' AS model_name,
        {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
        '{{ status }}' AS status,
        '{{ rg }}' AS run_group_id,
        '{{ ck }}' AS chunk_key,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.model_name = S.model_name AND T.execution_ts = S.execution_ts AND T.status = S.status
       AND T.run_group_id = S.run_group_id AND T.chunk_key = S.chunk_key
    WHEN MATCHED THEN UPDATE SET
      updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, updated_at)
      VALUES (S.model_name, S.execution_ts, S.status, S.run_group_id, S.chunk_key, S.updated_at)
    {%- endset -%}
    {{ sundial_dbt_shared.with_merge_retry(merge_sql) }}
  {% endif %}
{% endmacro %}

{#
  log_run_results — on-run-end aggregator.

  Emits one terminal row ('succeeded' or 'failed') per model touched in this
  invocation. "Touched" = appeared as a built model and/or had at least one
  test in this run's results.

  Status rules (match the goals stated in the macro's spec):
    - started   : written by pre-hook only.
    - failed    : materialization failed (any non-'success' build status,
                  including 'error' and 'skipped') OR a test reported
                  'fail'/'error' (fatal severity).
    - succeeded : materialization succeeded this invocation, OR it's a
                  tests-only invocation and no test was fatal. A 'warn'
                  test result is non-fatal in dbt and is treated as success
                  (handles goal: "test fails but dbt Cloud / Astro marks
                  the run succeeded → succeeded").

  Works the same way regardless of orchestrator:
    - dbt build (dbt Cloud, local): models + tests in one invocation.
    - dbt run only (Cosmos "run" task, local): models only.
    - dbt test only (Cosmos "test" task, local): tests only.
  In every case, on-run-end writes a terminal row, so the table never
  stays at 'started' for a model that dbt actually finished.
#}
{% macro log_run_results() %}
  {% if execute and results %}
    {# 1) Group test outcomes by parent model. #}
    {% set tests_by_model = {} %}
    {% for res in results if res.node.resource_type == 'test' %}
      {% for parent in res.node.depends_on.nodes if parent.startswith('model.') %}
        {% set parent_name = parent.split('.')[-1] %}
        {% if parent_name not in tests_by_model %}
          {% do tests_by_model.update({parent_name: []}) %}
        {% endif %}
        {% do tests_by_model[parent_name].append(res.status) %}
      {% endfor %}
    {% endfor %}

    {# 2) Build outcomes keyed by model name. #}
    {% set model_build = {} %}
    {% for res in results if res.node.resource_type == 'model' %}
      {% do model_build.update({res.node.name: res.status}) %}
    {% endfor %}

    {# 3) Union of models built and/or tested this invocation. #}
    {% set touched = [] %}
    {% for name in model_build.keys() %}
      {% do touched.append(name) %}
    {% endfor %}
    {% for name in tests_by_model.keys() %}
      {% if name not in touched %}
        {% do touched.append(name) %}
      {% endif %}
    {% endfor %}

    {# 4) Pick a terminal status per touched model. #}
    {% set rows = [] %}
    {% for name in touched %}
      {% set build_status = model_build.get(name) %}
      {% set test_statuses = tests_by_model.get(name, []) %}
      {% set has_failing_test = ('fail' in test_statuses) or ('error' in test_statuses) %}
      {% set tests_ran = test_statuses | length > 0 %}
      {% set build_failed = build_status is not none and build_status != 'success' %}
      {% set build_ok = build_status == 'success' %}

      {% if build_failed or has_failing_test %}
        {% do rows.append({'name': name, 'status': 'failed'}) %}
      {% elif build_ok or tests_ran %}
        {% do rows.append({'name': name, 'status': 'succeeded'}) %}
      {% endif %}
    {% endfor %}

    {# Exclude models that backed off behind another run's lock this run: they
       carry a 'locked_out' row and never actually ran, so they must NOT be
       overwritten with failed/succeeded. #}
    {% set _rg = var('run_group_id', invocation_id) %}
    {% set _locked = run_query(
         "SELECT DISTINCT model_name FROM " ~ sundial_dbt_shared.dbt_completions_table()
         ~ " WHERE status = 'locked_out' AND run_group_id = '" ~ _rg ~ "'") %}
    {% set _locked_names = _locked.columns[0].values() | list if _locked is not none and _locked.columns | length > 0 else [] %}
    {% set rows = rows | rejectattr('name', 'in', _locked_names) | list %}

    {% if rows | length > 0 %}
      {%- set merge_sql -%}
      MERGE INTO {{ sundial_dbt_shared.dbt_completions_table() }} T
      USING (
        {% for row in rows %}
          SELECT
            '{{ row.name }}' AS model_name,
            {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
            '{{ row.status }}' AS status,
            '{{ _rg }}' AS run_group_id,
            '{{ var('chunk_key', 'full') }}' AS chunk_key,
            CURRENT_TIMESTAMP() AS updated_at
          {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
      ) S
      ON T.model_name = S.model_name AND T.execution_ts = S.execution_ts AND T.status = S.status
         AND T.run_group_id = S.run_group_id AND T.chunk_key = S.chunk_key
      WHEN MATCHED THEN UPDATE SET
        updated_at = S.updated_at
      WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, updated_at)
        VALUES (S.model_name, S.execution_ts, S.status, S.run_group_id, S.chunk_key, S.updated_at)
      {%- endset -%}
      {{ sundial_dbt_shared.with_merge_retry(merge_sql) }}
    {% else %}
      SELECT 1 AS no_results_to_log
    {% endif %}
  {% else %}
    SELECT 1 AS no_results_to_log
  {% endif %}
{% endmacro %}

{# ------------------------------------------------------------------ #}
{#  Cross-orchestrator run lock — realized purely on 'started' + terminal.#}
{#                                                                    #}
{#  The lock is NOT a separate table or status; it IS the per-model      #}
{#  'started' row, made run-aware. Holder = ``run_group_id`` (Airflow dag  #}
{#  run_id via var, else invocation_id), so every chunk/task of ONE run    #}
{#  shares the lock (teammates) while a different run_group is a stranger. #}
{#                                                                    #}
{#  A run_group G HOLDS model M iff it has a 'started' row for M with NO    #}
{#  terminal row (succeeded/failed/locked_out/crashed) for the same        #}
{#  (model, run_group, chunk). Release is simply the terminal row that      #}
{#  log_model_status / log_run_results already write — there is no          #}
{#  separate release step.                                                 #}
{#                                                                    #}
{#  ROLE OF heartbeat_at — crash recovery ONLY. It plays NO part in the     #}
{#  lock judgement. Its sole job: a 'started' whose heartbeat is older      #}
{#  than ``dbt_run_lock_ttl_minutes`` (default 240) — i.e. the process      #}
{#  died (OOM/eviction) and never wrote a terminal — is swept to a          #}
{#  'crashed' terminal row, which then frees the lock via the normal        #}
{#  started/terminal logic. (Set the TTL above the longest single-model     #}
{#  build; heartbeat is stamped at acquire and not beaten mid-build.)       #}
{#                                                                    #}
{#  acquire_run_lock() (pre-hook, REPLACES log_model_status('started')):    #}
{#    1) reclaim: sweep stale 'started' rows for M to 'crashed';            #}
{#    2) write my 'started' (updated_at on insert → stable priority key);   #}
{#    3) look (started/terminal only): if a foreign run_group HOLDS M with   #}
{#       PRIORITY (earlier updated_at, ties by run_group_id), flip my row    #}
{#       to 'locked_out', log the holder, and raise — the model FAILS, but   #}
{#       its status is 'locked_out' (filtered from the view; never rewritten #}
{#       to 'failed' by log_run_results).                                   #}
{#                                                                    #}
{#  Write-first-then-look + single warehouse clock ⇒ at least one run        #}
{#  always sees the other and exactly the earliest proceeds (no double run,  #}
{#  no mutual lockout).                                                     #}
{# ------------------------------------------------------------------ #}
{% macro acquire_run_lock() %}
  {%- if execute -%}
    {%- set rg = var('run_group_id', invocation_id) -%}
    {%- set ck = var('chunk_key', 'full') -%}
    {%- set ttl = var('dbt_run_lock_ttl_minutes', 240) | int -%}
    {%- set tbl = sundial_dbt_shared.dbt_completions_table() -%}
    {%- set terminal = "('succeeded','failed','locked_out','crashed')" -%}

    {# 1) CRASH RECLAIM (the only use of heartbeat_at): any 'started' for this
       model whose heartbeat is older than the TTL and which never reached a
       terminal is swept to 'crashed', so the lock logic below sees it freed. #}
    {%- set reclaim_sql -%}
      MERGE {{ tbl }} T
      USING (
        SELECT s.model_name, s.execution_ts, s.run_group_id, s.chunk_key
        FROM {{ tbl }} s
        WHERE s.model_name = '{{ this.name }}'
          AND s.status = 'started'
          AND s.heartbeat_at IS NOT NULL
          AND s.heartbeat_at < {{ sundial_dbt_shared.lock_ts_sub('CURRENT_TIMESTAMP()', ttl) }}
          AND NOT EXISTS (
            SELECT 1 FROM {{ tbl }} t
            WHERE t.model_name = s.model_name AND t.run_group_id = s.run_group_id
              AND t.chunk_key = s.chunk_key AND t.status IN {{ terminal }}
          )
      ) S
      ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
         AND T.chunk_key = S.chunk_key AND T.status = 'crashed'
      WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, updated_at)
        VALUES (S.model_name, S.execution_ts, 'crashed', S.run_group_id, S.chunk_key, CURRENT_TIMESTAMP())
    {%- endset -%}
    {% do run_query(sundial_dbt_shared.with_merge_retry(reclaim_sql)) %}

    {# 2) Write my 'started' row. updated_at set on insert only (stable →
       priority); re-merge just refreshes heartbeat_at (crash-recovery liveness). #}
    {%- set merge_sql -%}
      MERGE {{ tbl }} T
      USING (
        SELECT '{{ this.name }}' AS model_name,
               {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
               '{{ rg }}' AS run_group_id, '{{ ck }}' AS chunk_key,
               CURRENT_TIMESTAMP() AS now_ts
      ) S
      ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
         AND T.chunk_key = S.chunk_key AND T.status = 'started'
      WHEN MATCHED THEN UPDATE SET heartbeat_at = S.now_ts
      WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, heartbeat_at, updated_at)
        VALUES (S.model_name, S.execution_ts, 'started', S.run_group_id, S.chunk_key, S.now_ts, S.now_ts)
    {%- endset -%}
    {% do run_query(sundial_dbt_shared.with_merge_retry(merge_sql)) %}

    {# 3) Look — started/terminal ONLY (no heartbeat): a foreign run_group that
       HOLDS M (a 'started' chunk with no terminal) and has priority over me. #}
    {%- set verify_sql -%}
      SELECT f.run_group_id
      FROM {{ tbl }} f
      WHERE f.model_name = '{{ this.name }}'
        AND f.status = 'started'
        AND f.run_group_id != '{{ rg }}'
        AND NOT EXISTS (
          SELECT 1 FROM {{ tbl }} t
          WHERE t.model_name = f.model_name AND t.run_group_id = f.run_group_id
            AND t.chunk_key = f.chunk_key AND t.status IN {{ terminal }}
        )
        AND (
          f.updated_at < (
            SELECT MIN(m.updated_at) FROM {{ tbl }} m
            WHERE m.model_name = '{{ this.name }}' AND m.run_group_id = '{{ rg }}' AND m.status = 'started'
          )
          OR (
            f.updated_at = (
              SELECT MIN(m.updated_at) FROM {{ tbl }} m
              WHERE m.model_name = '{{ this.name }}' AND m.run_group_id = '{{ rg }}' AND m.status = 'started'
            )
            AND f.run_group_id < '{{ rg }}'
          )
        )
      ORDER BY f.updated_at, f.run_group_id
      LIMIT 1
    {%- endset -%}
    {%- set res = run_query(verify_sql) -%}
    {%- set rows = res.rows if res is not none else [] -%}

    {%- if rows | length > 0 -%}
      {%- set winner = rows[0][0] -%}
      {# Lost the race: flip my 'started' to the 'locked_out' terminal (NOT failed). #}
      {%- set lockout_sql -%}
        MERGE {{ tbl }} T
        USING (
          SELECT '{{ this.name }}' AS model_name, '{{ rg }}' AS run_group_id,
                 '{{ ck }}' AS chunk_key, CURRENT_TIMESTAMP() AS now_ts
        ) S
        ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
           AND T.chunk_key = S.chunk_key AND T.status = 'started'
        WHEN MATCHED THEN UPDATE SET status = 'locked_out', updated_at = S.now_ts
      {%- endset -%}
      {% do run_query(sundial_dbt_shared.with_merge_retry(lockout_sql)) %}
      {{ log("dbt_run_lock: model '" ~ this.name ~ "' locked out — run_group '" ~ winner ~ "' is currently running; this run will not build it.", info=True) }}
      {{ exceptions.raise_compiler_error(
          "dbt_run_lock: model '" ~ this.name ~ "' is locked out — run_group '" ~ winner
          ~ "' holds it. Recorded status='locked_out' (not a model failure).") }}
    {%- endif -%}
  {%- endif -%}
  SELECT 1 AS lock_acquired
{% endmacro %}

{# ------------------------------------------------------------------ #}
{#  Incremental watermark — "data present till", sourced from            #}
{#  dbt_completions instead of MAX(timestamp_col) on the target.         #}
{#                                                                    #}
{#  These operate on the SAME per-run 'started' row acquire_run_lock      #}
{#  writes (keyed model_name/run_group_id/chunk_key): the window columns  #}
{#  live on that row, so status + lock + watermark are one row, not       #}
{#  three.                                                               #}
{#                                                                    #}
{#  Tenant wiring (warehouse-specific start_ts/end_ts call these):        #}
{#    where {{ start_ts(...) }} <= ts and ts <= {{ end_ts(...) }}         #}
{#  with start_ts() using read_watermark(this.name) for its lower bound   #}
{#  and calling record_window_start()/record_window_end() as it renders;  #}
{#  commit_window() wired as a post-hook.                                 #}
{#                                                                    #}
{#  read_watermark mirrors sundial's last_processed_timestamp:            #}
{#  MAX(window_end_ts) over this model's COMPLETE runs — run_groups whose  #}
{#  every 'started' chunk has committed (build succeeded). A run with any  #}
{#  uncommitted chunk (in-flight / failed) contributes NOTHING, so the     #}
{#  watermark holds at the previous complete run (gap-safe). MAX gives     #}
{#  no-regress (a partial backfill's historical window_end is ignored).    #}
{# ------------------------------------------------------------------ #}
{% macro read_watermark(model_name) %}
  SELECT MAX(w.window_end_ts)
  FROM {{ sundial_dbt_shared.dbt_completions_table() }} w
  WHERE w.model_name = '{{ model_name }}'
    AND w.status = 'started'
    AND w.window_end_ts IS NOT NULL
    AND w.run_group_id NOT IN (
      SELECT u.run_group_id
      FROM {{ sundial_dbt_shared.dbt_completions_table() }} u
      WHERE u.model_name = '{{ model_name }}'
        AND u.status = 'started'
        AND u.window_committed_at IS NULL
    )
{% endmacro %}

{% macro _should_record_window() %}
  {{ return(execute
            and model is not none
            and model.config.get('materialized') == 'incremental'
            and var('record_incremental_window', true)) }}
{% endmacro %}

{# Upsert window_end_ts onto this run's 'started' row (the watermark
   contributor). end_sql is a self-contained DATETIME expression. #}
{% macro record_window_end(end_sql) %}
  {% if sundial_dbt_shared._should_record_window() %}
    {%- set rg = var('run_group_id', invocation_id) -%}
    {%- set ck = var('chunk_key', 'full') -%}
    {%- set sql -%}
      MERGE {{ sundial_dbt_shared.dbt_completions_table() }} T
      USING (
        SELECT '{{ this.name }}' AS model_name,
               {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
               '{{ rg }}' AS run_group_id, '{{ ck }}' AS chunk_key,
               ({{ end_sql }}) AS we
      ) S
      ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
         AND T.chunk_key = S.chunk_key AND T.status = 'started'
      WHEN MATCHED THEN UPDATE SET window_end_ts = S.we
      WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, window_end_ts, updated_at)
        VALUES (S.model_name, S.execution_ts, 'started', S.run_group_id, S.chunk_key, S.we, CURRENT_TIMESTAMP())
    {%- endset -%}
    {% do run_query(sundial_dbt_shared.with_merge_retry(sql)) %}
  {% endif %}
{% endmacro %}

{# Upsert window_start_ts onto the 'started' row. start_sql may reference
   read_watermark (a scalar subquery over this table) — resolve it to a
   literal FIRST so the MERGE source never reads the table it writes. #}
{% macro record_window_start(start_sql) %}
  {% if sundial_dbt_shared._should_record_window() %}
    {%- set rg = var('run_group_id', invocation_id) -%}
    {%- set ck = var('chunk_key', 'full') -%}
    {%- set res = run_query("SELECT (" ~ start_sql ~ ") AS ws") -%}
    {%- set ws = res.rows[0][0] if res is not none and res.rows | length > 0 else none -%}
    {% if ws is not none %}
      {%- set sql -%}
        MERGE {{ sundial_dbt_shared.dbt_completions_table() }} T
        USING (
          SELECT '{{ this.name }}' AS model_name,
                 {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
                 '{{ rg }}' AS run_group_id, '{{ ck }}' AS chunk_key,
                 CAST('{{ ws }}' AS {{ sundial_dbt_shared.completions_col_type('datetime') }}) AS ws
        ) S
        ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
           AND T.chunk_key = S.chunk_key AND T.status = 'started'
        WHEN MATCHED THEN UPDATE SET window_start_ts = S.ws
        WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, window_start_ts, updated_at)
          VALUES (S.model_name, S.execution_ts, 'started', S.run_group_id, S.chunk_key, S.ws, CURRENT_TIMESTAMP())
      {%- endset -%}
      {% do run_query(sundial_dbt_shared.with_merge_retry(sql)) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{# POST-HOOK: stamp this run's chunk as committed on BUILD success (before dbt
   tests run as separate nodes) — matching sundial advancing last_processed
   before its data-quality checks. Only a committed chunk counts toward the
   watermark; a run is complete once all its chunks are committed. #}
{% macro commit_window() %}
  {% if sundial_dbt_shared._should_record_window() %}
    UPDATE {{ sundial_dbt_shared.dbt_completions_table() }}
    SET window_committed_at = CURRENT_TIMESTAMP()
    WHERE model_name = '{{ this.name }}'
      AND run_group_id = '{{ var('run_group_id', invocation_id) }}'
      AND chunk_key = '{{ var('chunk_key', 'full') }}'
      AND status = 'started'
      AND window_end_ts IS NOT NULL
  {% else %}
    SELECT 1 AS no_window_to_commit
  {% endif %}
{% endmacro %}
