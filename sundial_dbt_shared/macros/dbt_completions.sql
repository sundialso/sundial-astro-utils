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
{#      set. Snowflake: var `target_schema` is REQUIRED (no           #}
{#      `target.schema` fallback); `target_database` is optional       #}
{#      (otherwise `target.database` is used).                         #}
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
{#      # ensure_run_group_columns() MUST come right after the create #}
{#      # so pre-existing tables gain run_group_id/chunk_key/         #}
{#      # heartbeat_at before any hook MERGEs into those columns.     #}
{#      - "{{ sundial_dbt_shared.ensure_run_group_columns() }}"       #}
{#      - "{{ sundial_dbt_shared.create_dbt_completions_view() }}"    #}
{#    on-run-end:                                                     #}
{#      - "{{ sundial_dbt_shared.log_run_results() }}"                #}
{#    models:                                                         #}
{#      <project>:                                                    #}
{#        # acquire_run_lock() REPLACES log_model_status('started') as #}
{#        # the pre-hook: it writes the same run-aware 'started' row    #}
{#        # AND enforces the cross-run lock. Using log_model_status     #}
{#        # here instead leaves the lock inert and writes 'started'     #}
{#        # rows with a NULL heartbeat that crash-reclaim can't sweep.  #}
{#        +pre-hook:  ["{{ sundial_dbt_shared.acquire_run_lock() }}"]            #}
{#        +post-hook: ["{{ sundial_dbt_shared.log_model_status('succeeded') }}"] #}
{#                                                                    #}
{#  Migration (upgrading an existing tenant to run grouping + lock):  #}
{#    1. ensure_run_group_columns() (wired above) back-fills the three #}
{#       new columns on the existing dbt_completions_raw table.        #}
{#    2. The dbt_completions VIEW is CREATE … IF NOT EXISTS, so the new #}
{#       'locked_out'/'crashed' filter does NOT apply until the view is #}
{#       DROPPED once; the next on-run-start recreates it with the      #}
{#       filter. Until then those rows leak into what sundial reads.    #}
{#    3. Snowflake: `target_schema` is now REQUIRED — the old           #}
{#       `target.schema` profile fallback was removed. Tenants relying  #}
{#       on it must set the `target_schema` var explicitly.             #}
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
    model_name   {{ sundial_dbt_shared.completions_col_type('string') }},
    execution_ts {{ sundial_dbt_shared.completions_col_type('string') }},
    status       {{ sundial_dbt_shared.completions_col_type('string') }},
    updated_at   {{ sundial_dbt_shared.completions_col_type('timestamp') }},
    run_group_id {{ sundial_dbt_shared.completions_col_type('string') }},
    chunk_key    {{ sundial_dbt_shared.completions_col_type('string') }},
    heartbeat_at {{ sundial_dbt_shared.completions_col_type('timestamp') }}
  )
{% endmacro %}

{# Additive column add for tables created before run grouping / the lock.
   Wire into on-run-start AFTER create_dbt_completions_table(). BigQuery and
   Snowflake both support ADD COLUMN IF NOT EXISTS. #}
{% macro ensure_run_group_columns() %}
  ALTER TABLE {{ sundial_dbt_shared.dbt_completions_table() }}
    ADD COLUMN IF NOT EXISTS run_group_id {{ sundial_dbt_shared.completions_col_type('string') }},
    ADD COLUMN IF NOT EXISTS chunk_key    {{ sundial_dbt_shared.completions_col_type('string') }},
    ADD COLUMN IF NOT EXISTS heartbeat_at {{ sundial_dbt_shared.completions_col_type('timestamp') }}
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
    - failed    : materialization failed (any non-'success', non-'skipped'
                  build status, e.g. 'error') OR a test reported 'fail'/'error'
                  (fatal severity).
    - succeeded : materialization succeeded this invocation, OR it's a
                  tests-only invocation and no test was fatal. A 'warn'
                  test result is non-fatal in dbt and is treated as success
                  (handles goal: "test fails but dbt Cloud / Astro marks
                  the run succeeded → succeeded").
    - skipped   : dbt never ran the model this invocation (an upstream failed,
                  or it was a locked-out model's descendant). A skipped model
                  did NOT run, so NO row is written — we neither succeed nor
                  fail it, and its pre-hooks never fired so there is no
                  'started' row to reconcile. Skipped tests likewise don't
                  count as "tests ran".

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
      {# Skipped tests didn't actually run — exclude them so a model whose tests
         were all skipped isn't recorded 'succeeded' off the back of them. #}
      {% set tests_ran = (test_statuses | reject('equalto', 'skipped') | list) | length > 0 %}
      {% set build_skipped = build_status == 'skipped' %}
      {% set build_failed = build_status is not none and build_status not in ('success', 'skipped') %}
      {% set build_ok = build_status == 'success' %}

      {% if build_skipped %}
        {# dbt skipped this model — it never ran, so record nothing. #}
      {% elif build_failed or has_failing_test %}
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
{#    2) write my 'started' (updated_at on insert → stable priority key; a    #}
{#       retry after lockout REVIVES my own 'locked_out' row, not a new one);  #}
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
      MERGE INTO {{ tbl }} T
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

    {# 2) Write my 'started' row. Matched on the live lifecycle row for
       (model, run_group, chunk) — there is at most one, either 'started' or
       'locked_out' (the lockout in step 3 flips the SAME row rather than adding
       one), so this MERGE can never match two:
         - NOT MATCHED        → first acquire: INSERT 'started' with updated_at=now
                                (my stable priority key).
         - MATCHED 'started'  → same attempt re-running: just refresh heartbeat_at;
                                updated_at stays put so my priority never drifts.
         - MATCHED 'locked_out' → I lost a prior race and Airflow is RETRYING me
                                under the same run_group. REVIVE that row to
                                'started' with a FRESH priority (updated_at=now)
                                instead of leaving it and inserting a parallel
                                row. Keeping one lifecycle row means no stale
                                'locked_out' survives to make log_run_results
                                suppress this attempt's terminal. #}
    {%- set merge_sql -%}
      MERGE INTO {{ tbl }} T
      USING (
        SELECT '{{ this.name }}' AS model_name,
               {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
               '{{ rg }}' AS run_group_id, '{{ ck }}' AS chunk_key,
               CURRENT_TIMESTAMP() AS now_ts
      ) S
      ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
         AND T.chunk_key = S.chunk_key AND T.status IN ('started', 'locked_out')
      WHEN MATCHED AND T.status = 'locked_out' THEN
        UPDATE SET status = 'started', updated_at = S.now_ts, heartbeat_at = S.now_ts
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
        MERGE INTO {{ tbl }} T
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
