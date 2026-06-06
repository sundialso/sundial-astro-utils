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
{#      set. Snowflake: `target_schema`/`target_database` vars win,    #}
{#      otherwise the active profile's `target.schema`/`target.database`#}
{#      are used as fallbacks.                                         #}
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
{#        # acquire_run_lock() REPLACES log_model_status('started') as #}
{#        # the pre-hook: it writes the same run-aware 'started' row    #}
{#        # AND enforces the cross-run lock. Using log_model_status     #}
{#        # here instead leaves the lock inert and writes 'started'     #}
{#        # rows with a NULL heartbeat that crash-reclaim can't sweep.  #}
{#        +pre-hook:  ["{{ sundial_dbt_shared.acquire_run_lock() }}"]            #}
{#        +post-hook: ["{{ sundial_dbt_shared.log_model_status('succeeded') }}"] #}
{#      # Incremental models additionally validate backfill bounds as   #}
{#      # the FIRST pre-hook and record their watermark window from the  #}
{#      # tenant's start_ts()/end_ts() — see validate_partial_backfill / #}
{#      # record_window_start / record_window_end below.                #}
{#                                                                    #}
{#  First-time setup:                                                 #}
{#    - create_dbt_completions_table() defines the FULL schema (status  #}
{#      + lock + watermark columns) and is self-healing: on a fresh     #}
{#      table it CREATEs IF NOT EXISTS; on an OLD table missing the      #}
{#      lock/watermark columns it CREATE OR REPLACEs (drops + rebuilds   #}
{#      with the full schema — the old rows are discarded, fine for a    #}
{#      brand-new feature). No ALTER-based column migration.            #}
{#    - The dbt_completions VIEW is CREATE OR REPLACE, so a view         #}
{#      definition change (chunk-aware rollup / locked_out filter)        #}
{#      applies on the NEXT run automatically — no manual drop. The       #}
{#      replace is atomic, so readers never see a missing view.          #}
{#    - Snowflake: `target_schema` var wins, else the `target.schema`    #}
{#      profile value is used as a fallback.                            #}
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

{# Returns the dbt_completions_raw table as a Relation object (database/schema/
   identifier), so create_dbt_completions_table() can introspect its columns at
   runtime via adapter.get_relation / get_columns_in_relation. The string FQN
   used in DDL comes from dbt_completions_table(); this is the structured form.
   ⚠️ TEMPORARY — exists only for the create-or-replace migration in
   create_dbt_completions_table(); delete both together once all tables are
   on the full schema. #}
{% macro completions_relation() %}
  {{ return(adapter.dispatch('completions_relation', 'sundial_dbt_shared')()) }}
{% endmacro %}

{% macro execution_ts_as_datestr() %}
  {{ return(adapter.dispatch('execution_ts_as_datestr', 'sundial_dbt_shared')()) }}
{% endmacro %}

{% macro completions_col_type(kind) %}
  {{ return(adapter.dispatch('completions_col_type', 'sundial_dbt_shared')(kind)) }}
{% endmacro %}

{# Wraps a MERGE statement — or the CREATE OR REPLACE VIEW DDL — in
   warehouse-specific concurrency-retry logic. BigQuery uses snapshot isolation:
   concurrent MERGEs into the shared dbt_completions table abort with "Could not
   serialize access ... due to concurrent update", and concurrent CREATE OR
   REPLACE VIEW on the same view aborts with a concurrent-update/DDL error, once
   parallelism exceeds what it will queue. The BigQuery impl re-runs the
   statement (a fresh snapshot each attempt) on that class of error; Snowflake
   serialises DML/DDL via locks, so its impl emits the statement unchanged. #}
{% macro with_merge_retry(merge_sql) %}
  {{ return(adapter.dispatch('with_merge_retry', 'sundial_dbt_shared')(merge_sql)) }}
{% endmacro %}

{# Subtract ``minutes`` from a warehouse timestamp expression — the run-lock
   TTL/heartbeat staleness check. #}
{% macro lock_ts_sub(ts_expr, minutes) %}
  {{ return(adapter.dispatch('lock_ts_sub', 'sundial_dbt_shared')(ts_expr, minutes)) }}
{% endmacro %}

{# Coerce a warehouse timestamp/datetime/date/string expression to the
   completions 'datetime' column type (naive: BigQuery DATETIME / Snowflake
   TIMESTAMP_NTZ). The start_ts/end_ts watermark columns are 'datetime', and
   BigQuery does not implicitly convert TIMESTAMP->DATETIME — so wrap any
   start_ts()/end_ts() expression with this before writing it. Both adapters
   interpret a bare TIMESTAMP at UTC (see the per-adapter impls for tz caveats).#}
{% macro to_completions_datetime(ts_expr) %}
  {{ return(adapter.dispatch('to_completions_datetime', 'sundial_dbt_shared')(ts_expr)) }}
{% endmacro %}

{# Returns a SQL expression for "today minus ``days``" as a 'YYYY-MM-DD' string,
   to compare against the execution_ts column (also a YYYY-MM-DD string).
   Evaluated at query time (uses CURRENT_DATE), so the view's window slides. #}
{% macro execution_ts_days_ago(days) %}
  {{ return(adapter.dispatch('execution_ts_days_ago', 'sundial_dbt_shared')(days)) }}
{% endmacro %}

{#
  create_dbt_completions_table — self-healing on-run-start create.

  ⚠️ TEMPORARY (remove the CREATE OR REPLACE branch once every tenant's
  dbt_completions_raw has the full lock/watermark schema). After that, revert
  this macro to a plain `CREATE TABLE IF NOT EXISTS (...)` — the runtime
  column-introspection + completions_relation() primitive exist ONLY to migrate
  old tables by replacement and should be deleted with it.

  At runtime it introspects the existing dbt_completions_raw (if any) and picks
  the right DDL:
    - table absent, OR present with the full schema  -> CREATE TABLE IF NOT EXISTS
      (a harmless no-op when it already exists).
    - table present but MISSING any lock/watermark column -> CREATE OR REPLACE
      TABLE with the full schema. This DROPS the old table and its rows; that is
      acceptable because the lock/watermark feature is new and a pre-feature
      table holds nothing worth preserving for it. (NOTE: only triggers on the
      transition run; once the columns exist no replace ever happens. Avoid
      deploying this for the first time mid-write under chunked fan-out, where a
      parallel invocation could replace the table while another is writing.)

  Detection uses dbt's warehouse-agnostic relation APIs (no information_schema
  dialect handling); column names are lower-cased before comparison so it works
  on Snowflake's upper-cased catalog too. Both DDL paths go through
  with_merge_retry to absorb concurrent-DDL errors from parallel on-run-start.
#}
{% macro create_dbt_completions_table() %}
  {%- set cols -%}
    model_name          {{ sundial_dbt_shared.completions_col_type('string') }},
    execution_ts        {{ sundial_dbt_shared.completions_col_type('string') }},
    status              {{ sundial_dbt_shared.completions_col_type('string') }},
    updated_at          {{ sundial_dbt_shared.completions_col_type('timestamp') }},
    run_group_id        {{ sundial_dbt_shared.completions_col_type('string') }},
    chunk_key           {{ sundial_dbt_shared.completions_col_type('string') }},
    heartbeat_at        {{ sundial_dbt_shared.completions_col_type('timestamp') }},
    start_ts     {{ sundial_dbt_shared.completions_col_type('datetime') }},
    end_ts       {{ sundial_dbt_shared.completions_col_type('datetime') }}
  {%- endset -%}

  {%- set required = ['run_group_id', 'chunk_key', 'heartbeat_at', 'start_ts', 'end_ts'] -%}
  {%- set rel = sundial_dbt_shared.completions_relation() -%}
  {%- set existing = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) if execute else none -%}

  {%- if existing is not none -%}
    {%- set have = adapter.get_columns_in_relation(existing) | map(attribute='name') | map('lower') | list -%}
    {%- set missing = required | reject('in', have) | list -%}
  {%- else -%}
    {%- set missing = [] -%}
  {%- endif -%}

  {%- if existing is not none and missing | length > 0 -%}
    {%- set ddl -%}
      CREATE OR REPLACE TABLE {{ sundial_dbt_shared.dbt_completions_table() }} (
        {{ cols }}
      )
    {%- endset -%}
    {{ sundial_dbt_shared.with_merge_retry(ddl) }}
  {%- else -%}
    {%- set ddl -%}
      CREATE TABLE IF NOT EXISTS {{ sundial_dbt_shared.dbt_completions_table() }} (
        {{ cols }}
      )
    {%- endset -%}
    {{ sundial_dbt_shared.with_merge_retry(ddl) }}
  {%- endif -%}
{% endmacro %}

{#
  create_dbt_completions_view — one row per (model, execution_ts) = that date's
  latest run, collective across chunks.

  The hooks MERGE every status transition into dbt_completions_raw (several rows
  per model per run, one per chunk per status). This view reduces that to ONE
  row per (model_name, execution_ts): within each date the LATEST run_group (a
  later re-run / backfill supersedes an earlier one on the same date), rolled up
  across that run's chunks. A model keeps one row per execution_ts it ran on in
  the window — not just its most recent date.

  Rollup rule (per chunk, take its LATEST status by updated_at — so a retry that
  succeeds clears an earlier 'failed' for the same chunk):
    - failed    if ANY chunk's latest status is 'failed'
    - started   else if ANY chunk's latest status is 'started' (still in flight)
    - succeeded else (every chunk's latest is a non-failed terminal)
  This is correct latest-state semantics (not the old count-equality, which
  mis-read succeeded-only and mismatched-chunk runs).

  'locked_out' / 'crashed' rows are excluded — they mean "this run produced no
  data", so they never mask the real data-producing run.

  Exposed columns = all of dbt_completions_raw EXCEPT heartbeat_at:
    model_name, execution_ts, status, run_group_id, chunk_key,
    start_ts, end_ts, updated_at
  Aggregated to the one-row grain:
    - run_group_id   : the winning run_group (single value).
    - chunk_key      : the chunk key if the run was single-chunk (e.g. 'full'),
                       else the sentinel '__all__' (the row is a cross-chunk
                       rollup spanning every chunk of the run).
    - start_ts: MIN across the run's chunks (overall covered-from).
    - end_ts  : MAX across the run's chunks (overall covered-to).
    - updated_at     : MAX across the run.

  Bounded to the last ``dbt_completions_view_days`` days (default 30) of
  execution_ts so long-dead models drop off and the scan stays small; a model
  that last ran 2 days ago still appears, at its older latest execution_ts.

  Emitted as CREATE OR REPLACE VIEW so a definition change applies on the next
  run with no manual drop; the replace is atomic on both warehouses, and the DDL
  is wrapped in with_merge_retry to absorb BigQuery's metadata-level
  concurrent-update error when parallel on-run-start invocations replace it at
  once (all write a byte-identical definition, so whichever lands last is right).
#}
{% macro create_dbt_completions_view() %}
  {%- set days = var('dbt_completions_view_days', 30) | int -%}
  {%- set view_sql -%}
  CREATE OR REPLACE VIEW {{ sundial_dbt_shared.dbt_completions_view() }} AS
  WITH live AS (
    SELECT model_name, execution_ts, run_group_id, chunk_key, status, updated_at,
           start_ts, end_ts
    FROM {{ sundial_dbt_shared.dbt_completions_table() }}
    WHERE status NOT IN ('locked_out', 'crashed')
      AND execution_ts >= {{ sundial_dbt_shared.execution_ts_days_ago(days) }}
  ),
  {# Per (model, execution_ts): pick the latest run_group (max updated_at, then
     run_group_id for a deterministic tie-break). A later re-run / backfill on the
     same date supersedes an earlier one. #}
  ranked AS (
    SELECT
      model_name, execution_ts, run_group_id,
      ROW_NUMBER() OVER (
        PARTITION BY model_name, execution_ts
        ORDER BY MAX(updated_at) DESC, run_group_id DESC
      ) AS _rn
    FROM live
    GROUP BY model_name, execution_ts, run_group_id
  ),
  winner AS (
    SELECT model_name, execution_ts, run_group_id
    FROM ranked WHERE _rn = 1
  ),
  win_rows AS (
    SELECT l.*
    FROM live l
    JOIN winner w
      ON l.model_name = w.model_name
     AND l.execution_ts = w.execution_ts
     AND l.run_group_id = w.run_group_id
  ),
  {# Each chunk's latest status only (so a successful retry supersedes a prior
     'failed' for that chunk). #}
  chunk_final AS (
    SELECT model_name, execution_ts, chunk_key, status
    FROM (
      SELECT model_name, execution_ts, chunk_key, status,
        ROW_NUMBER() OVER (
          PARTITION BY model_name, execution_ts, chunk_key ORDER BY updated_at DESC
        ) AS _crn
      FROM win_rows
    ) c
    WHERE _crn = 1
  ),
  status_roll AS (
    SELECT model_name, execution_ts,
      CASE
        WHEN SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) > 0 THEN 'failed'
        WHEN SUM(CASE WHEN status = 'started' THEN 1 ELSE 0 END) > 0 THEN 'started'
        ELSE 'succeeded'
      END AS status
    FROM chunk_final
    GROUP BY model_name, execution_ts
  ),
  {# run_group / chunk_key / window columns for the winning run. start_ts/end_ts
     live only on 'started' rows; MIN/MAX skip the NULLs on terminal rows. #}
  win_agg AS (
    SELECT model_name, execution_ts,
      MAX(run_group_id) AS run_group_id,
      CASE WHEN COUNT(DISTINCT chunk_key) = 1 THEN MAX(chunk_key) ELSE '__all__' END AS chunk_key,
      MIN(start_ts) AS start_ts,
      MAX(end_ts)   AS end_ts,
      MAX(updated_at)      AS updated_at
    FROM win_rows
    GROUP BY model_name, execution_ts
  )
  SELECT
    a.model_name,
    a.execution_ts,
    s.status,
    a.run_group_id,
    a.chunk_key,
    a.start_ts,
    a.end_ts,
    a.updated_at
  FROM win_agg a
  JOIN status_roll s
    ON a.model_name = s.model_name AND a.execution_ts = s.execution_ts
  {%- endset -%}
  {{ sundial_dbt_shared.with_merge_retry(view_sql) }}
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
{#  and calling record_window_start()/record_window_end() as it renders.  #}
{#  The watermark advances via the 'succeeded' terminal (build + blocking  #}
{#  tests) that log_model_status / log_run_results already write — there   #}
{#  is no separate commit step.                                           #}
{#                                                                    #}
{#  read_watermark mirrors sundial's last_processed_timestamp:            #}
{#  MAX(end_ts) over this model's COMPLETE runs — run_groups whose  #}
{#  every 'started' chunk reached a 'succeeded' terminal. Because          #}
{#  log_run_results only writes 'succeeded' when the build AND its         #}
{#  blocking (error-severity) tests pass, a failing table test holds the   #}
{#  watermark — matching sundial, where a pipeline-blocking table-test     #}
{#  failure fails materialization and last_processed never advances.       #}
{#  Any chunk that is in-flight / failed / locked_out / crashed lacks a    #}
{#  'succeeded' sibling, so its whole run contributes NOTHING (gap-safe);  #}
{#  MAX gives no-regress (a partial backfill's historical end is ignored). #}
{# ------------------------------------------------------------------ #}
{% macro read_watermark(model_name) %}
  SELECT MAX(w.end_ts)
  FROM {{ sundial_dbt_shared.dbt_completions_table() }} w
  WHERE w.model_name = '{{ model_name }}'
    AND w.status = 'started'
    AND w.end_ts IS NOT NULL
    -- Exclude w if its OWN run_group has any 'started' chunk with NO 'succeeded'
    -- sibling (still running, failed, blocking-test-failed, locked_out, crashed).
    -- NOT EXISTS (not NOT IN): null-safe, so a stray run_group_id = NULL row can't
    -- poison the predicate and silently blank the whole watermark.
    AND NOT EXISTS (
      SELECT 1
      FROM {{ sundial_dbt_shared.dbt_completions_table() }} u
      WHERE u.model_name = w.model_name
        AND u.run_group_id = w.run_group_id
        AND u.status = 'started'
        AND NOT EXISTS (
          SELECT 1 FROM {{ sundial_dbt_shared.dbt_completions_table() }} s
          WHERE s.model_name = u.model_name AND s.run_group_id = u.run_group_id
            AND s.chunk_key = u.chunk_key AND s.status = 'succeeded'
        )
    )
{% endmacro %}

{% macro _should_record_window() %}
  {{ return(execute
            and model is not none
            and model.config.get('materialized') == 'incremental'
            and var('record_incremental_window', true)) }}
{% endmacro %}

{# Upsert end_ts onto this run's 'started' row (the watermark
   contributor). end_sql is a self-contained timestamp/datetime/date expression;
   it is coerced to the 'datetime' column type via to_completions_datetime(), so
   a tz-aware TIMESTAMP end_ts() is accepted without a manual cast. #}
{% macro record_window_end(end_sql) %}
  {% if sundial_dbt_shared._should_record_window() %}
    {# Dedupe identical writes within a model render: a model often calls end_ts()
       many times (one per CTE/WHERE) with the SAME expression, which would fire an
       identical MERGE each time and hammer the rate-limited completions table.
       Memoise the recorded end expressions on the model node so each distinct
       value writes once. (Distinct lags → distinct end_sql → each still recorded;
       last distinct value wins on end_ts, as before.) #}
    {%- set memo_key = '__sundial_recorded_end__' ~ (end_sql | trim) -%}
    {%- if model is none or model.get(memo_key) is none -%}
      {%- if model is not none -%}{%- do model.update({memo_key: true}) -%}{%- endif -%}
      {%- set rg = var('run_group_id', invocation_id) -%}
      {%- set ck = var('chunk_key', 'full') -%}
      {%- set sql -%}
        MERGE INTO {{ sundial_dbt_shared.dbt_completions_table() }} T
        USING (
          SELECT '{{ this.name }}' AS model_name,
                 {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
                 '{{ rg }}' AS run_group_id, '{{ ck }}' AS chunk_key,
                 {{ sundial_dbt_shared.to_completions_datetime(end_sql) }} AS we
        ) S
        ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
           AND T.chunk_key = S.chunk_key AND T.status = 'started'
        WHEN MATCHED THEN UPDATE SET end_ts = S.we
        WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, end_ts, updated_at)
          VALUES (S.model_name, S.execution_ts, 'started', S.run_group_id, S.chunk_key, S.we, CURRENT_TIMESTAMP())
      {%- endset -%}
      {% do run_query(sundial_dbt_shared.with_merge_retry(sql)) %}
    {%- endif -%}
  {% endif %}
{% endmacro %}

{# Upsert start_ts onto the 'started' row. start_sql may reference
   read_watermark (a scalar subquery over this table) — resolve it to a
   literal FIRST so the MERGE source never reads the table it writes. The
   resolution SELECT coerces start_sql to the 'datetime' type via
   to_completions_datetime(), so the resolved value is naive and the re-injected
   literal can't carry a tz offset that would break the CAST. #}
{% macro record_window_start(start_sql) %}
  {% if sundial_dbt_shared._should_record_window() %}
    {%- set rg = var('run_group_id', invocation_id) -%}
    {%- set ck = var('chunk_key', 'full') -%}
    {%- set res = run_query("SELECT " ~ sundial_dbt_shared.to_completions_datetime(start_sql) ~ " AS ws") -%}
    {%- set ws = res.rows[0][0] if res is not none and res.rows | length > 0 else none -%}
    {% if ws is not none %}
      {%- set sql -%}
        MERGE INTO {{ sundial_dbt_shared.dbt_completions_table() }} T
        USING (
          SELECT '{{ this.name }}' AS model_name,
                 {{ sundial_dbt_shared.execution_ts_as_datestr() }} AS execution_ts,
                 '{{ rg }}' AS run_group_id, '{{ ck }}' AS chunk_key,
                 CAST('{{ ws }}' AS {{ sundial_dbt_shared.completions_col_type('datetime') }}) AS ws
        ) S
        ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
           AND T.chunk_key = S.chunk_key AND T.status = 'started'
        WHEN MATCHED THEN UPDATE SET start_ts = S.ws
        WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, start_ts, updated_at)
          VALUES (S.model_name, S.execution_ts, 'started', S.run_group_id, S.chunk_key, S.ws, CURRENT_TIMESTAMP())
      {%- endset -%}
      {% do run_query(sundial_dbt_shared.with_merge_retry(sql)) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{# ------------------------------------------------------------------ #}
{#  Partial-backfill bounds validation — mirrors sundial's              #}
{#  _run_partial_backfill_state_validations. A partial backfill may only #}
{#  reprocess history the table has ALREADY covered: its end_ts must not #}
{#  exceed the table's processed watermark (read_watermark). Also        #}
{#  start_ts <= end_ts and start_ts >= first_timestamp.                  #}
{#                                                                    #}
{#  Wire as the FIRST pre-hook (before acquire_run_lock) on incremental  #}
{#  models, or call from the tenant's start_ts, passing the model's      #}
{#  first_timestamp. No-op unless both backfill vars are set. Raises (the #}
{#  run fails) on a violation — matching sundial rejecting the backfill   #}
{#  state. Bounds are evaluated on the warehouse (CAST to the 'datetime' #}
{#  type) so no Python date parsing is needed.                           #}
{# ------------------------------------------------------------------ #}
{% macro validate_partial_backfill(model_name, first_timestamp=none) %}
  {%- if execute
        and var('backfill_start_ts', none) is not none
        and var('backfill_end_ts', none) is not none -%}
    {%- set dt = sundial_dbt_shared.completions_col_type('datetime') -%}
    {%- set bstart = "CAST('" ~ var('backfill_start_ts') ~ "' AS " ~ dt ~ ")" -%}
    {%- set bend = "CAST('" ~ var('backfill_end_ts') ~ "' AS " ~ dt ~ ")" -%}
    {%- set first_sql = ("CAST('" ~ first_timestamp ~ "' AS " ~ dt ~ ")") if first_timestamp is not none else "CAST(NULL AS " ~ dt ~ ")" -%}
    {%- set q -%}
      SELECT
        ({{ bstart }} > {{ bend }})                                      AS start_after_end,
        (wm IS NOT NULL AND {{ bend }} > wm)                             AS end_beyond_table,
        ({{ first_sql }} IS NOT NULL AND {{ bstart }} < {{ first_sql }}) AS start_before_first,
        wm                                                               AS wm
      FROM (SELECT ({{ sundial_dbt_shared.read_watermark(model_name) }}) AS wm)
    {%- endset -%}
    {%- set r = run_query(q) -%}
    {%- if r is not none and r.rows | length > 0 -%}
      {%- set row = r.rows[0] -%}
      {%- if row[0] -%}
        {{ exceptions.raise_compiler_error(
            "partial backfill: backfill_start_ts (" ~ var('backfill_start_ts')
            ~ ") is after backfill_end_ts (" ~ var('backfill_end_ts') ~ ") for " ~ model_name) }}
      {%- endif -%}
      {%- if row[1] -%}
        {{ exceptions.raise_compiler_error(
            "partial backfill: backfill_end_ts (" ~ var('backfill_end_ts')
            ~ ") is beyond " ~ model_name ~ "'s processed watermark (" ~ row[3]
            ~ "). A partial backfill may only reprocess already-covered history.") }}
      {%- endif -%}
      {%- if row[2] -%}
        {{ exceptions.raise_compiler_error(
            "partial backfill: backfill_start_ts (" ~ var('backfill_start_ts')
            ~ ") is before first_timestamp (" ~ first_timestamp ~ ") for " ~ model_name) }}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  SELECT 1 AS backfill_validated
{% endmacro %}

