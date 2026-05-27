{# ------------------------------------------------------------------ #}
{#  Tracks per-model, per-run completion status in BigQuery.          #}
{#                                                                    #}
{#  Tenant requirements:                                              #}
{#    - vars `target_project` and `target_dataset` must be set in     #}
{#      the tenant's dbt_project.yml.                                 #}
{#    - The tenant must define an `execution_ts()` macro that returns #}
{#      a BigQuery DATETIME expression (used as the run's logical     #}
{#      timestamp). Unqualified calls below resolve to the tenant's   #}
{#      definition via dbt's macro lookup.                            #}
{#                                                                    #}
{#  Wire-up in tenant dbt_project.yml:                                #}
{#    on-run-start:                                                   #}
{#      - "{{ sundial_dbt_shared.create_dbt_completions_table() }}"   #}
{#    on-run-end:                                                     #}
{#      - "{{ sundial_dbt_shared.log_run_results() }}"                #}
{#    models:                                                         #}
{#      <project>:                                                    #}
{#        +pre-hook:  ["{{ sundial_dbt_shared.log_model_status('started') }}"]   #}
{#        +post-hook: ["{{ sundial_dbt_shared.log_model_status('succeeded') }}"] #}
{# ------------------------------------------------------------------ #}

{% macro dbt_completions_table() %}
  `{{ var('target_project') }}.{{ var('target_dataset') }}.dbt_completions`
{% endmacro %}

{% macro create_dbt_completions_table() %}
  CREATE TABLE IF NOT EXISTS {{ sundial_dbt_shared.dbt_completions_table() }} (
    model_name STRING,
    execution_ts string,
    status STRING,
    updated_at TIMESTAMP
  )
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
    MERGE {{ sundial_dbt_shared.dbt_completions_table() }} T
    USING (
      SELECT
        '{{ this.name }}' AS model_name,
        cast(cast(({{ execution_ts() }})  as date) as string) AS execution_ts,
        '{{ status }}' AS status,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.model_name = S.model_name AND T.execution_ts = S.execution_ts AND T.status = S.status
    WHEN MATCHED THEN UPDATE SET
      updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, updated_at)
      VALUES (S.model_name, S.execution_ts, S.status, S.updated_at)
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

    {% if rows | length > 0 %}
      MERGE {{ sundial_dbt_shared.dbt_completions_table() }} T
      USING (
        {% for row in rows %}
          SELECT
            '{{ row.name }}' AS model_name,
            cast(cast(({{ execution_ts() }}) as date) as string) AS execution_ts,
            '{{ row.status }}' AS status,
            CURRENT_TIMESTAMP() AS updated_at
          {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
      ) S
      ON T.model_name = S.model_name AND T.execution_ts = S.execution_ts AND T.status = S.status
      WHEN MATCHED THEN UPDATE SET
        updated_at = S.updated_at
      WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, updated_at)
        VALUES (S.model_name, S.execution_ts, S.status, S.updated_at)
    {% else %}
      SELECT 1 AS no_results_to_log
    {% endif %}
  {% else %}
    SELECT 1 AS no_results_to_log
  {% endif %}
{% endmacro %}
