# sundial-airflow-utils

Shared Airflow + Astronomer Cosmos utilities used by every Sundial dbt tenant
repo (`client_a_dbt`, `client_b_dbt`, `client_c_dbt`, ...).

The goal: keep all reusable DAG plumbing here so each tenant repo only carries
its own connection IDs, schedule, and dbt project files.

## What's inside

| Module | Purpose |
| --- | --- |
| `sundial_airflow.dag_factory.make_dbt_dag` | The single entry point each tenant calls to build a fully wired Cosmos DAG. |
| `sundial_airflow.backfill.make_backfill_dag` | Lineage-preserving chunked backfill DAG factory (Snowflake + BigQuery). Mirrors dbt lineage one-for-one; per-model chunking via a tenant-side `chunking_config.json`. See [Backfill framework](#backfill-framework) below. |
| `sundial_airflow.slack_alerts.dag_failure_alert` | `on_failure_callback` that posts to Slack via the `sundial_slack_webhook` connection. |
| `sundial_airflow.hooks` | `_skip_unselected` / `_skip_tests_if_disabled` pre-execute hooks. |
| `sundial_airflow.source_discovery` | Parse `sources.yml` + singular tests to find source tables that need source tests. |
| `sundial_airflow.params` | Standard `airflow.models.param.Param` set used by every tenant. |

## Using it from a tenant repo

In `requirements.txt`:

```
sundial-airflow-utils @ git+https://github.com/sundial-astro-sdk/sundial-astro-utils.git@main
```

(Or, if you're using the BuildKit secrets pattern recommended for tenant
Dockerfiles, install the SDK from a Dockerfile `RUN` step instead. See
your tenant's `Dockerfile` for the canonical example.)

In a DAG file:

```python
from datetime import timedelta
from pendulum import datetime

from sundial_airflow.dag_factory import make_dbt_dag

from include.constants import (
    DBT_BQ_DATASET,
    dbt_project_path,
    get_profile_config,
    venv_execution_config,
)

dag = make_dbt_dag(
    dag_id="dbt_example_client",
    tenant="example_client",
    start_date=datetime(2025, 4, 22),
    schedule="0 8 * * *",
    warehouse="bigquery",
    dbt_project_path=dbt_project_path,
    dbt_profile_name="example_client_dbt",
    venv_execution_config=venv_execution_config,
    profile_config_factory=get_profile_config,
    default_dataset_or_schema=DBT_BQ_DATASET,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=3),
    },
)
```

The factory takes care of:
- Slack failure alert wired into `default_args["on_failure_callback"]`.
- `tenant:<name>` DAG tag.
- The full standard parameter set (`backfill_mode`, `select`, `exclude`,
  `skip_tests`, `empty`, `vars`, `target`, ...).
- `prepare_dbt_args` task building the `--vars` blob and resolving model
  selection via `dbt ls`.
- Per-source-table `DbtTestLocalOperator`s.
- The Cosmos `DbtTaskGroup`.

## Local development

When iterating on the SDK and a tenant repo together, install the SDK in
editable mode into the tenant's venv:

```bash
pip install -e ~/Documents/sundial-airflow-utils
```

That overrides the `git+https://...` URL from `requirements.txt` until you run
`pip install -r requirements.txt --force-reinstall` again.

## Backfill framework

A separate factory builds **manually-triggered, lineage-preserving
chunked backfill DAGs**. The factory reads
`target/manifest.json`, classifies every eligible model as
**chunked** (per the tenant's `chunking_config.json`) or **full-refresh**
(everything else), and emits a DAG whose graph mirrors the dbt lineage
one-for-one — each model becomes a `TaskGroup` whose shape depends on
its kind.

dbt runs target whatever warehouse `backfill_profile_config` points at,
so the framework works for both **Snowflake** and **BigQuery** tenants.
Only the optional audit-table writer is warehouse-specific; pass
`warehouse="snowflake"` (default) or `warehouse="bigquery"` to pick the
matching `BACKFILL_AUDIT` dialect.

```python
from pendulum import datetime

from sundial_airflow import dag_failure_alert
from sundial_airflow.backfill import make_backfill_dag

from include.constants import (
    AIRFLOW_HOME,
    DBT_SNOWFLAKE_CONN_ID,
    dbt_project_path,
    get_backfill_profile_config,
    venv_execution_config,
)

dag = make_backfill_dag(
    dag_id="backfill_example_tenant",
    tenant="example_tenant",
    start_date=datetime(2026, 1, 1),
    dbt_project_path=dbt_project_path,
    dbt_profile_name="example_tenant_dbt",
    venv_execution_config=venv_execution_config,
    backfill_profile_config=get_backfill_profile_config(),
    chunking_config_path=AIRFLOW_HOME / "include" / "chunking_config.json",
    warehouse="snowflake",
    warehouse_conn_id=DBT_SNOWFLAKE_CONN_ID,
    audit_schema="DBT_BACKFILLS",
    base_vars={"target_schema": "DBT_BACKFILLS"},
    max_active_tasks=44,
    on_failure_callback=dag_failure_alert,
)
```

For a **BigQuery** tenant, swap the warehouse, warehouse connection, and the
`base_vars` key dbt expects (`target_dataset` instead of `target_schema`);
`audit_schema` then names the BigQuery **dataset** for `BACKFILL_AUDIT`. Set
`bq_location` to the dataset's region — audit query jobs default to `US`
otherwise:

```python
dag = make_backfill_dag(
    ...,
    warehouse="bigquery",
    warehouse_conn_id=DBT_BIGQUERY_CONN_ID,   # a GCP connection
    bq_location="US",
    bq_audit_project="your-gcp-project",      # where the backfill dataset lives
    bq_audit_create_dataset=False,            # default; needs datasets.create if True
    audit_schema="dbt_backfills",
    base_vars={"target_dataset": "dbt_backfills"},
)
```

Install the matching provider extra: `pip install sundial-airflow-utils[snowflake]`
or `pip install sundial-airflow-utils[bigquery]`. `warehouse_conn_id=None` disables
auditing entirely.

### How it works

- **Lineage-preserving graph.** Every inter-model edge from
  `manifest.depends_on` is wired into the DAG; the graph view matches
  the daily Cosmos DAG, with one TaskGroup per model.
- **Two task shapes.**
  - *Full-refresh* (default): one `dbt run --select <name>` per model.
  - *Chunked* (opt-in): one `run_chunks` task per model that loops N
    date windows sequentially (computed at DAG parse time from the
    model's `start_ts(<col>, <lookback>, '<first_ts>')` anchor up to
    today, divided by `chunk_size` months in `chunking_config.json`).
    Windows for the same model never run in parallel — this avoids
    BigQuery `insert_overwrite` races on `<model>__dbt_tmp`. Different
    models still run concurrently up to `max_active_tasks`.
- **Strict allowlist.** A model becomes chunked iff (1) it has an
  enabled entry in `chunking_config.json` with a positive `chunk_size`
  AND (2) its SQL contains a `start_ts(...)` call. Anything else runs
  full-refresh.
- **`select` Param.** Operators can scope a run to a dbt-style selector
  (`model+`, `+model+`, etc.) at trigger time; unselected models skip
  via `trigger_rule="none_failed"` without breaking downstream tasks.
- **Audit trail.** Each successful chunk writes one row to
  `<audit_schema>.BACKFILL_AUDIT` (auto-created). Reporting is
  ad-hoc — operators run `dbt run-operation backfill_report` (or
  `backfill_warehouse_report`) against the audit table after the run
  completes.
- **Manual only.** `schedule=None`, `max_active_runs=1`.

### Tenant-side artifacts

Each tenant repo also needs:

| Path | Purpose |
| --- | --- |
| `include/chunking_config.json` | Per-tenant allowlist of `{model_name, chunking_enabled, chunk_size}` entries. **Tenant-specific** — stays in the dbt repo, never in this package. |
| `macros/start_ts.sql` + `macros/end_ts.sql` | Temporal window macros. The factory injects `backfill_start_ts` / `backfill_end_ts` dbt vars that these macros consume. |
| `macros/backfill_coverage.sql` | `dbt run-operation` for previewing chunk eligibility. |
| `macros/backfill_report.sql` + `macros/backfill_warehouse_report.sql` | *Optional* — ad-hoc reporting macros run manually against `BACKFILL_AUDIT`. Not invoked by the DAG. |
| `dags/backfill_<tenant>.py` | ~30-line file that calls `make_backfill_dag`. |
| A dedicated compute resource (Snowflake warehouse / BigQuery reservation or project) | Sized for the backfill workload; **never reuse production compute**. |

The macros are currently copy-pasted across tenant repos. A planned
follow-up is to ship them as a dbt package (`sundial-dbt-utils`) so
tenants get them via `packages.yml`.

See `<tenant>_dbt/BACKFILL.md` in your tenant repo for the operator-side runbook.

## Releasing

Push to `main`. Tenant deploys pick up the new code on their next
`pip install -r requirements.txt`. Pin tenants to a tag (e.g. `@v0.2.0`) when
you need controlled rollouts.
