# sundial-airflow-utils

Shared Airflow + Astronomer Cosmos utilities used by every Sundial dbt tenant
repo (`client_a_dbt`, `client_b_dbt`, `client_c_dbt`, ...).

The goal: keep all reusable DAG plumbing here so each tenant repo only carries
its own connection IDs, schedule, and dbt project files.

## What's inside

| Module | Purpose |
| --- | --- |
| `sundial_airflow.create_dag.create_dag` | Chunking-enabled entry point for tenants rolling out chunking. |
| `sundial_airflow.dag_factory.make_dbt_dag` | Cosmos-only factory (no chunk task groups) for all other tenants. |
| `sundial_airflow.dag_factory_legacy.make_dbt_dag_legacy` | Deprecated alias for `make_dbt_dag` (backward compat). |
| `sundial_airflow.feature_flags` | `SUNDIAL_CHUNKING_ENABLED` flag and `resolve_dag_schedules()` helper. |
| `sundial_airflow.slack_alerts.build_failure_alert_task` | Terminal `one_failed` task (added by the factories) that posts to Slack via the `sundial_slack_webhook` connection when any task fails. Runs on a worker so its logs are visible (Airflow 3 hides DAG-level callback logs). |
| `sundial_airflow.profiles.bigquery_profile_args` | Builds the BigQuery Cosmos `profile_args` for a tenant's `get_profile_config`; adds Dataproc keys (native dbt Python models) only when `DBT_DATAPROC_REGION` + `DBT_GCS_BUCKET` are set. |
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

from sundial_airflow.create_dag import create_dag
from sundial_airflow.dag_factory import make_dbt_dag
from sundial_airflow.feature_flags import is_chunking_enabled, resolve_dag_schedules

from include.constants import (
    DBT_BQ_DATASET,
    dbt_project_path,
    get_profile_config,
    venv_execution_config,
)

DAG_SCHEDULE = "0 8 * * *"
create_schedule, legacy_schedule = resolve_dag_schedules(DAG_SCHEDULE)

_COMMON = dict(
    tenant="example_client",
    start_date=datetime(2025, 4, 22),
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

dag = create_dag(
    dag_id="dbt_example_client",
    schedule=create_schedule,
    chunking_config_path=dbt_project_path.parent / "include" / "chunking_config.json",
    **_COMMON,
)

legacy_dag = make_dbt_dag(
    dag_id="dbt_example_client_legacy",
    schedule=legacy_schedule,
    **_COMMON,
)
```

The factory takes care of:
- Slack failure alert as a terminal `one_failed` task (`slack_failure_alert`)
  that posts via the `sundial_slack_webhook` connection when any task fails.
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

## Chunking

Chunking is built into `create_dag` — there is no separate backfill DAG.
Tenants without chunking keep using `dag_factory.make_dbt_dag` unchanged.
For gradual rollout, toggle per deployment with `SUNDIAL_CHUNKING_ENABLED=true`
and use `resolve_dag_schedules()` so only one of `create_dag` / `make_dbt_dag`
is scheduled at a time.
Pass `chunking_config_path` and the factory reads `target/manifest.json`,
classifies each eligible model as **chunked** (per the tenant's
`chunking_config.json`) or **full-refresh**, and decides at run time whether
each chunked model runs as a single incremental pass or fans out into mapped
`chunk_<YYYY-MM>` tasks.

The disposition depends on the run:

- **Daily / incremental** (`backfill_mode=none`): single pass, unless the gap
  between the model's watermark and today exceeds `chunk_size` months — then it
  chunks from the watermark.
- **Full backfill** (`backfill_mode=full`): always chunks from the model's
  `first_timestamp` anchor up to today.
- **Partial backfill** (`backfill_mode=partial` + `start_ts`/`end_ts`): single
  pass when the window is ≤ `chunk_size`; otherwise chunks the requested window
  on the anchor-aligned grid.

Chunk windows are anchored to each model's `first_timestamp` and stepped by
`chunk_size` months, so the same calendar range always maps to the same
`chunk_key` (idempotent re-runs).

### Tenant-side artifacts

| Path | Purpose |
| --- | --- |
| `include/chunking_config.json` | Per-tenant allowlist of `{model_name, chunking_enabled, chunk_size}` entries. **Tenant-specific** — stays in the dbt repo, never in this package. |
| `macros/start_ts.sql` + `macros/end_ts.sql` | Thin shims to `sundial_dbt_shared` incremental macros. The factory injects `backfill_start_ts` / `backfill_end_ts` per chunk. |
| `dbt_project.yml` `dispatch` | **Required for parallel chunking** — routes `dbt.make_temp_relation` to `sundial_dbt_shared.default__make_temp_relation` so each chunk builds its own `<model>__dbt_tmp__<YYYYMMDD>` staging table instead of racing on a shared one. |
| `dbt_project.yml` `+post-hook` | `{{ sundial_dbt_shared.drop_backfill_tmp_table() }}` (no-op on daily runs and on Snowflake, where the incremental materialization already drops its staging table). |

Shared chunking dbt macros (`backfill_tmp_relation`, incremental windows,
completions) live in the `sundial_dbt_shared` package inside this repo;
tenants install them via `packages.yml`.

Example tenant `dbt_project.yml` wiring:

```yaml
dispatch:
  - macro_namespace: dbt
    search_order: ['your_dbt_project', 'sundial_dbt_shared', 'dbt']

models:
  your_dbt_project:
    +post-hook:
      - "{{ sundial_dbt_shared.log_model_status('succeeded') }}"
      - "{{ sundial_dbt_shared.drop_backfill_tmp_table() }}"
```

Without the `dispatch` entry, a package-level `make_temp_relation` does **not**
reliably override dbt's built-in one, every chunk creates the same
`<model>__dbt_tmp`, and parallel chunks deadlock on the staging table (the
"waiting on transaction lock" symptom).

## Releasing

Push to `main`. Tenant deploys pick up the new code on their next
`pip install -r requirements.txt`. Pin tenants to a tag (e.g. `@v0.2.0`) when
you need controlled rollouts.
