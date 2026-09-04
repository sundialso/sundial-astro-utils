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
| `sundial_airflow.slack_alerts.build_failure_alert_task` | Terminal `all_done` task that posts a Slack alert listing failed tasks (skips on success). Always to `#etl-alerts`, plus any `SUNDIAL_SLACK_EXTRA_ALERT_CHANNELS`. |
| `sundial_airflow.slack_alerts.build_success_alert_task` | Terminal `all_done` task that posts a Slack success ping when no tasks failed (skips on failure or Slack errors). Always to `#pipeline-completion-alerts` only. |
| `sundial_airflow.profiles.bigquery_profile_args` | Builds the BigQuery Cosmos `profile_args` for a tenant's `get_profile_config`; adds Dataproc keys (native dbt Python models) only when `DBT_DATAPROC_REGION` + `DBT_GCS_BUCKET` are set. |
| `sundial_airflow.hooks` | `_skip_unselected` / `_skip_tests_if_disabled` pre-execute hooks. |
| `sundial_airflow.source_discovery` | Parse `sources.yml` + singular tests to find source tables that need source tests. |
| `sundial_airflow.params` | Standard `airflow.models.param.Param` set used by every tenant. |
| `sundial_airflow.run_input` | Shared parse of DAG params (`select`, backfill window, `run_context`, …) used by `prepare_dbt_args`, Slack alerts, and notify. |
| `_sundial_csid` | Snowflake partner attribution (CSID). Autoloaded at interpreter startup by `_sundial_csid.pth`; no import needed from a DAG. See below. |

### Snowflake partner attribution (CSID)

Installing this package tags Snowflake sessions opened by dbt with Sundial's
CSID, `Sundial_Analytics`. Nothing to wire up — `_sundial_csid.pth` runs at
interpreter startup and arms a lazy import hook.

`dbt-snowflake` hardcodes `application="dbt"` in its `connect` call, with no
profiles.yml key or env var to override it, and the connector's `SF_PARTNER`
fallback only fires when `application` is absent. The hook wraps
`SnowflakeConnection.__init__` and rewrites the value when it is unset or
dbt-shaped; a deliberate value from tenant code is left alone, as are calls
that resolve through `connections.toml`.

This reaches dbt because Cosmos runs it **in-process**: with dbt-core importable
alongside Airflow — which every Snowflake tenant has, via either
`astronomer-cosmos[dbt-snowflake]` or a direct `dbt-snowflake` pin —
`_discover_invocation_mode` selects `InvocationMode.DBT_RUNNER` and
`ExecutionConfig.dbt_executable_path` is not used. A tenant that drops dbt from
the Airflow environment falls back to `SUBPROCESS` against `dbt_venv`, where
this package is not installed, and attribution is lost silently.

Ground truth is `ACCOUNT_USAGE.SESSIONS.CLIENT_APPLICATION_ID`, not this repo.
If a patch ever fails it is parked rather than raised — read it with
`python -c "import _sundial_csid; print(_sundial_csid.PATCH_FAILURES)"`.

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
- Slack failure alert as a terminal `all_done` task (`slack_failure_alert`) that
  posts one message listing every failed task (skips on success).
- Slack success alert as a sibling `all_done` task (`slack_success_alert`) that
  posts to `#pipeline-completion-alerts` when no tasks failed (skips on failure).
- `tenant:<name>` DAG tag.
- The full standard parameter set (`backfill_mode`, `select`, `exclude`,
  `skip_tests`, `empty`, `vars`, `target`, ...).
- `prepare_dbt_args` task building the `--vars` blob and resolving model
  selection via `dbt ls`.
- Per-source-table `DbtTestLocalOperator`s.
- The Cosmos `DbtTaskGroup`.

## Slack alerts

Posts via Slack API connection `astro-alerts-bot`. Invite the bot into every
target channel (required for private ones).

### Failure (`slack_failure_alert`)

- Always posts to `#etl-alerts`.
- To also alert elsewhere, set `SUNDIAL_SLACK_EXTRA_ALERT_CHANNELS` on that
  deployment — comma-separated channel names or Slack IDs.
- Each extra channel is attempted independently, so a misconfigured extra still
  lets the `#etl-alerts` message through; the task then fails listing the
  channels that could not be reached.
- Skips when no tasks failed.
- Includes the `select` used (`A+`, `tag:daily`, …) or `all` when none was set.

### Success (`slack_success_alert`)

- Posts only to `#pipeline-completion-alerts` (internal; extras are ignored).
  Invite `@astro-alerts-bot` into that channel before the first deploy.
- Includes run type (`normal` / `full_backfill` / `partial_backfill`), plus
  `execution_ts` for normal and full backfill, or `start_ts` / `end_ts` for
  partial backfill.
- Includes the `select` used (`A+`, `tag:daily`, …) or `all` when none was set.
- Skips when any task failed.
- Also skips if Slack cannot be reached (bot not in the channel, Slack down),
  so a successful dbt run is not marked failed by the ping.

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
