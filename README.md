# sundial-airflow-utils

Shared Airflow + Astronomer Cosmos utilities used by every Sundial dbt tenant
repo (`citizen_dbt`, `mirage_dbt`, `chronicle_dbt`, ...).

The goal: keep all reusable DAG plumbing here so each tenant repo only carries
its own connection IDs, schedule, and dbt project files.

## What's inside

| Module | Purpose |
| --- | --- |
| `sundial_airflow.dag_factory.make_dbt_dag` | The single entry point each tenant calls to build a fully wired Cosmos DAG. |
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
`citizen_dbt/Dockerfile` for the canonical example.)

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
    dag_id="dbt_citizen",
    tenant="citizen",
    start_date=datetime(2025, 4, 22),
    schedule="0 8 * * *",
    warehouse="bigquery",
    dbt_project_path=dbt_project_path,
    dbt_profile_name="citizen_dbt",
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

## Releasing

Push to `main`. Tenant deploys pick up the new code on their next
`pip install -r requirements.txt`. Pin tenants to a tag (e.g. `@v0.2.0`) when
you need controlled rollouts.
