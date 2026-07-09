"""Shared dbt profile-arg builders for tenant repos.

Every BigQuery tenant's ``get_profile_config`` builds an identical
``profile_args`` dict for Cosmos' ``GoogleCloudServiceAccountDictProfileMapping``.
Centralizing it here means one place to evolve the profile (and to add native
dbt Python-model / Dataproc support) instead of an identical copy in every
tenant's ``include/constants.py``.

Only ``os`` is imported so this module stays importable — and unit-testable —
without Airflow or Cosmos installed. The tenant-specific values (project,
dataset, region, bucket) still come from the deployment's environment, so
sharing the builder does not centralize any tenant configuration.
"""
from __future__ import annotations

import os
from typing import Any

# Standard env-var contract shared across BigQuery tenants.
THREADS_ENV = "DBT_THREADS"
DATAPROC_REGION_ENV = "DBT_DATAPROC_REGION"
GCS_BUCKET_ENV = "DBT_GCS_BUCKET"
DATAPROC_SUBMISSION_METHOD_ENV = "DBT_DATAPROC_SUBMISSION_METHOD"

_DEFAULT_THREADS = 4
_DEFAULT_SUBMISSION_METHOD = "serverless"


def bigquery_profile_args(
    *,
    project: str,
    dataset: str,
    location: str,
    threads: int | None = None,
) -> dict[str, Any]:
    """Build ``profile_args`` for a BigQuery Cosmos profile.

    ``project`` / ``dataset`` / ``location`` are the tenant's identifiers (the
    caller already holds them as module constants). ``threads`` defaults to the
    ``DBT_THREADS`` env var, then ``4``.

    Native dbt Python models on BigQuery execute on Dataproc, which needs
    ``dataproc_region`` + ``gcs_bucket`` (and a ``submission_method``) in the
    profile. Those keys are emitted ONLY when both ``DBT_DATAPROC_REGION`` and
    ``DBT_GCS_BUCKET`` are set — so a tenant without the Dataproc infra keeps an
    unchanged SQL-only profile and there is no behavior change on upgrade until
    the env vars are provisioned. ``DBT_DATAPROC_SUBMISSION_METHOD`` defaults to
    ``serverless``.
    """
    if threads is None:
        threads = int(os.environ.get(THREADS_ENV, str(_DEFAULT_THREADS)))

    args: dict[str, Any] = {
        "project": project,
        "dataset": dataset,
        "location": location,
        "threads": threads,
    }

    region = os.environ.get(DATAPROC_REGION_ENV)
    bucket = os.environ.get(GCS_BUCKET_ENV)
    if region and bucket:
        args["submission_method"] = os.environ.get(
            DATAPROC_SUBMISSION_METHOD_ENV, _DEFAULT_SUBMISSION_METHOD
        )
        args["dataproc_region"] = region
        args["gcs_bucket"] = bucket

    return args
