"""Shared dbt profile-arg builders for tenant repos.

Centralizes the ``profile_args`` every BigQuery tenant builds identically.
Only ``os`` is imported so the module stays importable without Airflow/Cosmos.
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

    ``threads`` defaults to ``DBT_THREADS`` (then ``4``). The Dataproc keys for
    native Python models are added only when both ``DBT_DATAPROC_REGION`` and
    ``DBT_GCS_BUCKET`` are set, so SQL-only tenants are unaffected;
    ``DBT_DATAPROC_SUBMISSION_METHOD`` defaults to ``serverless``.
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
