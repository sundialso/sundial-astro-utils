"""Standard ``Param`` set used by every Sundial dbt DAG.

Tenants get the full param surface "for free" via :func:`build_standard_params`
and can append / override entries before passing the dict into the DAG factory.
"""
from __future__ import annotations

from typing import Literal

from airflow.models.param import Param

Warehouse = Literal["bigquery", "snowflake"]


def build_standard_params(
    *,
    warehouse: Warehouse,
    default_dataset_or_schema: str,
    target_choices: list[str] | None = None,
) -> dict[str, Param]:
    """Build the ``params`` dict every tenant DAG accepts.

    Parameters
    ----------
    warehouse:
        Either ``"bigquery"`` or ``"snowflake"``. Affects only the description
        of the dataset / schema param so the UI uses the right vocabulary.
    default_dataset_or_schema:
        Plain text shown to the user as the default value.
    target_choices:
        ``enum`` for the ``target`` param. Defaults to ``["dev"]``.
    """
    if warehouse == "bigquery":
        target_label = "BigQuery dataset"
        target_field = "dataset"
    else:
        target_label = "Snowflake schema"
        target_field = "schema"

    target_choices = target_choices or ["dev"]

    return {
        "backfill_mode": Param(
            default="none",
            type="string",
            enum=["none", "full", "partial"],
            description=(
                "none = normal incremental | "
                "full = --full-refresh from epoch | "
                "partial = backfill between start_ts and end_ts"
            ),
        ),
        "start_ts": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Partial backfill start (e.g. 2025-01-01T00:00:00). "
                "Only used when backfill_mode=partial."
            ),
        ),
        "end_ts": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Partial backfill end (e.g. 2025-06-30T23:59:59). "
                "Only used when backfill_mode=partial."
            ),
        ),
        target_field: Param(
            default=None,
            type=["null", "string"],
            description=(
                f"Target {target_label}. Leave blank for default "
                f"({default_dataset_or_schema})."
            ),
        ),
        "select": Param(
            default=None,
            type=["null", "string"],
            description=(
                "dbt model selection: 'model_a model_b', '+model' (upstream), "
                "'model+' (downstream), '+model+' (both), 'tag:daily'. "
                "Leave blank for all."
            ),
        ),
        "exclude": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Exclude models: 'model_a model_b', 'tag:wip'. "
                "Leave blank to exclude nothing."
            ),
        ),
        "execution_ts": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Execution date (e.g. 2026-03-19). Used by models and source "
                "tests. Defaults to current UTC date."
            ),
        ),
        "empty": Param(
            default=False,
            type="boolean",
            description=(
                "Validate SQL only \u2014 runs with LIMIT 0, no data processed."
            ),
        ),
        "fail_fast": Param(
            default=False,
            type="boolean",
            description=(
                "Stop on first failure. Cosmos already skips downstream "
                "dependents on failure."
            ),
        ),
        "skip_tests": Param(
            default=False,
            type="boolean",
            description=(
                "Skip all tests (source tests + model tests). "
                "Models still run normally."
            ),
        ),
        "vars": Param(
            default=None,
            type=["null", "string"],
            description=(
                'Extra dbt variables as JSON, e.g. {"key": "value"}. '
                f"Merged with {target_field}/backfill vars."
            ),
        ),
        "target": Param(
            default=target_choices[0],
            type="string",
            enum=target_choices,
            description="dbt profile target.",
        ),
    }
