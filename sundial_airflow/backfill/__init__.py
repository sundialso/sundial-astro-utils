"""Lineage-aware chunked backfill DAG framework for Sundial dbt tenants.

Tenant DAGs import only :func:`make_backfill_dag`; the other names are
exposed for introspection (tests, ad-hoc tooling, custom DAGs).

See :mod:`sundial_airflow.backfill.dag_factory` for the factory's
docstring, and the tenant's ``BACKFILL.md`` for the operator-side
runbook.
"""
from sundial_airflow.backfill.dag_factory import make_backfill_dag
from sundial_airflow.backfill.manifest_parser import (
    CHUNKED,
    FULL_REFRESH,
    BackfillModel,
    ChunkingConfigEntry,
    compute_static_chunks,
    load_backfill_models,
    load_chunking_config,
    topological_order,
)

__all__ = [
    "make_backfill_dag",
    "BackfillModel",
    "ChunkingConfigEntry",
    "CHUNKED",
    "FULL_REFRESH",
    "load_chunking_config",
    "load_backfill_models",
    "topological_order",
    "compute_static_chunks",
]
