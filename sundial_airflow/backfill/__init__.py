"""Lineage-aware chunked backfill DAG framework. Tenant DAGs call
:func:`make_backfill_dag`; other names are exported for tests and tooling."""
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
