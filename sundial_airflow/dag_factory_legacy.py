"""Deprecated alias for ``dag_factory.make_dbt_dag``.

Tenant ``*_legacy.py`` DAG files may keep importing ``make_dbt_dag_legacy``
during the chunking rollout; it resolves to the same Cosmos-only factory.
"""
from sundial_airflow.dag_factory import make_dbt_dag as make_dbt_dag_legacy

__all__ = ["make_dbt_dag_legacy"]
