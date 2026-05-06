"""Public API for sundial-airflow-utils.

Tenant DAG files should import from this top-level module rather than the
private submodules so we can rearrange internals without breaking them.
"""
from sundial_airflow.dag_factory import make_dbt_dag
from sundial_airflow.slack_alerts import task_failure_alert

__all__ = ["make_dbt_dag", "task_failure_alert"]

__version__ = "0.1.0"
