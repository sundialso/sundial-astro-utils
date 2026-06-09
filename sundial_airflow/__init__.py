"""Public API for sundial-airflow-utils."""
from sundial_airflow.dag_factory import make_dbt_dag
from sundial_airflow.listeners import (
    DbtCompletionsListener,
    SundialDbtCompletionsPlugin,
)
from sundial_airflow.slack_alerts import dag_failure_alert
from sundial_airflow.warehouses import WarehouseAdapter, get_adapter, register

__all__ = [
    "make_dbt_dag",
    "dag_failure_alert",
    "DbtCompletionsListener",
    "SundialDbtCompletionsPlugin",
    # Warehouse adapter API — subclass + register to add a new warehouse.
    "WarehouseAdapter",
    "register",
    "get_adapter",
]

__version__ = "0.1.0"
