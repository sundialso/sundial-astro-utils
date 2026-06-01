"""``BACKFILL_AUDIT`` writers — abstract base + one subclass per warehouse.

The factory picks a subclass by warehouse. Writers wrap a hook and stay
Airflow-free, so they unit-test with ``MagicMock``.
"""
from ._base import AUDIT_TABLE, AuditWriter, validate_schema
from .bigquery import BigQueryAuditWriter
from .snowflake import SnowflakeAuditWriter

__all__ = [
    "AUDIT_TABLE",
    "AuditWriter",
    "validate_schema",
    "SnowflakeAuditWriter",
    "BigQueryAuditWriter",
]
