"""Warehouse adapters for the ``dbt_completions`` listener.

The listener logic (which task transitions to reconcile, how to derive a
model's terminal status) is warehouse-agnostic. Everything that *is*
warehouse-specific lives here behind one small interface:

  - which Airflow connection types to auto-discover, and the fallback conn_id
  - how to build the fully-qualified, correctly-quoted completions table name
    from the ``dbt_vars`` blob
  - how to run the idempotent MERGE/upsert

Adding a new warehouse = subclass :class:`WarehouseAdapter`, implement the two
abstract methods, and :func:`register` it. No changes to ``listeners.py``.

The ``warehouse`` selector is plumbed through the ``prepare_dbt_args`` XCom by
``make_dbt_dag`` (see ``dag_factory.py``), so the listener knows which adapter
to use per DAG without any extra configuration.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Any

log = logging.getLogger(__name__)

# Single, deployment-level connection override (warehouse-agnostic). A
# per-warehouse override ``SUNDIAL_DBT_CONN_ID_<WAREHOUSE>`` takes precedence
# over this — useful when a deployment runs more than one warehouse.
_CONN_ID_ENV = "SUNDIAL_DBT_CONN_ID"


# ---------------------------------------------------------------------------
# Shared MERGE skeleton
# ---------------------------------------------------------------------------
# The MERGE structure is identical across ANSI-compatible warehouses (BigQuery,
# Snowflake, …); only identifier quoting and parameter binding differ. Each
# adapter builds the per-row ``SELECT`` fragments (with its own placeholder
# syntax) and the quoted table name, then hands them here.
def _merge_sql(table_fqn: str, row_selects: list[str]) -> str:
    # Key includes run_group_id + chunk_key to match the dbt macros' schema. The
    # dbt_completions VIEW picks the winning run_group per (model, execution_ts)
    # and joins on run_group_id, so the reconciliation row MUST carry the run's
    # run_group_id (and chunk_key) — a NULL run_group row would win the ranking by
    # updated_at but then fail the view's run_group join and drop the model.
    return f"""
    MERGE INTO {table_fqn} T
    USING (
        {" UNION ALL ".join(row_selects)}
    ) S
    ON T.model_name = S.model_name
       AND T.execution_ts = S.execution_ts
       AND T.status = S.status
       AND T.run_group_id = S.run_group_id
       AND T.chunk_key = S.chunk_key
    WHEN MATCHED THEN UPDATE SET updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT (model_name, execution_ts, status, run_group_id, chunk_key, updated_at)
      VALUES (S.model_name, S.execution_ts, S.status, S.run_group_id, S.chunk_key, S.updated_at)
    """


# ---------------------------------------------------------------------------
# Connection auto-discovery (shared, keyed by conn_type)
# ---------------------------------------------------------------------------
# Cache the lookup per warehouse name: ``{wh_name: conn_id | None}``. DAG/conn
# topology doesn't change within a worker process — restart Airflow if
# connections change.
_AUTO_CONN_CACHE: dict[str, str | None] = {}


def _autodiscover_conn(conn_types: tuple[str, ...], wh_name: str) -> str | None:
    """Find the single connection of one of ``conn_types`` in Airflow's metadata
    DB. Returns the ``conn_id`` if exactly one matches, ``None`` if zero (none
    configured) or multiple (ambiguous — disambiguate via env var). Cached.
    """
    if wh_name in _AUTO_CONN_CACHE:
        return _AUTO_CONN_CACHE[wh_name]
    result: str | None = None
    try:
        from airflow.models import Connection
        from airflow.utils.session import create_session

        with create_session() as session:
            conns = (
                session.query(Connection)
                .filter(Connection.conn_type.in_(conn_types))
                .all()
            )
        names = [c.conn_id for c in conns]
        if len(names) == 1:
            log.info(
                "DbtCompletionsListener: auto-discovered %s conn=%s", wh_name, names[0]
            )
            result = names[0]
        elif len(names) > 1:
            log.warning(
                "DbtCompletionsListener: multiple %s connections (%s); "
                "set %s_%s or %s to disambiguate",
                wh_name,
                names,
                _CONN_ID_ENV,
                wh_name.upper(),
                _CONN_ID_ENV,
            )
        else:
            log.info("DbtCompletionsListener: no %s-typed connection found", wh_name)
    except Exception as exc:
        log.debug("%s conn auto-discovery failed: %s", wh_name, exc)
    _AUTO_CONN_CACHE[wh_name] = result
    return result


# ---------------------------------------------------------------------------
# Adapter interface + registry
# ---------------------------------------------------------------------------
class WarehouseAdapter(ABC):
    """One adapter per warehouse. Subclass, set the three class attributes,
    implement :meth:`build_table_fqn` and :meth:`upsert`, then :func:`register`.
    """

    #: Matches the ``warehouse`` value plumbed through ``prepare_dbt_args``.
    name: str
    #: Airflow ``conn_type``s this warehouse uses (for auto-discovery).
    conn_types: tuple[str, ...]
    #: Last-resort conn_id when none is configured or discoverable.
    default_conn_id: str

    @abstractmethod
    def build_table_fqn(self, dbt_vars: dict[str, Any], table: str) -> str | None:
        """Return the fully-qualified, correctly-quoted completions table name
        built from the ``dbt_vars`` blob, or ``None`` if required identifiers
        (project/database/dataset/schema) are missing.
        """

    @abstractmethod
    def upsert(
        self,
        conn_id: str,
        table_fqn: str,
        execution_ts: str,
        run_group_id: str,
        rows: list[dict[str, str]],
        chunk_key: str = "full",
    ) -> None:
        """Run the idempotent MERGE writing one row per ``{model_name, status}``
        for ``(execution_ts, run_group_id, chunk_key)`` into ``table_fqn``.

        ``run_group_id`` ties the reconciliation row to the run's other rows
        (started / failed) so the dbt_completions view rolls it up correctly.
        ``chunk_key`` is ``"full"`` for non-chunked models (all current tenants).
        """

    def resolve_conn_id(self) -> str:
        """Per-warehouse env override → global env override → auto-discovery →
        hardcoded default.
        """
        per_wh = os.environ.get(f"{_CONN_ID_ENV}_{self.name.upper()}")
        if per_wh:
            return per_wh
        explicit = os.environ.get(_CONN_ID_ENV)
        if explicit:
            return explicit
        auto = _autodiscover_conn(self.conn_types, self.name)
        return auto or self.default_conn_id


_REGISTRY: dict[str, WarehouseAdapter] = {}


def register(adapter: WarehouseAdapter) -> None:
    _REGISTRY[adapter.name] = adapter


def get_adapter(name: str) -> WarehouseAdapter | None:
    return _REGISTRY.get(name)


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
class BigQueryAdapter(WarehouseAdapter):
    name = "bigquery"
    conn_types = ("google_cloud_platform", "gcpbigquery")
    default_conn_id = "google_cloud_default"

    def build_table_fqn(self, dbt_vars: dict[str, Any], table: str) -> str | None:
        project = dbt_vars.get("target_project")
        dataset = dbt_vars.get("target_dataset")
        if not (project and dataset):
            return None
        return f"`{project}.{dataset}.{table}`"

    def upsert(
        self,
        conn_id: str,
        table_fqn: str,
        execution_ts: str,
        run_group_id: str,
        rows: list[dict[str, str]],
        chunk_key: str = "full",
    ) -> None:
        if not rows:
            return
        try:
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
            from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
        except ImportError:
            log.warning(
                "apache-airflow-providers-google not installed; dbt_completions "
                "reconciliation skipped for %s",
                table_fqn,
            )
            return

        selects: list[str] = []
        params: list[Any] = [
            ScalarQueryParameter("e", "STRING", execution_ts),
            ScalarQueryParameter("rg", "STRING", run_group_id),
            ScalarQueryParameter("ck", "STRING", chunk_key),
        ]
        for i, row in enumerate(rows):
            selects.append(
                f"SELECT @m{i} AS model_name, @e AS execution_ts, @s{i} AS status, "
                f"@rg AS run_group_id, @ck AS chunk_key, CURRENT_TIMESTAMP() AS updated_at"
            )
            params.append(ScalarQueryParameter(f"m{i}", "STRING", row["model_name"]))
            params.append(ScalarQueryParameter(f"s{i}", "STRING", row["status"]))

        sql = _merge_sql(table_fqn, selects)
        hook = BigQueryHook(gcp_conn_id=conn_id, use_legacy_sql=False)
        client = hook.get_client()
        client.query(sql, job_config=QueryJobConfig(query_parameters=params)).result()


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------
class SnowflakeAdapter(WarehouseAdapter):
    name = "snowflake"
    conn_types = ("snowflake",)
    default_conn_id = "snowflake_default"

    def build_table_fqn(self, dbt_vars: dict[str, Any], table: str) -> str | None:
        schema = dbt_vars.get("target_schema")
        if not schema:
            return None
        # The database part is optional: prefer an explicit ``target_database``,
        # fall back to ``target_project`` (the shared field ``make_dbt_dag``
        # fills from ``default_project``). If neither is set, emit a two-part
        # name and let Snowflake resolve the database from the connection/role.
        database = dbt_vars.get("target_database") or dbt_vars.get("target_project")
        if database:
            return f"{database}.{schema}.{table}"
        return f"{schema}.{table}"

    def upsert(
        self,
        conn_id: str,
        table_fqn: str,
        execution_ts: str,
        run_group_id: str,
        rows: list[dict[str, str]],
        chunk_key: str = "full",
    ) -> None:
        if not rows:
            return
        try:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        except ImportError:
            log.warning(
                "apache-airflow-providers-snowflake not installed; dbt_completions "
                "reconciliation skipped for %s",
                table_fqn,
            )
            return

        # Snowflake's connector uses pyformat (``%(name)s``) parameter binding.
        selects: list[str] = []
        params: dict[str, str] = {"e": execution_ts, "rg": run_group_id, "ck": chunk_key}
        for i, row in enumerate(rows):
            selects.append(
                f"SELECT %(m{i})s AS model_name, %(e)s AS execution_ts, %(s{i})s AS status, "
                f"%(rg)s AS run_group_id, %(ck)s AS chunk_key, CURRENT_TIMESTAMP() AS updated_at"
            )
            params[f"m{i}"] = row["model_name"]
            params[f"s{i}"] = row["status"]

        sql = _merge_sql(table_fqn, selects)
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        hook.run(sql, parameters=params)


# Register the built-in adapters at import time.
register(BigQueryAdapter())
register(SnowflakeAdapter())
