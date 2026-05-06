"""Snowflake variant of the ``report_data_processed`` task.

Summarises bytes scanned / time per ``query_tag`` for queries against the
configured warehouse during the current DAG run window. Best-effort: failures
are swallowed and logged.
"""
from __future__ import annotations

import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


def make_report_task(
    *,
    snowflake_conn_id: str,
    warehouse: str,
):
    """Return a ``@task`` callable that builds the Snowflake cost report."""

    @task(task_id="report_data_processed", trigger_rule="all_done", retries=0)
    def report_data_processed(**context):
        try:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            dag_run = context["dag_run"]
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

            query = f"""
            SELECT
                query_tag,
                ROUND(SUM(bytes_scanned) / POWER(1024, 3), 4) AS gb_scanned,
                ROUND(SUM(TIMESTAMPDIFF(MILLISECOND, start_time, end_time)) / 1000, 1) AS time_taken_s
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => '{dag_run.start_date.isoformat()}'::TIMESTAMP_LTZ,
                END_TIME_RANGE_END   => CURRENT_TIMESTAMP()
            ))
            WHERE execution_status = 'SUCCESS'
              AND query_type IN ('SELECT', 'CREATE_TABLE_AS_SELECT', 'INSERT')
              AND warehouse_name = '{warehouse}'
            GROUP BY query_tag
            ORDER BY gb_scanned DESC
            """
            rows = hook.get_records(query)

            header = f"{'Query Tag':<50} {'GB Scanned':>13} {'Time (s)':>10}"
            separator = "-" * len(header)
            lines = ["\n" + separator, header, separator]

            total_gb = 0.0
            total_time = 0.0

            for row in rows:
                name = row[0] or "(untagged)"
                gb = float(row[1] or 0)
                time_s = float(row[2] or 0)
                lines.append(f"{name:<50} {gb:>13.4f} {time_s:>10.1f}")
                total_gb += gb
                total_time += time_s

            lines.append(separator)
            lines.append(f"{'TOTAL':<50} {total_gb:>13.4f} {total_time:>10.1f}")
            lines.append(separator)

            report = "\n".join(lines)
            logger.info(report)
            return report
        except Exception:
            logger.exception(
                "report_data_processed failed; swallowing error so the DAG "
                "run status is not affected by reporting issues."
            )
            return None

    return report_data_processed
