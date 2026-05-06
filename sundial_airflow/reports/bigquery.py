"""BigQuery variant of the ``report_data_processed`` task.

Summarises bytes / time / job count per ``pipeline`` label for jobs that ran
during the current DAG run window. Best-effort: any failure is swallowed and
logged so reporting issues never fail the DAG.
"""
from __future__ import annotations

import logging
import os

from airflow.decorators import task

logger = logging.getLogger(__name__)


def make_report_task(
    *,
    bq_project: str,
    bq_location: str,
    gcp_conn_id: str,
):
    """Return a ``@task`` callable that builds the BigQuery cost report.

    Parameters
    ----------
    bq_project:
        The BigQuery project that owns the ``INFORMATION_SCHEMA`` table being
        scanned.
    bq_location:
        The region (``"US"``, ``"EU"``, ...) used to address the
        ``region-<location>`` slot.
    gcp_conn_id:
        Airflow connection ID providing GCP credentials.
    """

    @task(task_id="report_data_processed", trigger_rule="all_done", retries=0)
    def report_data_processed(**context):
        try:
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

            dag_run = context["dag_run"]
            start_iso = dag_run.start_date.isoformat()

            hook = BigQueryHook(
                gcp_conn_id=gcp_conn_id,
                use_legacy_sql=False,
                location=bq_location,
            )

            query = f"""
            SELECT
              COALESCE(
                (SELECT value FROM UNNEST(labels) WHERE key = 'pipeline'),
                '(untagged)'
              ) AS pipeline_tag,
              ROUND(SUM(total_bytes_processed) / POWER(1024, 3), 4) AS gb_processed,
              ROUND(
                SUM(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) / 1000, 1
              ) AS time_taken_s,
              COUNT(*) AS job_count
            FROM `{bq_project}`.`region-{bq_location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE creation_time >= TIMESTAMP('{start_iso}')
              AND state = 'DONE'
              AND error_result IS NULL
              AND job_type = 'QUERY'
              AND statement_type IN (
                'SELECT', 'CREATE_TABLE_AS_SELECT', 'INSERT',
                'MERGE', 'CREATE_VIEW', 'CREATE_TABLE'
              )
            GROUP BY pipeline_tag
            ORDER BY gb_processed DESC
            """

            client = hook.get_client(project_id=bq_project)
            rows = list(client.query(query).result())

            header = (
                f"{'Pipeline Tag':<40} {'GB Processed':>14} "
                f"{'Time (s)':>10} {'Jobs':>6}"
            )
            separator = "-" * len(header)
            lines = ["\n" + separator, header, separator]

            total_gb = 0.0
            total_time = 0.0
            total_jobs = 0

            for row in rows:
                name = row["pipeline_tag"] or "(untagged)"
                gb = float(row["gb_processed"] or 0)
                time_s = float(row["time_taken_s"] or 0)
                job_count = int(row["job_count"] or 0)
                lines.append(
                    f"{name:<40} {gb:>14.4f} {time_s:>10.1f} {job_count:>6}"
                )
                total_gb += gb
                total_time += time_s
                total_jobs += job_count

            lines.append(separator)
            lines.append(
                f"{'TOTAL':<40} {total_gb:>14.4f} "
                f"{total_time:>10.1f} {total_jobs:>6}"
            )
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


def resolve_bq_location(default: str = "US") -> str:
    """Resolve the BigQuery location, honouring ``DBT_BQ_LOCATION`` if set."""
    return os.environ.get("DBT_BQ_LOCATION", default)
