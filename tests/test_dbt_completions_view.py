#!/usr/bin/env python3
"""Unit tests for the dbt_completions VIEW rollup logic (create_dbt_completions_view).

The view is warehouse-agnostic standard SQL (CTEs + window functions). These tests
run that exact rollup logic on DuckDB against a seeded dbt_completions_raw table and
assert the collapsed one-row-per-(model, execution_ts) output.

IMPORTANT — keep ROLLUP_SQL below in sync with
``sundial_dbt_shared/macros/dbt_completions.sql :: create_dbt_completions_view``.
Only two warehouse-specific pieces are substituted for DuckDB:
  - the table FQN          -> the literal table name ``dbt_completions_raw``
  - execution_ts_days_ago  -> CAST((CURRENT_DATE - INTERVAL 'N days') AS VARCHAR)
Everything else (the CTE chain, the rollup CASE, the window MIN/MAX, the chunk_key
'__all__' sentinel) is copied verbatim, so a logic regression here = a real bug.

Run:  python3 tests/test_dbt_completions_view.py
"""
from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta

import duckdb

# ---------------------------------------------------------------------------
# The view's rollup query — mirror of create_dbt_completions_view's body.
# {days} is the dbt_completions_view_days window (default 30).
# ---------------------------------------------------------------------------
ROLLUP_SQL = """
WITH live AS (
  SELECT model_name, execution_ts, run_group_id, chunk_key, status, updated_at,
         start_ts, end_ts
  FROM dbt_completions_raw
  WHERE status NOT IN ('locked_out', 'crashed')
    AND execution_ts >= CAST((CURRENT_DATE - INTERVAL '{days} days') AS VARCHAR)
),
ranked AS (
  SELECT
    model_name, execution_ts, run_group_id,
    ROW_NUMBER() OVER (
      PARTITION BY model_name, execution_ts
      ORDER BY MAX(updated_at) DESC, run_group_id DESC
    ) AS _rn
  FROM live
  GROUP BY model_name, execution_ts, run_group_id
),
winner AS (
  SELECT model_name, execution_ts, run_group_id
  FROM ranked WHERE _rn = 1
),
win_rows AS (
  SELECT l.*
  FROM live l
  JOIN winner w
    ON l.model_name = w.model_name
   AND l.execution_ts = w.execution_ts
   AND l.run_group_id = w.run_group_id
),
chunk_final AS (
  SELECT model_name, execution_ts, chunk_key, status
  FROM (
    SELECT model_name, execution_ts, chunk_key, status,
      ROW_NUMBER() OVER (
        PARTITION BY model_name, execution_ts, chunk_key ORDER BY updated_at DESC
      ) AS _crn
    FROM win_rows
  ) c
  WHERE _crn = 1
),
status_roll AS (
  SELECT model_name, execution_ts,
    CASE
      WHEN SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) > 0 THEN 'failed'
      WHEN SUM(CASE WHEN status = 'started' THEN 1 ELSE 0 END) > 0 THEN 'started'
      ELSE 'succeeded'
    END AS status
  FROM chunk_final
  GROUP BY model_name, execution_ts
),
win_agg AS (
  SELECT model_name, execution_ts,
    MAX(run_group_id) AS run_group_id,
    CASE WHEN COUNT(DISTINCT chunk_key) = 1 THEN MAX(chunk_key) ELSE '__all__' END AS chunk_key,
    MIN(start_ts) AS start_ts,
    MAX(end_ts)   AS end_ts,
    MAX(updated_at)      AS updated_at
  FROM win_rows
  GROUP BY model_name, execution_ts
)
SELECT
  a.model_name,
  a.execution_ts,
  s.status,
  a.run_group_id,
  a.chunk_key,
  a.start_ts,
  a.end_ts,
  a.updated_at
FROM win_agg a
JOIN status_roll s
  ON a.model_name = s.model_name AND a.execution_ts = s.execution_ts
ORDER BY a.model_name, a.execution_ts
"""

TODAY = date.today()
D = lambda n: (TODAY - timedelta(days=n)).isoformat()  # noqa: E731  execution_ts n days ago
_BASE = datetime(2026, 1, 1, 0, 0, 0)
_T = lambda tick: _BASE + timedelta(minutes=tick)  # noqa: E731  monotonic updated_at


class ViewTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.con = duckdb.connect(":memory:")
        self.con.execute(
            """
            CREATE TABLE dbt_completions_raw (
              model_name      VARCHAR,
              execution_ts    VARCHAR,
              status          VARCHAR,
              updated_at      TIMESTAMP,
              run_group_id    VARCHAR,
              chunk_key       VARCHAR,
              heartbeat_at    TIMESTAMP,
              start_ts TIMESTAMP,
              end_ts   TIMESTAMP
            )
            """
        )

    def tearDown(self) -> None:
        self.con.close()

    # -- helpers ----------------------------------------------------------
    def add(self, model, status, rg, chunk, tick, execution_ts=None, ws=None, we=None):
        """Insert one raw status-transition row. ``tick`` is a monotonic int that
        becomes updated_at (higher = later). ws/we are window_start/end datetimes."""
        self.con.execute(
            "INSERT INTO dbt_completions_raw "
            "(model_name, execution_ts, status, updated_at, run_group_id, chunk_key, "
            " heartbeat_at, start_ts, end_ts) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            [model, execution_ts or D(0), status, _T(tick), rg, chunk, ws, we],
        )

    def view(self, days=30):
        cols = [
            "model_name", "execution_ts", "status", "run_group_id",
            "chunk_key", "start_ts", "end_ts", "updated_at",
        ]
        rows = self.con.execute(ROLLUP_SQL.format(days=days)).fetchall()
        return [dict(zip(cols, r)) for r in rows]

    def one(self, days=30):
        rows = self.view(days=days)
        self.assertEqual(len(rows), 1, f"expected exactly 1 view row, got {rows}")
        return rows[0]

    # -- tests ------------------------------------------------------------
    def test_single_normal_run_succeeded(self):
        self.add("orders", "started", "R1", "full", 1)
        self.add("orders", "succeeded", "R1", "full", 2)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")
        self.assertEqual(r["chunk_key"], "full")
        self.assertEqual(r["run_group_id"], "R1")

    def test_chunked_one_chunk_failed_is_failed(self):
        for ck in ("A", "B", "C"):
            self.add("ev", "started", "R1", ck, 1)
        self.add("ev", "succeeded", "R1", "A", 2)
        self.add("ev", "failed", "R1", "B", 3)
        self.add("ev", "succeeded", "R1", "C", 2)
        r = self.one()
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["chunk_key"], "__all__")  # spans chunks

    def test_first_run_chunk_failed_second_run_passed(self):
        # Run 1 (chunked): chunk B failed.  Run 2 (unchunked): passed, later.
        for ck in ("A", "B", "C"):
            self.add("orders", "started", "R1", ck, 1)
        self.add("orders", "succeeded", "R1", "A", 2)
        self.add("orders", "failed", "R1", "B", 3)
        self.add("orders", "succeeded", "R1", "C", 2)
        self.add("orders", "started", "R2", "full", 4)
        self.add("orders", "succeeded", "R2", "full", 5)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")  # latest run wins
        self.assertEqual(r["run_group_id"], "R2")
        self.assertEqual(r["chunk_key"], "full")

    def test_retry_within_same_run_group_failed_then_succeeded(self):
        # Same run_group: first attempt failed, retry succeeded -> latest per chunk wins.
        self.add("orders", "started", "R1", "full", 1)
        self.add("orders", "failed", "R1", "full", 2)
        self.add("orders", "succeeded", "R1", "full", 3)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")

    def test_in_flight_is_started(self):
        self.add("orders", "started", "R1", "full", 1)
        r = self.one()
        self.assertEqual(r["status"], "started")

    def test_chunked_one_in_flight_rest_succeeded_is_started(self):
        for ck in ("A", "B"):
            self.add("orders", "started", "R1", ck, 1)
        self.add("orders", "succeeded", "R1", "A", 2)  # B still only started
        r = self.one()
        self.assertEqual(r["status"], "started")

    def test_locked_out_does_not_mask_real_run(self):
        # Older real run succeeded; newer run only produced a locked_out row.
        self.add("orders", "started", "R1", "full", 1)
        self.add("orders", "succeeded", "R1", "full", 2)
        self.add("orders", "locked_out", "R2", "full", 3)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")
        self.assertEqual(r["run_group_id"], "R1")

    def test_crashed_rows_excluded(self):
        self.add("orders", "started", "R1", "full", 1)
        self.add("orders", "succeeded", "R1", "full", 2)
        self.add("orders", "crashed", "R2", "full", 3)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")

    def test_succeeded_only_no_started_row(self):
        # Regression for the old count-equality bug: a succeeded row with no
        # started row must read 'succeeded', not 'started'.
        self.add("orders", "succeeded", "R1", "full", 1)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")

    def test_stale_model_appears_at_old_execution_ts(self):
        self.add("orders", "started", "R1", "full", 1, execution_ts=D(2))
        self.add("orders", "succeeded", "R1", "full", 2, execution_ts=D(2))
        r = self.one()
        self.assertEqual(r["execution_ts"], D(2))
        self.assertEqual(r["status"], "succeeded")

    def test_older_than_window_is_excluded(self):
        self.add("orders", "started", "R1", "full", 1, execution_ts=D(40))
        self.add("orders", "succeeded", "R1", "full", 2, execution_ts=D(40))
        self.assertEqual(self.view(days=30), [])  # outside 30-day window

    def test_one_row_per_execution_ts(self):
        # Failed run on an older date, succeeded run on a newer date -> BOTH dates
        # appear, one row each (grain = one row per (model, execution_ts)).
        self.add("orders", "started", "R1", "full", 1, execution_ts=D(3))
        self.add("orders", "failed", "R1", "full", 2, execution_ts=D(3))
        self.add("orders", "started", "R2", "full", 3, execution_ts=D(1))
        self.add("orders", "succeeded", "R2", "full", 4, execution_ts=D(1))
        rows = {r["execution_ts"]: r["status"] for r in self.view()}
        self.assertEqual(rows, {D(3): "failed", D(1): "succeeded"})

    def test_window_min_max_across_chunks(self):
        # window_*_ts live on the started rows; rollup takes MIN(start), MAX(end).
        ws1, we1 = datetime(2026, 6, 1), datetime(2026, 6, 2)
        ws2, we2 = datetime(2026, 6, 3), datetime(2026, 6, 5)
        self.add("inc", "started", "R1", "A", 1, ws=ws1, we=we1)
        self.add("inc", "started", "R1", "B", 1, ws=ws2, we=we2)
        self.add("inc", "succeeded", "R1", "A", 2)
        self.add("inc", "succeeded", "R1", "B", 2)
        r = self.one()
        self.assertEqual(r["status"], "succeeded")
        self.assertEqual(r["start_ts"], ws1)  # MIN
        self.assertEqual(r["end_ts"], we2)    # MAX

    def test_non_incremental_has_null_window(self):
        self.add("dim", "started", "R1", "full", 1)
        self.add("dim", "succeeded", "R1", "full", 2)
        r = self.one()
        self.assertIsNone(r["start_ts"])
        self.assertIsNone(r["end_ts"])

    def test_multiple_models_each_one_row(self):
        self.add("a", "started", "R1", "full", 1)
        self.add("a", "succeeded", "R1", "full", 2)
        self.add("b", "started", "R1", "full", 1)
        self.add("b", "failed", "R1", "full", 2)
        rows = {r["model_name"]: r["status"] for r in self.view()}
        self.assertEqual(rows, {"a": "succeeded", "b": "failed"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
