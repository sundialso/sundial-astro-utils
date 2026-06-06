#!/usr/bin/env python3
"""Unit tests for the incremental watermark read (read_watermark).

read_watermark returns MAX(end_ts) over a model's COMPLETE run_groups —
run_groups where every 'started' chunk has a 'succeeded' sibling. A run with any
failed / in-flight chunk contributes NOTHING (gap-safe), and MAX gives no-regress.

These tests run that exact SQL on DuckDB against a seeded dbt_completions_raw.

IMPORTANT — keep WATERMARK_SQL below in sync with
``sundial_dbt_shared/macros/dbt_completions.sql :: read_watermark``. It is copied
verbatim except the table FQN -> the literal ``dbt_completions_raw`` and the
model name -> a format placeholder.

Run:  python3 tests/test_read_watermark.py
"""
from __future__ import annotations

import unittest
from datetime import datetime

import duckdb

# Mirror of read_watermark's body. {model} is the target model_name.
WATERMARK_SQL = """
SELECT MAX(w.end_ts)
FROM dbt_completions_raw w
WHERE w.model_name = '{model}'
  AND w.status = 'started'
  AND w.end_ts IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dbt_completions_raw u
    WHERE u.model_name = w.model_name
      AND u.run_group_id = w.run_group_id
      AND u.status = 'started'
      AND NOT EXISTS (
        SELECT 1 FROM dbt_completions_raw s
        WHERE s.model_name = u.model_name AND s.run_group_id = u.run_group_id
          AND s.chunk_key = u.chunk_key AND s.status = 'succeeded'
      )
  )
"""

MODEL = "orders"


def dt(month, day):
    return datetime(2026, month, day, 0, 0, 0)


class WatermarkTestCase(unittest.TestCase):
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

    def add(self, status, rg, chunk, we=None, model=MODEL):
        """Insert one row. end_ts (we) is set only on the 'started' rows
        (that's where record_window_end writes it)."""
        self.con.execute(
            "INSERT INTO dbt_completions_raw "
            "(model_name, execution_ts, status, updated_at, run_group_id, chunk_key, "
            " heartbeat_at, start_ts, end_ts) "
            "VALUES (?, '2026-06-05', ?, NULL, ?, ?, NULL, NULL, ?)",
            [model, status, rg, chunk, we],
        )

    def watermark(self, model=MODEL):
        return self.con.execute(WATERMARK_SQL.format(model=model)).fetchone()[0]

    # -- a fully-succeeded chunked run helper ------------------------------
    def complete_run(self, rg, chunk_ends):
        """chunk_ends: {chunk_key: window_end datetime}. Writes a started row
        (carrying window_end) + a succeeded sibling for each chunk."""
        for ck, we in chunk_ends.items():
            self.add("started", rg, ck, we=we)
            self.add("succeeded", rg, ck)

    # -- tests ------------------------------------------------------------
    def test_complete_single_run_returns_max_end(self):
        self.complete_run("R1", {"A": dt(6, 2), "B": dt(6, 6)})
        self.assertEqual(self.watermark(), dt(6, 6))  # MAX across chunks

    def test_partial_chunk_failure_excludes_whole_run(self):
        # Prior complete run covered up to 05-31.
        self.complete_run("P", {"full": dt(5, 31)})
        # Run R: A & C succeed (C has the MAX end_ts), B FAILS.
        self.add("started", "R", "A", we=dt(6, 2)); self.add("succeeded", "R", "A")
        self.add("started", "R", "B", we=dt(6, 4)); self.add("failed", "R", "B")
        self.add("started", "R", "C", we=dt(6, 6)); self.add("succeeded", "R", "C")
        # Even though C (06-06) passed, R is incomplete -> excluded entirely.
        self.assertEqual(self.watermark(), dt(5, 31))

    def test_retry_completes_run_then_advances(self):
        self.complete_run("P", {"full": dt(5, 31)})
        self.add("started", "R", "A", we=dt(6, 2)); self.add("succeeded", "R", "A")
        self.add("started", "R", "B", we=dt(6, 4)); self.add("failed", "R", "B")
        self.add("started", "R", "C", we=dt(6, 6)); self.add("succeeded", "R", "C")
        # Airflow retries chunk B under the SAME run_group; it now succeeds.
        self.add("succeeded", "R", "B")
        self.assertEqual(self.watermark(), dt(6, 6))  # R complete -> MAX advances

    def test_no_regress_partial_backfill(self):
        # A normal run reached 06-10; a completed partial backfill only covered 06-03.
        self.complete_run("NORMAL", {"full": dt(6, 10)})
        self.complete_run("BACKFILL", {"full": dt(6, 3)})
        self.assertEqual(self.watermark(), dt(6, 10))  # MAX, never regresses

    def test_multiple_complete_runs_take_max(self):
        self.complete_run("R1", {"full": dt(6, 5)})
        self.complete_run("R2", {"full": dt(6, 8)})
        self.assertEqual(self.watermark(), dt(6, 8))

    def test_no_successful_run_returns_null(self):
        # Only a failed run exists.
        self.add("started", "R", "full", we=dt(6, 5)); self.add("failed", "R", "full")
        self.assertIsNone(self.watermark())

    def test_in_flight_run_returns_null(self):
        # started, no terminal yet.
        self.add("started", "R", "full", we=dt(6, 5))
        self.assertIsNone(self.watermark())

    def test_in_flight_does_not_regress_existing_watermark(self):
        # A complete run set 06-08; a new run is mid-flight (started only).
        self.complete_run("DONE", {"full": dt(6, 8)})
        self.add("started", "NEW", "full", we=dt(6, 9))  # in flight, no succeeded
        self.assertEqual(self.watermark(), dt(6, 8))  # NEW excluded until it completes

    def test_other_models_isolated(self):
        self.complete_run("R1", {"full": dt(6, 5)})
        # A different model with a later watermark must not leak into MODEL's read.
        self.add("started", "OTHER_RG", "full", we=dt(6, 30), model="other")
        self.add("succeeded", "OTHER_RG", "full", model="other")
        self.assertEqual(self.watermark(MODEL), dt(6, 5))
        self.assertEqual(self.watermark("other"), dt(6, 30))

    def test_null_run_group_id_does_not_poison_watermark(self):
        # A stray 'started' row with run_group_id = NULL (e.g. a legacy row) must NOT
        # blank the watermark. The null-safe NOT EXISTS predicate ignores it and still
        # returns the complete run's watermark. (With the old NOT IN, the NULL poisoned
        # the predicate and this returned NULL.)
        self.complete_run("R1", {"full": dt(6, 5)})
        self.add("started", None, "full", we=None)  # legacy NULL-run_group row
        self.assertEqual(self.watermark(), dt(6, 5))


if __name__ == "__main__":
    unittest.main(verbosity=2)
