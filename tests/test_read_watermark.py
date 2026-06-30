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

    def test_test_failure_on_scheduled_run_does_not_advance_watermark(self):
        # Normal scheduled run (single 'full' chunk), NOT chunked. The model
        # MATERIALIZED fine — its 'started' row carries the recorded end_ts
        # (record_window_end ran during the build) — but its dbt TEST FAILED, so
        # log_run_results wrote status='failed' and NO 'succeeded' row.
        # read_watermark must NOT pick this run's end_ts: with no 'succeeded'
        # sibling the run is incomplete, so the watermark stays at the last
        # TEST-PASSING run (gap-safe — next run re-processes this range).
        self.complete_run("PREV", {"full": dt(6, 4)})            # last good = 06-04
        self.add("started", "CUR", "full", we=dt(6, 5))          # built; end_ts recorded
        self.add("failed", "CUR", "full")                        # test failed -> 'failed'
        self.assertEqual(self.watermark(), dt(6, 4))             # NOT 06-05

    def test_test_failure_then_later_passing_run_advances(self):
        # The failure does not get STUCK: once a subsequent scheduled run passes
        # its tests, that complete run_group's end_ts wins via MAX and the
        # watermark advances past the failed run.
        self.complete_run("PREV", {"full": dt(6, 4)})
        self.add("started", "CUR", "full", we=dt(6, 5))
        self.add("failed", "CUR", "full")                        # 06-05 run: test failed
        self.complete_run("NEXT", {"full": dt(6, 6)})            # 06-06 run: test passed
        self.assertEqual(self.watermark(), dt(6, 6))             # advances; failed run skipped

    def test_failed_scheduled_run_then_successful_rerun_advances_for_today(self):
        # Yesterday's scheduled run FAILED for the model, then it was re-run and
        # SUCCEEDED. Today's scheduled run must read the re-run's watermark: it
        # should NOT stay stuck at the prior baseline, and the leftover 'failed'
        # row from the failed attempt must NOT poison the read.
        self.complete_run("BASE", {"full": dt(6, 1)})        # last good baseline = 06-01

        # Yesterday's run D1: model built (end_ts 06-02 recorded) but FAILED ->
        # 'started' + 'failed', no 'succeeded'.
        self.add("started", "D1", "full", we=dt(6, 2))
        self.add("failed", "D1", "full")
        self.assertEqual(self.watermark(), dt(6, 1))         # failed attempt ignored

        # Re-run under the SAME run_group (Airflow retry) now SUCCEEDS: the
        # 'started' chunk gains a 'succeeded' sibling alongside the stale 'failed'.
        self.add("succeeded", "D1", "full")
        # Today reads it correctly: D1 is complete despite the stale 'failed' row,
        # so the watermark advances to the re-run's end_ts.
        self.assertEqual(self.watermark(), dt(6, 2))

    def test_failed_scheduled_run_then_rerun_in_different_run_group_advances(self):
        # Same as above, but the re-run is a FRESH trigger under a DIFFERENT
        # run_group (not an in-place Airflow retry). The failed run_group D1 stays
        # forever-incomplete (started + failed, never gets a 'succeeded'), but that
        # dangling run_group must NOT block the watermark: read_watermark's guard
        # is scoped PER run_group, so D1 only excludes ITSELF. The fresh complete
        # run_group D2 supplies today's watermark.
        self.complete_run("BASE", {"full": dt(6, 1)})        # last good baseline = 06-01

        # Yesterday's run D1 FAILED and is never retried in place.
        self.add("started", "D1", "full", we=dt(6, 2))
        self.add("failed", "D1", "full")
        self.assertEqual(self.watermark(), dt(6, 1))         # D1 ignored

        # Re-run today under a NEW run_group D2 SUCCEEDS.
        self.complete_run("D2", {"full": dt(6, 2)})
        # D1 is still incomplete but harmless; D2 is complete -> watermark = 06-02.
        self.assertEqual(self.watermark(), dt(6, 2))

    def test_today_normal_run_reads_yesterdays_successful_watermark(self):
        # YESTERDAY: the scheduled run failed (D1), then was re-run successfully
        # under a DIFFERENT run_group (D2, end_ts 06-02) — both yesterday.
        # TODAY a normal run starts; at the moment it reads the watermark (in
        # start_ts) it must pick yesterday's SUCCESSFUL end_ts (06-02), ignoring
        # both D1's failed attempt AND today's OWN in-flight 'started' row.
        self.complete_run("BASE", {"full": dt(6, 1)})
        self.add("started", "D1", "full", we=dt(6, 2))          # yesterday: failed attempt
        self.add("failed", "D1", "full")
        self.complete_run("D2", {"full": dt(6, 2)})             # yesterday: successful rerun
        self.add("started", "TODAY", "full", we=dt(6, 3))       # today's run, in-flight
        # The latest SUCCESSFUL run for the model is D2 -> today reads 06-02.
        self.assertEqual(self.watermark(), dt(6, 2))

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

    def test_chunked_deferred_build_test_pass_advances_watermark(self):
        # Chunk builds defer terminal status; the shared model test finalizes every
        # deferred chunk_key in the run_group.
        self.complete_run("PREV", {"full": dt(6, 4)})
        self.add("started", "R", "2024-01", we=dt(6, 2))
        self.add("started", "R", "2024-02", we=dt(6, 5))
        self.assertEqual(self.watermark(), dt(6, 4))  # in-flight until test
        self.add("succeeded", "R", "2024-01")
        self.add("succeeded", "R", "2024-02")
        self.assertEqual(self.watermark(), dt(6, 5))

    def test_chunked_deferred_build_test_fail_holds_watermark(self):
        self.complete_run("PREV", {"full": dt(6, 4)})
        self.add("started", "R", "2024-01", we=dt(6, 2))
        self.add("started", "R", "2024-02", we=dt(6, 5))
        self.add("failed", "R", "2024-01")
        self.add("failed", "R", "2024-02")
        self.assertEqual(self.watermark(), dt(6, 4))

    def test_chunked_build_failure_blocks_finalize_succeeded(self):
        # One chunk failed at build time (immediate 'failed'); test pass must not
        # write 'succeeded' for that chunk — run stays incomplete.
        self.complete_run("PREV", {"full": dt(6, 4)})
        self.add("started", "R", "A", we=dt(6, 2))
        self.add("succeeded", "R", "A")
        self.add("started", "R", "B", we=dt(6, 5))
        self.add("failed", "R", "B")
        self.assertEqual(self.watermark(), dt(6, 4))


if __name__ == "__main__":
    unittest.main(verbosity=2)
