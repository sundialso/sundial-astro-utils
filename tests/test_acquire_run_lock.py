#!/usr/bin/env python3
"""Unit tests for the cross-run lock (acquire_run_lock).

The lock is realized purely on the per-(model, run_group, chunk) 'started' row +
terminal rows — no separate lock table. acquire_run_lock does, in order:
  1. reclaim : sweep stale 'started' rows (heartbeat older than TTL, no terminal)
               to a 'crashed' terminal, freeing a crashed run's lock.
  2. write   : upsert MY 'started' row (revive my own 'locked_out' on retry).
  3. look    : find a FOREIGN run_group that HOLDS the model (a 'started' chunk
               with no terminal) and has PRIORITY (earlier updated_at; ties by
               smaller run_group_id). If found -> I lose: flip my row to
               'locked_out' and raise.

Write-first-then-look + a single warehouse clock ⇒ at least one run sees the
other and EXACTLY the earliest proceeds (no double-run, no mutual lockout).

These tests embed the macro's actual MERGE / verify SQL on DuckDB, substituting
only CURRENT_TIMESTAMP() with a controllable timestamp so races/ties are
deterministic, and the table FQN with the literal table name.

IMPORTANT — keep the SQL in the _write_started / _look / _lockout / _reclaim
helpers in sync with
``sundial_dbt_shared/macros/dbt_completions.sql :: acquire_run_lock``.

Run:  python3 tests/test_acquire_run_lock.py
"""
from __future__ import annotations

import unittest

import duckdb

MODEL = "m"
EXEC_TS = "2026-06-05"
TERMINAL = "('succeeded','failed','locked_out','crashed')"


def ts(hh, mm=0):
    return f"2026-06-05 {hh:02d}:{mm:02d}:00"


class LockTestCase(unittest.TestCase):
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

    # -- macro-mirroring steps -------------------------------------------
    def _write_started(self, rg, ck, now, model=MODEL):
        self.con.execute(
            f"""
            MERGE INTO dbt_completions_raw T
            USING (
              SELECT '{model}' AS model_name, '{EXEC_TS}' AS execution_ts,
                     '{rg}' AS run_group_id, '{ck}' AS chunk_key,
                     TIMESTAMP '{now}' AS now_ts
            ) S
            ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
               AND T.chunk_key = S.chunk_key AND T.status IN ('started', 'locked_out')
            WHEN MATCHED AND T.status = 'locked_out' THEN
              UPDATE SET status = 'started', updated_at = S.now_ts, heartbeat_at = S.now_ts
            WHEN MATCHED THEN UPDATE SET heartbeat_at = S.now_ts
            WHEN NOT MATCHED THEN INSERT
              (model_name, execution_ts, status, run_group_id, chunk_key, heartbeat_at, updated_at)
              VALUES (S.model_name, S.execution_ts, 'started', S.run_group_id, S.chunk_key, S.now_ts, S.now_ts)
            """
        )

    def _look(self, rg, model=MODEL):
        row = self.con.execute(
            f"""
            SELECT f.run_group_id
            FROM dbt_completions_raw f
            WHERE f.model_name = '{model}'
              AND f.status = 'started'
              AND f.run_group_id != '{rg}'
              AND NOT EXISTS (
                SELECT 1 FROM dbt_completions_raw t
                WHERE t.model_name = f.model_name AND t.run_group_id = f.run_group_id
                  AND t.chunk_key = f.chunk_key AND t.status IN {TERMINAL}
              )
              AND (
                f.updated_at < (
                  SELECT MIN(m.updated_at) FROM dbt_completions_raw m
                  WHERE m.model_name = '{model}' AND m.run_group_id = '{rg}' AND m.status = 'started'
                )
                OR (
                  f.updated_at = (
                    SELECT MIN(m.updated_at) FROM dbt_completions_raw m
                    WHERE m.model_name = '{model}' AND m.run_group_id = '{rg}' AND m.status = 'started'
                  )
                  AND f.run_group_id < '{rg}'
                )
              )
            ORDER BY f.updated_at, f.run_group_id
            LIMIT 1
            """
        ).fetchone()
        return row[0] if row else None

    def _lockout(self, rg, ck, now, model=MODEL):
        self.con.execute(
            f"""
            MERGE INTO dbt_completions_raw T
            USING (
              SELECT '{model}' AS model_name, '{rg}' AS run_group_id,
                     '{ck}' AS chunk_key, TIMESTAMP '{now}' AS now_ts
            ) S
            ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
               AND T.chunk_key = S.chunk_key AND T.status = 'started'
            WHEN MATCHED THEN UPDATE SET status = 'locked_out', updated_at = S.now_ts
            """
        )

    def _reclaim(self, now, ttl, model=MODEL):
        self.con.execute(
            f"""
            MERGE INTO dbt_completions_raw T
            USING (
              SELECT s.model_name, s.execution_ts, s.run_group_id, s.chunk_key
              FROM dbt_completions_raw s
              WHERE s.model_name = '{model}'
                AND s.status = 'started'
                AND s.heartbeat_at IS NOT NULL
                AND s.heartbeat_at < (TIMESTAMP '{now}' - INTERVAL '{ttl} minutes')
                AND NOT EXISTS (
                  SELECT 1 FROM dbt_completions_raw t
                  WHERE t.model_name = s.model_name AND t.run_group_id = s.run_group_id
                    AND t.chunk_key = s.chunk_key AND t.status IN {TERMINAL}
                )
            ) S
            ON T.model_name = S.model_name AND T.run_group_id = S.run_group_id
               AND T.chunk_key = S.chunk_key AND T.status = 'crashed'
            WHEN NOT MATCHED THEN INSERT
              (model_name, execution_ts, status, run_group_id, chunk_key, updated_at)
              VALUES (S.model_name, S.execution_ts, 'crashed', S.run_group_id, S.chunk_key, TIMESTAMP '{now}')
            """
        )

    # -- high-level driver ------------------------------------------------
    def acquire(self, rg, now, ck="full", reclaim=False, ttl=240):
        """Run the full pre-hook. Returns None if the lock is acquired, else the
        winning foreign run_group_id (the macro would raise here -> 'locked_out')."""
        if reclaim:
            self._reclaim(now, ttl)
        self._write_started(rg, ck, now)
        winner = self._look(rg)
        if winner is not None:
            self._lockout(rg, ck, now)
        return winner

    def release(self, rg, now, ck="full", status="succeeded", model=MODEL):
        """A run reaching a terminal (what log_model_status/log_run_results write)."""
        self.con.execute(
            "INSERT INTO dbt_completions_raw "
            "(model_name, execution_ts, status, run_group_id, chunk_key, updated_at) "
            f"VALUES ('{model}', '{EXEC_TS}', '{status}', '{rg}', '{ck}', TIMESTAMP '{now}')"
        )

    def statuses(self, rg, ck="full", model=MODEL):
        rows = self.con.execute(
            "SELECT status FROM dbt_completions_raw "
            f"WHERE model_name='{model}' AND run_group_id='{rg}' AND chunk_key='{ck}' "
            "ORDER BY status"
        ).fetchall()
        return [r[0] for r in rows]

    # -- tests ------------------------------------------------------------
    def test_uncontended_acquires(self):
        self.assertIsNone(self.acquire("R1", ts(10)))
        self.assertEqual(self.statuses("R1"), ["started"])

    def test_sequential_earlier_run_wins(self):
        self.assertIsNone(self.acquire("R1", ts(10)))          # first in
        self.assertEqual(self.acquire("R2", ts(10, 5)), "R1")  # later run loses
        self.assertEqual(self.statuses("R1"), ["started"])
        self.assertEqual(self.statuses("R2"), ["locked_out"])  # NOT 'failed'

    def test_interleaved_writes_then_looks_exactly_one_wins(self):
        # Both write their 'started' before either looks (the race window).
        self._write_started("R1", "full", ts(10))
        self._write_started("R2", "full", ts(10, 5))
        w1 = self._look("R1")  # sees R2 (later) -> no priority -> R1 proceeds
        w2 = self._look("R2")  # sees R1 (earlier) -> R2 loses
        self.assertIsNone(w1)
        self.assertEqual(w2, "R1")

    def test_tie_breaks_on_smaller_run_group_id(self):
        # Identical updated_at -> the lexicographically smaller run_group wins.
        self._write_started("A", "full", ts(10))
        self._write_started("B", "full", ts(10))
        self.assertIsNone(self._look("A"))   # 'B' is not < 'A' -> A proceeds
        self.assertEqual(self._look("B"), "A")  # 'A' < 'B' -> B loses

    def test_three_runs_only_earliest_acquires(self):
        self.assertIsNone(self.acquire("R1", ts(10)))
        self.assertEqual(self.acquire("R2", ts(10, 5)), "R1")
        self.assertEqual(self.acquire("R3", ts(10, 9)), "R1")  # R1 still the holder
        self.assertEqual(self.statuses("R1"), ["started"])
        self.assertEqual(self.statuses("R2"), ["locked_out"])
        self.assertEqual(self.statuses("R3"), ["locked_out"])

    def test_teammates_same_run_group_do_not_lock_each_other(self):
        # Two chunks of the SAME run_group -> different chunk_key, same run_group.
        self.assertIsNone(self.acquire("R1", ts(10), ck="A"))
        self.assertIsNone(self.acquire("R1", ts(10), ck="B"))
        self.assertEqual(self.statuses("R1", ck="A"), ["started"])
        self.assertEqual(self.statuses("R1", ck="B"), ["started"])

    def test_retry_while_holder_running_locks_out_again(self):
        self.assertIsNone(self.acquire("R1", ts(10)))
        self.assertEqual(self.acquire("R2", ts(10, 5)), "R1")   # locked out
        # Airflow retries R2 under the same run_group while R1 still holds.
        self.assertEqual(self.acquire("R2", ts(10, 10)), "R1")  # locked out again
        self.assertEqual(self.statuses("R2"), ["locked_out"])   # still one row

    def test_retry_after_holder_finishes_acquires(self):
        self.assertIsNone(self.acquire("R1", ts(10)))
        self.assertEqual(self.acquire("R2", ts(10, 5)), "R1")   # locked out
        self.release("R1", ts(10, 30))                          # R1 finishes (succeeded)
        # Retry R2: revives its locked_out row to started; R1 no longer holds.
        self.assertIsNone(self.acquire("R2", ts(10, 40)))
        self.assertEqual(self.statuses("R2"), ["started"])      # revived, not a 2nd row

    def test_crash_reclaim_frees_stale_lock(self):
        # R1 started at 10:00 and never reached a terminal (process died). Heartbeat
        # is 10:00; TTL is 240 min. A new run arrives 5h later (15:00) -> stale.
        self._write_started("R1", "full", ts(10))
        winner = self.acquire("R2", ts(15), reclaim=True, ttl=240)
        self.assertIsNone(winner)                               # R2 acquires
        self.assertIn("crashed", self.statuses("R1"))           # R1 swept to crashed
        self.assertEqual(self.statuses("R2"), ["started"])

    def test_reclaim_leaves_fresh_lock_alone(self):
        # Same as above but the holder's heartbeat is still within TTL -> NOT stale.
        self._write_started("R1", "full", ts(14, 59))           # heartbeat 14:59
        winner = self.acquire("R2", ts(15), reclaim=True, ttl=240)  # only 1 min old
        self.assertEqual(winner, "R1")                          # R2 locked out
        self.assertNotIn("crashed", self.statuses("R1"))
        self.assertEqual(self.statuses("R2"), ["locked_out"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
