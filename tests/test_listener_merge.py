#!/usr/bin/env python3
"""Tests for the listener's reconciliation MERGE (sundial_airflow/warehouses.py).

The MERGE must match the dbt_completions_raw schema the macros write: the key
includes run_group_id + chunk_key, and the INSERT lists them. Without these the
listener's reconciliation row gets NULL run_group_id and is dropped by the
dbt_completions view's run_group join (the view picks a winning run_group and
joins on it), so a manual Mark Success/Failed would silently vanish.

warehouses.py keeps its airflow/provider imports inside methods, so _merge_sql
imports cleanly without airflow installed.

Run:  python3 tests/test_listener_merge.py
"""
from __future__ import annotations

import importlib.util
import os
import unittest

import duckdb

# Load warehouses.py directly by path — importing the sundial_airflow package
# runs __init__.py, which pulls in dag_factory (and airflow). warehouses.py keeps
# its airflow/provider imports inside methods, so loading the file is airflow-free.
_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sundial_airflow",
    "warehouses.py",
)
_spec = importlib.util.spec_from_file_location("_warehouses_standalone", _PATH)
_warehouses = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_warehouses)
_merge_sql = _warehouses._merge_sql


class MergeSqlTests(unittest.TestCase):
    def setUp(self):
        self.sql = " ".join(_merge_sql("`p.d.dbt_completions_raw`", ["SELECT 1"]).split())

    def test_key_includes_run_group_and_chunk(self):
        self.assertIn("T.model_name = S.model_name", self.sql)
        self.assertIn("T.execution_ts = S.execution_ts", self.sql)
        self.assertIn("T.status = S.status", self.sql)
        self.assertIn("T.run_group_id = S.run_group_id", self.sql)
        self.assertIn("T.chunk_key = S.chunk_key", self.sql)

    def test_insert_lists_new_columns(self):
        self.assertIn(
            "INSERT (model_name, execution_ts, status, run_group_id, chunk_key, updated_at)",
            self.sql,
        )
        self.assertIn(
            "VALUES (S.model_name, S.execution_ts, S.status, S.run_group_id, S.chunk_key, S.updated_at)",
            self.sql,
        )

    def test_matched_refreshes_updated_at(self):
        self.assertIn("WHEN MATCHED THEN UPDATE SET updated_at = S.updated_at", self.sql)


_RAW_DDL = """
CREATE TABLE dbt_completions_raw (
  model_name VARCHAR, execution_ts VARCHAR, status VARCHAR, updated_at TIMESTAMP,
  run_group_id VARCHAR, chunk_key VARCHAR, heartbeat_at TIMESTAMP,
  start_ts TIMESTAMP, end_ts TIMESTAMP
)
"""


class MergeRunGroupScopingTests(unittest.TestCase):
    """Run the ACTUAL _merge_sql in DuckDB to prove the reconciliation only
    touches the targeted run_group's rows.

    Scenario: the DAG ran 3 times on 2026-06-07 → 3 run_groups (rg1/rg2/rg3) for
    the same model+execution_ts, each with started + a terminal. rg2 failed. An
    operator marks rg2's model success → the listener MERGEs 'succeeded' for rg2.
    Only rg2 must change; rg1 and rg3 stay byte-identical.
    """

    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.con.execute(_RAW_DDL)
        seed = [
            ("orders", "2026-06-07", "started",   "2026-06-07 01:00:00", "rg1", "full"),
            ("orders", "2026-06-07", "succeeded", "2026-06-07 01:05:00", "rg1", "full"),
            ("orders", "2026-06-07", "started",   "2026-06-07 02:00:00", "rg2", "full"),
            ("orders", "2026-06-07", "failed",    "2026-06-07 02:05:00", "rg2", "full"),
            ("orders", "2026-06-07", "started",   "2026-06-07 03:00:00", "rg3", "full"),
            ("orders", "2026-06-07", "succeeded", "2026-06-07 03:05:00", "rg3", "full"),
        ]
        for r in seed:
            self.con.execute(
                "INSERT INTO dbt_completions_raw "
                "(model_name, execution_ts, status, updated_at, run_group_id, chunk_key) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                list(r),
            )

    def tearDown(self):
        self.con.close()

    def _rows(self, rg):
        return self.con.execute(
            "SELECT status, updated_at FROM dbt_completions_raw "
            "WHERE run_group_id = ? ORDER BY status, updated_at",
            [rg],
        ).fetchall()

    def _reconcile(self, rg, status="succeeded", ts="2026-06-07 09:00:00"):
        # Mirror what the adapter builds, in DuckDB-compatible SQL.
        sel = (
            f"SELECT 'orders' AS model_name, '2026-06-07' AS execution_ts, "
            f"'{status}' AS status, '{rg}' AS run_group_id, 'full' AS chunk_key, "
            f"TIMESTAMP '{ts}' AS updated_at"
        )
        self.con.execute(_merge_sql("dbt_completions_raw", [sel]))

    def test_reconcile_only_affects_target_run_group(self):
        before_rg1, before_rg3 = self._rows("rg1"), self._rows("rg3")
        self._reconcile("rg2")  # operator marks the 2nd run's model success
        # other runs untouched
        self.assertEqual(self._rows("rg1"), before_rg1)
        self.assertEqual(self._rows("rg3"), before_rg3)
        # rg2 gained a 'succeeded' row alongside its started + failed
        self.assertEqual(
            [r[0] for r in self._rows("rg2")], ["failed", "started", "succeeded"]
        )

    def test_reconcile_is_idempotent(self):
        self._reconcile("rg2", ts="2026-06-07 09:00:00")
        self._reconcile("rg2", ts="2026-06-07 09:30:00")  # re-mark: must not duplicate
        succeeded = self.con.execute(
            "SELECT COUNT(*) FROM dbt_completions_raw "
            "WHERE run_group_id='rg2' AND status='succeeded'"
        ).fetchone()[0]
        self.assertEqual(succeeded, 1)
        # the repeat refreshed updated_at on the same row
        ts = self.con.execute(
            "SELECT updated_at FROM dbt_completions_raw "
            "WHERE run_group_id='rg2' AND status='succeeded'"
        ).fetchone()[0]
        self.assertEqual(str(ts), "2026-06-07 09:30:00")


if __name__ == "__main__":
    unittest.main(verbosity=2)
