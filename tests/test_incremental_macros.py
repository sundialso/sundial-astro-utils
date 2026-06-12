#!/usr/bin/env python3
"""Unit tests for the shared incremental-window macros (start_ts / end_ts /
execution_ts) in sundial_dbt_shared/macros/incremental.sql.

These render the ACTUAL macro source through Jinja for both warehouses, stubbing
the dbt builtins (var / is_incremental / this / adapter.dispatch / return) and the
sundial_dbt_shared.read_watermark + record_window_end helpers. They assert the
generated SQL has the right dialect + window semantics — they do NOT hit a
warehouse.

IMPORTANT: keep in sync with incremental.sql + adapters/{bigquery,snowflake}.sql.

Run:  python3 tests/test_incremental_macros.py
"""
from __future__ import annotations

import os
import re
import unittest

import jinja2

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MACRO_FILES = [
    "sundial_dbt_shared/macros/incremental.sql",
    "sundial_dbt_shared/macros/adapters/bigquery.sql",
    "sundial_dbt_shared/macros/adapters/snowflake.sql",
]


def _load_template():
    src = ""
    for f in MACRO_FILES:
        src += open(os.path.join(ROOT, f)).read() + "\n"
    # Strip the package qualifier so macros resolve by bare name in one template.
    src = src.replace("sundial_dbt_shared.", "")
    return jinja2.Environment(extensions=["jinja2.ext.do"]).from_string(src)


TMPL = _load_template()


class _Relation(str):
    """Mimics dbt's `this`: renders as the FQN, but exposes `.name`."""
    def __new__(cls, fqn, name):
        obj = super().__new__(cls, fqn)
        obj.name = name
        return obj


def module(prefix, vars_=None, resolved="2026-06-05 23:59:59"):
    """Build the rendered macro module for one adapter (`bigquery`/`snowflake`).

    Stubs run_query to return `resolved` as the resolved start_ts literal, and
    tracks the call count so the memoisation can be asserted. `model` is a real
    dict so the per-render memo (model.get / model.update) works.
    """
    vars_ = vars_ or {}
    calls = {"n": 0, "last_sql": None}

    def run_query(sql):
        calls["n"] += 1
        calls["last_sql"] = sql

        class Res:
            rows = [[resolved]]
        return Res()

    logged = {"start_values": [], "validated": []}

    g = {
        "return": lambda x: x,
        "var": lambda name, default=None: vars_.get(name, default),
        "is_incremental": lambda: vars_.get("__incremental", True),
        "execute": vars_.get("__execute", True),
        "read_watermark": lambda m: "SELECT MAX(end_ts) FROM compl WHERE model_name='%s'" % m,
        "record_window_end": lambda expr: "",
        # lives in dbt_completions.sql (not loaded here) — stub + capture the value
        "record_window_start_value": lambda v: logged["start_values"].append(v),
        "validate_partial_backfill": lambda m, ft=None: logged["validated"].append((m, ft)),
        "run_query": run_query,
        "model": {},
        # str subclass so `{{ this }}` renders the FQN and `this.name` works
        "this": _Relation("`proj.ds.m`" if prefix == "bigquery" else '"DB"."SC"."M"', "my_model"),
    }

    class Adapter:
        def dispatch(self, name, pkg):
            return lambda *a: getattr(mod, "%s__%s" % (prefix, name))(*a)

    g["adapter"] = Adapter()
    mod = TMPL.make_module(g)
    mod._calls = calls
    mod._logged = logged
    return mod


def norm(s):
    return " ".join(str(s).split())


class IncrementalMacroTests(unittest.TestCase):
    # ---- execution_ts -------------------------------------------------
    def test_execution_ts_now_bigquery(self):
        self.assertEqual(
            norm(module("bigquery").execution_ts()),
            "CAST(CAST(CURRENT_TIMESTAMP() AS DATE) AS DATETIME)",
        )

    def test_execution_ts_now_snowflake(self):
        self.assertEqual(
            norm(module("snowflake").execution_ts()),
            "CAST(CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS DATE) AS TIMESTAMP_NTZ)",
        )

    def test_execution_ts_var_override(self):
        for wh, typ in (("bigquery", "DATETIME"), ("snowflake", "TIMESTAMP_NTZ")):
            out = norm(module(wh, {"execution_ts": "2026-06-06"}).execution_ts())
            self.assertEqual(out, "CAST('2026-06-06' AS %s)" % typ)

    # ---- end_ts -------------------------------------------------------
    def test_end_ts_lag0_subtracts_one_second(self):
        self.assertEqual(
            norm(module("bigquery").end_ts(0)),
            "DATETIME_ADD( CAST(CAST(CURRENT_TIMESTAMP() AS DATE) AS DATETIME) , INTERVAL -1 SECOND)",
        )
        self.assertIn("DATEADD(SECOND, -1,", norm(module("snowflake").end_ts(0)))

    def test_end_ts_lag_subtracts_days_then_second(self):
        bq = norm(module("bigquery").end_ts(2))
        self.assertIn("INTERVAL -2 DAY", bq)
        self.assertIn("INTERVAL -1 SECOND", bq)
        sf = norm(module("snowflake").end_ts(2))
        self.assertIn("DATEADD(DAY, -2,", sf)
        self.assertIn("DATEADD(SECOND, -1,", sf)

    def test_end_ts_backfill_override(self):
        out = norm(module("bigquery", {"backfill_end_ts": "2026-01-01"}).end_ts(0))
        self.assertEqual(out, "CAST('2026-01-01' AS DATETIME)")

    # ---- start_ts -----------------------------------------------------
    def test_start_ts_emits_resolved_literal_not_subquery(self):
        # The watermark is resolved at compile time → start_ts must emit a CONSTANT
        # literal (prunable on BigQuery require_partition_filter), NOT a subquery.
        out = norm(module("bigquery").start_ts("event_ts", 7, "2021-01-01"))
        self.assertEqual(out, "CAST('2026-06-05 23:59:59' AS DATETIME)")
        self.assertNotIn("SELECT", out)     # no inline subquery in the model SQL
        self.assertNotIn("GREATEST", out)   # GREATEST was resolved away
        self.assertNotIn("read_watermark", out)

    def test_start_ts_snowflake_literal(self):
        out = norm(module("snowflake").start_ts("event_ts", 7, "2021-01-01"))
        self.assertEqual(out, "CAST('2026-06-05 23:59:59' AS TIMESTAMP_NTZ)")

    def test_start_ts_resolve_sql_has_watermark_and_fallback(self):
        # The query that RESOLVES the literal still uses watermark + MAX fallback.
        mod = module("bigquery")
        mod.start_ts("event_ts", 7, "2021-01-01")
        sql = " ".join(mod._calls["last_sql"].split())
        self.assertIn("SELECT MAX(end_ts) FROM compl", sql)               # read_watermark
        self.assertIn("SELECT MAX(event_ts) FROM `proj.ds.m`", sql)       # first-run fallback
        self.assertIn("INTERVAL 1 SECOND", sql)                           # +1s resume
        self.assertIn("INTERVAL -7 DAY", sql)                             # lookback
        self.assertIn("GREATEST(", sql)                                   # floored at first

    def test_start_ts_memoized_one_query_per_model(self):
        # N start_ts() calls with the same args in one model render → ONE query.
        mod = module("bigquery")
        mod.start_ts("event_ts", 7, "2021-01-01")
        mod.start_ts("event_ts", 7, "2021-01-01")
        mod.start_ts("event_ts", 7, "2021-01-01")
        self.assertEqual(mod._calls["n"], 1)

    def test_start_ts_logs_resolved_value_once(self):
        # start_ts logging: the resolved literal is recorded into start_ts exactly
        # once per model (memoised), with the value the model actually uses.
        mod = module("bigquery")
        mod.start_ts("event_ts", 7, "2021-01-01")
        mod.start_ts("event_ts", 7, "2021-01-01")
        self.assertEqual(mod._logged["start_values"], ["2026-06-05 23:59:59"])

    def test_start_ts_backfill_override(self):
        mod = module("snowflake", {"backfill_start_ts": "2026-02-01"})
        out = norm(mod.start_ts("event_ts", 7, "2021-01-01"))
        self.assertEqual(out, "CAST('2026-02-01' AS TIMESTAMP_NTZ)")
        self.assertEqual(mod._logged["start_values"], ["2026-02-01"])

    # ---- partial-backfill validation gating ---------------------------
    def test_normal_run_does_not_validate_backfill(self):
        # No backfill vars → backfill branch not taken → validation never runs.
        mod = module("bigquery")
        mod.start_ts("event_ts", 7, "2021-01-01")
        self.assertEqual(mod._logged["validated"], [])

    def test_chunk_backfill_vars_do_not_validate(self):
        # Forward chunking passes backfill_start_ts but run_context is not
        # partial_backfill — watermark-beyond checks must not run.
        mod = module(
            "bigquery",
            {
                "backfill_start_ts": "2026-02-01",
                "backfill_end_ts": "2026-06-09",
                "run_context": "normal",
            },
        )
        mod.start_ts("event_ts", 7, "2021-01-01")
        self.assertEqual(mod._logged["validated"], [])

    def test_partial_backfill_run_validates_once(self):
        # Only explicit partial_backfill runs validate bounds (memoised once).
        mod = module(
            "bigquery",
            {
                "backfill_start_ts": "2026-02-01",
                "backfill_end_ts": "2026-03-01",
                "run_context": "partial_backfill",
            },
        )
        mod.start_ts("event_ts", 7, "2021-01-01")
        mod.start_ts("event_ts", 7, "2021-01-01")
        self.assertEqual(mod._logged["validated"], [("my_model", "2021-01-01")])

    def test_start_ts_non_incremental_is_first_timestamp(self):
        out = norm(module("bigquery", {"__incremental": False}).start_ts("event_ts", 7, "2021-01-01"))
        self.assertEqual(out, "CAST('2021-01-01' AS DATETIME)")


if __name__ == "__main__":
    unittest.main(verbosity=2)
