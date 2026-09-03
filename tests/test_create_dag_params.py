#!/usr/bin/env python3
"""Unit tests for create_dag param validation."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from sundial_airflow.run_input import validate_backfill_params

UTC = timezone.utc


class ValidateBackfillParamsTests(unittest.TestCase):
    def test_partial_accepts_mixed_datetime_and_string_window(self) -> None:
        validate_backfill_params(
            backfill_mode="partial",
            start_ts=datetime(2025, 9, 15, tzinfo=UTC),
            end_ts="2025-10-01",
            execution_ts=None,
        )

    def test_partial_rejects_inverted_window(self) -> None:
        with self.assertRaisesRegex(ValueError, "start_ts < end_ts"):
            validate_backfill_params(
                backfill_mode="partial",
                start_ts="2025-11-01",
                end_ts="2025-10-01",
                execution_ts=None,
            )

    def test_partial_requires_both_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires both start_ts and end_ts"):
            validate_backfill_params(
                backfill_mode="partial",
                start_ts="2025-11-01",
                end_ts=None,
                execution_ts=None,
            )

    def test_partial_rejects_execution_ts(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not accept execution_ts"):
            validate_backfill_params(
                backfill_mode="partial",
                start_ts="2025-01-01",
                end_ts="2025-06-01",
                execution_ts="2026-09-01",
            )

    def test_none_rejects_window_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not accept start_ts/end_ts"):
            validate_backfill_params(
                backfill_mode="none",
                start_ts="2025-01-01",
                end_ts=None,
                execution_ts=None,
            )


if __name__ == "__main__":
    unittest.main()
