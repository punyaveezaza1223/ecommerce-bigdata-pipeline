from __future__ import annotations

import unittest

from scripts.run_rakuten_bulk import MICRO_BATCH_ATTEMPTS, acquire_lock, cooldown_active, dag_finished, decide, metric_attempts_for_day, record_cooldown


class BulkSafetyTests(unittest.TestCase):
    def test_allows_micro_batch_under_confirmed_quota(self):
        self.assertTrue(decide(12, 2, 0, 20, 0, None, False).proceed)

    def test_quota_exhaustion_stops(self):
        self.assertFalse(decide(12, 2, 20, 20, 0, None, False).proceed)

    def test_429_stops(self):
        self.assertFalse(decide(12, 2, 0, 20, 0, None, True).proceed)

    def test_low_disk_stops(self):
        self.assertFalse(decide(9.99, 2, 0, 20, 0, None, False).proceed)

    def test_missing_ram_stops(self):
        self.assertFalse(decide(12, 0, 0, 20, 0, None, False).proceed)

    def test_batch_size_is_hard_capped(self):
        self.assertEqual(MICRO_BATCH_ATTEMPTS, 10)

    def test_dag_failure_stops(self):
        with self.assertRaises(RuntimeError):
            dag_finished("02_spark_etl", "failed")

    def test_missing_metrics_never_claims_complete_daily_coverage(self):
        attempts, complete = metric_attempts_for_day("2026-10-11", __import__("pathlib").Path("does-not-exist.jsonl"))
        self.assertEqual(attempts, 0)
        self.assertFalse(complete)

    def test_operational_cap_is_independent_of_daily_limit(self):
        self.assertFalse(decide(12, 2, 10, 10, 0, 1000, False).proceed)

    def test_second_instance_is_rejected(self):
        from pathlib import Path
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bulk.lock"
            acquire_lock(path)
            with self.assertRaises(RuntimeError): acquire_lock(path)

    def test_retry_after_cooldown_blocks_resume(self):
        from pathlib import Path
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cooldown.json"
            record_cooldown(60, path)
            self.assertTrue(cooldown_active(path))


if __name__ == "__main__": unittest.main()
