from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ingestion.external import rakuten_ingestion as rakuten


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.csv = self.root / "keywords.csv"
        self.csv.write_text("category,subcategory,keyword_ja,priority,status,last_page,last_run_at\na,b,one,high,pending,0,\na,b,two,high,pending,0,\na,b,three,high,pending,0,\n", encoding="utf-8")

    def tearDown(self): self.temp.cleanup()

    def test_checkpoint_overrides_stale_csv_and_resumes(self):
        state = {"version":2,"entries":{"one\t1":{"keyword":"one","page":1,"state":"persisted"}},"queries":{"one":{"state":"completed","last_page":1}}}
        self.assertEqual(rakuten.select_work(self.csv, state, 1, 10), [("two", 1), ("three", 1)])
        path = self.root / "state.json"; rakuten.atomic_json(path, state)
        self.assertEqual(rakuten.select_work(self.csv, rakuten.load_checkpoint(path), 1, 10), [("two", 1), ("three", 1)])

    def test_partial_pages_and_duplicate_entries(self):
        state = {"version":2,"entries":{"one\t1":{"keyword":"one","page":1,"state":"persisted"},"one-again\t1":{"keyword":"one","page":1,"state":"persisted"}},"queries":{"one":{"state":"partial","last_page":1}}}
        self.assertEqual(rakuten.select_work(self.csv, state, 3, 1), [("one", 2)])

    def test_atomic_checkpoint_persistence(self):
        path = self.root / "state.json"; state = rakuten.empty_checkpoint(); rakuten.atomic_json(path, state)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["version"], 2)
        self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_budget_includes_retry(self):
        response = Mock(status_code=429, headers={}, ok=False)
        session = Mock(); session.get.return_value = response
        metrics = {"http_attempts":0,"successful_requests":0,"http_429":0,"retries":0}
        with self.assertRaises(RuntimeError):
            rakuten.fetch(session, {}, "redacted", retries=5, remaining=1, metrics=metrics)
        self.assertEqual(metrics["http_attempts"], 1)
        self.assertEqual(metrics["http_429"], 1)

    def test_retry_consumes_the_same_hard_attempt_budget(self):
        first = Mock(status_code=500, headers={}, ok=False)
        second = Mock(status_code=200, headers={}, ok=True)
        second.json.return_value = {"items": []}
        session = Mock(); session.get.side_effect = [first, second]
        metrics = {"http_attempts":0,"successful_requests":0,"http_429":0,"retries":0}
        from unittest.mock import patch
        with patch("ingestion.external.rakuten_ingestion.time.sleep"):
            rakuten.fetch(session, {}, "redacted", retries=1, remaining=2, metrics=metrics)
        self.assertEqual(metrics["http_attempts"], 2)
        self.assertEqual(metrics["retries"], 1)

    def test_persist_batch_writes_parquet_before_checkpoint_and_resumes(self):
        checkpoint = self.root / "state.json"
        with patch.object(rakuten, "OUTPUT_DIR", self.root / "raw"):
            state = rakuten.empty_checkpoint()
            record = rakuten.normalized({"itemCode":"p1","itemName":"name","itemPrice":10}, __import__("datetime").datetime.now(__import__("datetime").timezone.utc))
            output, unique = rakuten.persist_batch([record], [("one", 1)], state, checkpoint, record["collected_at"])
        self.assertTrue(output.exists())
        self.assertEqual(unique, 1)
        loaded = rakuten.load_checkpoint(checkpoint)
        self.assertIn("one\t1", loaded["entries"])
        loaded["queries"]["one"] = {"state":"completed", "last_page":1}
        self.assertEqual(rakuten.select_work(self.csv, loaded, 1, 10), [("two", 1), ("three", 1)])

    def test_zero_item_page_is_checkpointed_with_an_empty_parquet(self):
        checkpoint = self.root / "state.json"
        with patch.object(rakuten, "OUTPUT_DIR", self.root / "raw"):
            state = rakuten.empty_checkpoint()
            output, unique = rakuten.persist_batch([], [("one", 1)], state, checkpoint, __import__("datetime").datetime.now(__import__("datetime").timezone.utc))
        self.assertTrue(output.exists())
        self.assertEqual(unique, 0)
        self.assertIn("one\t1", rakuten.load_checkpoint(checkpoint)["entries"])

    def test_ctrl_c_persists_successful_page_before_exit(self):
        checkpoint, metrics = self.root / "state.json", self.root / "metrics.jsonl"
        response = Mock(status_code=200, ok=True); response.json.return_value = {"items":[{"itemCode":"p1","itemName":"name","itemPrice":10}], "pageCount":1}
        session = Mock(); session.__enter__ = Mock(return_value=session); session.__exit__ = Mock(return_value=False); session.get.side_effect = [response, KeyboardInterrupt()]
        env = {"RAKUTEN_KEYWORD_CSV":str(self.csv), "RAKUTEN_CHECKPOINT_PATH":str(checkpoint), "RAKUTEN_METRICS_PATH":str(metrics), "RAKUTEN_PAGE_SIZE":"30", "RAKUTEN_MAX_PAGES":"1", "RAKUTEN_REQUEST_BUDGET":"10", "RAKUTEN_REQUEST_DELAY_SECONDS":"0", "RAKUTEN_MAX_RETRIES":"0", "RAKUTEN_DRY_RUN":"false", "RAKUTEN_APPLICATION_ID":"test", "RAKUTEN_ACCESS_KEY":"test"}
        with patch.dict("os.environ", env, clear=True), patch.object(rakuten, "OUTPUT_DIR", self.root / "raw"), patch("ingestion.external.rakuten_ingestion.requests.Session", return_value=session), patch("ingestion.external.rakuten_ingestion.time.sleep"):
            with self.assertRaises(KeyboardInterrupt): rakuten.main()
        loaded = rakuten.load_checkpoint(checkpoint)
        self.assertIn("one\t1", loaded["entries"])
        self.assertTrue(list((self.root / "raw").glob("*.parquet")))

    def test_main_flushes_after_each_ten_successful_requests(self):
        checkpoint, metrics = self.root / "state.json", self.root / "metrics.jsonl"
        lines = ["category,subcategory,keyword_ja,priority,status,last_page,last_run_at"] + [f"a,b,k{i},high,pending,0," for i in range(11)]
        self.csv.write_text("\n".join(lines) + "\n", encoding="utf-8")
        replies = []
        for i in range(11):
            reply = Mock(status_code=200, ok=True); reply.json.return_value = {"items":[{"itemCode":f"p{i}","itemName":"name","itemPrice":10}], "pageCount":1}; replies.append(reply)
        session = Mock(); session.__enter__ = Mock(return_value=session); session.__exit__ = Mock(return_value=False); session.get.side_effect = replies
        env = {"RAKUTEN_KEYWORD_CSV":str(self.csv), "RAKUTEN_CHECKPOINT_PATH":str(checkpoint), "RAKUTEN_METRICS_PATH":str(metrics), "RAKUTEN_PAGE_SIZE":"30", "RAKUTEN_MAX_PAGES":"1", "RAKUTEN_REQUEST_BUDGET":"11", "RAKUTEN_REQUEST_DELAY_SECONDS":"0", "RAKUTEN_MAX_RETRIES":"0", "RAKUTEN_DRY_RUN":"false", "RAKUTEN_APPLICATION_ID":"test", "RAKUTEN_ACCESS_KEY":"test"}
        with patch.dict("os.environ", env, clear=True), patch.object(rakuten, "OUTPUT_DIR", self.root / "raw"), patch("ingestion.external.rakuten_ingestion.requests.Session", return_value=session), patch("ingestion.external.rakuten_ingestion.time.sleep"):
            rakuten.main()
        self.assertEqual(len(list((self.root / "raw").glob("*.parquet"))), 2)
        self.assertEqual(len(rakuten.load_checkpoint(checkpoint)["entries"]), 11)


if __name__ == "__main__": unittest.main()
