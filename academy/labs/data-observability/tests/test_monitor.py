from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from monitor import inspect, utc

POINT = datetime(2026, 11, 3, 0, 0, tzinfo=timezone.utc)


class FreshnessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.manifest = self.folder / "manifest.json"
        self.dbfile = self.folder / "data.db"
        self.write_manifest()
        with sqlite3.connect(self.dbfile) as db:
            db.execute("CREATE TABLE orders (order_date TEXT)")
            db.execute("INSERT INTO orders VALUES ('2026-11-02')")

    def write_manifest(self, **overrides):
        metadata = {
            "generated_at_utc": "2026-11-03T00:00:00+00:00",
            "reject_ratio": 0.05,
            "quality_passed": True,
            "kpi_rows": 3,
        }
        metadata.update(overrides)
        self.manifest.write_text(json.dumps(metadata))

    def test_healthy(self):
        result = inspect(self.manifest, self.dbfile, as_of=POINT)
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["metrics"]["data_age_days"], 1)

    def test_stale_job(self):
        later = datetime(2026, 11, 4, tzinfo=timezone.utc)
        result = inspect(self.manifest, self.dbfile, as_of=later)
        self.assertIn("stale_job", result["issues"])

    def test_data_stale_even_if_job_fresh(self):
        later = datetime(2026, 11, 10, tzinfo=timezone.utc)
        self.write_manifest(generated_at_utc=later.isoformat())
        result = inspect(self.manifest, self.dbfile, as_of=later)
        self.assertNotIn("stale_job", result["issues"])
        self.assertIn("stale_data", result["issues"])

    def test_missing_manifest_and_database(self):
        result = inspect(self.folder / "missing.json", self.folder / "none.db",
                         as_of=POINT)
        self.assertIn("missing_database", result["issues"])
        self.assertIn("missing_manifest", result["issues"])

    def test_quality_violations(self):
        self.write_manifest(reject_ratio=0.8, quality_passed=False, kpi_rows=0)
        result = inspect(self.manifest, self.dbfile, as_of=POINT)
        self.assertIn("quality_not_confirmed", result["issues"])
        self.assertIn("quality_reject_ratio_exceeded", result["issues"])
        self.assertIn("missing_published_kpis", result["issues"])

    def test_naive_datetime_refused(self):
        with self.assertRaisesRegex(ValueError, "timezone"):
            utc("2026-11-03T00:00:00")


if __name__ == "__main__":
    unittest.main()
