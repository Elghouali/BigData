import csv
from pathlib import Path
import sqlite3
import tempfile
import unittest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline import load, money_cents  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[1] / "data" / "orders.csv"


class IngestionTests(unittest.TestCase):
    def test_fixture_revenue_and_rejections(self):
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "orders.db"
            summary = load(FIXTURE, target)
            self.assertEqual(summary["input_rows"], 10)
            self.assertEqual(summary["duplicate_source_rows"], 1)
            self.assertEqual(summary["loaded_unique_orders"], 4)
            self.assertEqual(summary["rejected_rows"], 5)
            self.assertEqual(summary["paid_revenue_cents"], 32500)

    def test_idempotent_second_run(self):
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "orders.db"
            first = load(FIXTURE, target)
            second = load(FIXTURE, target)
            self.assertEqual(first, second)
            with sqlite3.connect(target) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM raw_rows").fetchone()[0], 9)

    def test_money_exact_and_nonnegative(self):
        self.assertEqual(money_cents("0.01"), 1)
        self.assertEqual(money_cents("100.25"), 10025)
        for invalid in ("NaN", "-5", "1.999", "oops"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    money_cents(invalid)

    def test_conflicting_order_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "conflicting.csv"
            with source.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["order_id","order_date","customer_id","region","category","quantity","unit_price","status"])
                writer.writerow(["1","2026-11-01","A","Rabat","Home","1","10.00","paid"])
                writer.writerow(["1","2026-11-01","A","Rabat","Home","1","12.00","paid"])
            result = load(source, Path(d) / "db.sqlite")
            self.assertEqual(result["loaded_unique_orders"], 1)
            self.assertEqual(result["rejected_rows"], 1)
            with sqlite3.connect(Path(d) / "db.sqlite") as db:
                reason = db.execute("SELECT reason FROM rejects").fetchone()[0]
            self.assertEqual(reason, "conflicting_order_id")


if __name__ == "__main__":
    unittest.main()
