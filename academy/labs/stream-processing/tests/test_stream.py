import sys
import unittest
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from stream import read_events, simulate


class StreamTests(unittest.TestCase):
    def test_dedup_and_late(self):
        r = simulate(read_events(root / "data/events.csv"), 30)
        self.assertEqual(r["late_ids"], ["e4"])
        self.assertEqual(r["duplicate_ids"], ["e1"])
        self.assertEqual(r["unique_events"], 5)
        self.assertEqual(r["windows"], [
            {"minute": "2026-11-01T12:00:00", "category": "store", "amount": "15.00"},
            {"minute": "2026-11-01T12:01:00", "category": "online", "amount": "4.00"},
            {"minute": "2026-11-01T12:01:00", "category": "store", "amount": "7.00"},
        ])

    def test_allow_more_late_data(self):
        r = simulate(read_events(root / "data/events.csv"), 120)
        self.assertEqual(r["late_ids"], [])
        self.assertEqual(r["windows"][0]["amount"], "24.00")

    def test_reject_negative_policy(self):
        with self.assertRaises(ValueError):
            simulate([], -1)


if __name__ == "__main__":
    unittest.main()
