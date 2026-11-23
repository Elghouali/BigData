import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from inbox import apply_event, connect, summary


class InboxTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "notifications.db"
        self.event = {
            "event_id": "evt-1", "order_id": "1001",
            "event_type": "order-paid", "amount_cents": 20000,
        }

    def test_repeat_delivery_creates_only_one_effect(self):
        with connect(self.path) as db:
            self.assertTrue(apply_event(db, self.event))
            self.assertFalse(apply_event(db, self.event))
            self.assertEqual(summary(db), {"processed_events": 1, "notifications": 1})

    def test_restart_replays_safely(self):
        with connect(self.path) as db:
            self.assertTrue(apply_event(db, self.event))
        with connect(self.path) as db:
            self.assertFalse(apply_event(db, self.event))
            self.assertEqual(summary(db)["notifications"], 1)

    def test_invalid_event_not_written(self):
        with connect(self.path) as db:
            with self.assertRaisesRegex(ValueError, "invalid amount"):
                apply_event(db, {**self.event, "amount_cents": -1})
            self.assertEqual(summary(db)["processed_events"], 0)

    def test_transaction_rolls_back_if_notification_fails(self):
        with connect(self.path) as db:
            db.execute("""CREATE TRIGGER reject_notifications
                        BEFORE INSERT ON notifications
                        BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END;""")
            with self.assertRaises(sqlite3.IntegrityError):
                apply_event(db, self.event)
            self.assertEqual(summary(db)["processed_events"], 0)


if __name__ == "__main__":
    unittest.main()
