"""Transactional SQLite inbox model for a Kafka consumer."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3


def schema(db: sqlite3.Connection) -> None:
    db.executescript("""
    CREATE TABLE IF NOT EXISTS processed_events(
        event_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        event_type TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS notifications(
        event_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        body TEXT NOT NULL,
        FOREIGN KEY(event_id) REFERENCES processed_events(event_id)
    );
    """)


def validate(event: dict) -> None:
    if not isinstance(event, dict):
        raise ValueError("event must be an object")
    if not isinstance(event.get("event_id"), str) or not event["event_id"].strip():
        raise ValueError("missing event_id")
    if not isinstance(event.get("order_id"), str) or not event["order_id"].strip():
        raise ValueError("missing order_id")
    if event.get("event_type") != "order-paid":
        raise ValueError("unsupported event_type")
    if (not isinstance(event.get("amount_cents"), int)
            or isinstance(event["amount_cents"], bool)
            or event["amount_cents"] <= 0):
        raise ValueError("invalid amount_cents")


def apply_event(db: sqlite3.Connection, event: dict) -> bool:
    """Return True iff this event newly created a simulated notification.

    Unique event_id and notification write share one database transaction.
    """
    validate(event)
    with db:
        inserted = db.execute(
            "INSERT OR IGNORE INTO processed_events(event_id,order_id,event_type) VALUES (?,?,?)",
            (event["event_id"], event["order_id"], event["event_type"]),
        ).rowcount
        if inserted:
            db.execute(
                "INSERT INTO notifications(event_id,order_id,body) VALUES (?,?,?)",
                (event["event_id"], event["order_id"],
                 "Paid order " + event["order_id"]),
            )
    return bool(inserted)


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("PRAGMA foreign_keys=ON")
    schema(db)
    return db


def summary(db: sqlite3.Connection) -> dict:
    return {
        "processed_events": db.execute("SELECT count(*) FROM processed_events").fetchone()[0],
        "notifications": db.execute("SELECT count(*) FROM notifications").fetchone()[0],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=Path("output/inbox.db"))
    args = parser.parse_args()
    with connect(args.database) as db:
        print(json.dumps(summary(db), indent=2))
