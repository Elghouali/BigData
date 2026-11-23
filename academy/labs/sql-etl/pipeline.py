"""Idempotent retail CSV -> SQLite ETL using only the Python standard library."""
from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sqlite3

FIELDS = ("order_id", "order_date", "customer_id", "region",
          "category", "quantity", "unit_price", "status")


def money_cents(text: str) -> int:
    try:
        value = Decimal(str(text))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid_unit_price") from exc
    if not value.is_finite() or value < 0 or value.as_tuple().exponent < -2:
        raise ValueError("invalid_unit_price")
    return int(value * 100)


def parse_order(row: dict[str, str]) -> tuple:
    if not row.get("order_id", "").strip():
        raise ValueError("missing_order_id")
    if not row.get("customer_id", "").strip():
        raise ValueError("missing_customer")
    try:
        when = date.fromisoformat(row.get("order_date", ""))
    except ValueError as exc:
        raise ValueError("invalid_date") from exc
    try:
        qty = int(row.get("quantity", ""))
        if qty <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError("invalid_quantity") from exc
    price = money_cents(row.get("unit_price", ""))
    status = row.get("status", "").strip().lower()
    if status not in ("paid", "cancelled"):
        raise ValueError("invalid_status")
    return (
        row["order_id"].strip(), when.isoformat(), row["customer_id"].strip(),
        row.get("region", "").strip(), row.get("category", "").strip(),
        qty, price, status,
    )


def create_schema(db: sqlite3.Connection) -> None:
    db.executescript("""
    CREATE TABLE IF NOT EXISTS raw_rows(
      fingerprint TEXT PRIMARY KEY, payload_json TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS orders(
      order_id TEXT PRIMARY KEY, order_date TEXT NOT NULL,
      customer_id TEXT NOT NULL, region TEXT, category TEXT,
      quantity INTEGER NOT NULL CHECK(quantity > 0),
      price_cents INTEGER NOT NULL CHECK(price_cents >= 0),
      status TEXT NOT NULL CHECK(status IN ('paid','cancelled'))
    );
    CREATE TABLE IF NOT EXISTS rejects(
      fingerprint TEXT PRIMARY KEY, order_id TEXT, reason TEXT NOT NULL,
      payload_json TEXT NOT NULL
    );
    """)


def fingerprint(row: dict[str, str]) -> str:
    serialized = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load(source: Path, database: Path) -> dict:
    database.parent.mkdir(parents=True, exist_ok=True)
    summary = {"input_rows": 0, "duplicate_source_rows": 0,
               "loaded_unique_orders": 0, "rejected_rows": 0}
    with sqlite3.connect(database) as db:
        create_schema(db)
        with source.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != FIELDS:
                raise ValueError("unexpected_csv_columns")
            seen = set()
            for row in reader:
                summary["input_rows"] += 1
                digest = fingerprint(row)
                if digest in seen:
                    summary["duplicate_source_rows"] += 1
                    continue
                seen.add(digest)
                payload = json.dumps(row, sort_keys=True)
                with db:
                    db.execute("INSERT OR IGNORE INTO raw_rows VALUES (?, ?)",
                               (digest, payload))
                    try:
                        record = parse_order(row)
                        current = db.execute(
                            "SELECT order_id, order_date, customer_id, region, category, quantity, price_cents, status FROM orders WHERE order_id=?",
                            (record[0],)
                        ).fetchone()
                        if current is not None and tuple(current) != record:
                            raise ValueError("conflicting_order_id")
                        db.execute(
                            "INSERT OR IGNORE INTO orders VALUES (?,?,?,?,?,?,?,?)",
                            record
                        )
                    except ValueError as exc:
                        db.execute(
                            "INSERT OR IGNORE INTO rejects VALUES (?,?,?,?)",
                            (digest, row.get("order_id"), str(exc), payload),
                        )
        summary["loaded_unique_orders"] = db.execute(
            "SELECT COUNT(*) FROM orders").fetchone()[0]
        summary["rejected_rows"] = db.execute(
            "SELECT COUNT(*) FROM rejects").fetchone()[0]
        summary["paid_revenue_cents"] = db.execute(
            "SELECT COALESCE(SUM(quantity * price_cents),0) FROM orders WHERE status='paid'"
        ).fetchone()[0]
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--database", default="output/retail.db")
    args = parser.parse_args()
    result = load(Path(args.source), Path(args.database))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
