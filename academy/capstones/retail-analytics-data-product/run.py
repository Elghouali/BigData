"""Retail data product: stage -> verify quality -> atomically replace each output."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory

ACADEMY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ACADEMY / "labs" / "sql-etl"))
from pipeline import load  # noqa: E402

SOURCE = ACADEMY / "labs" / "sql-etl" / "data" / "orders.csv"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(source: Path, output: Path, max_reject_ratio: float = 0.60) -> dict:
    """Produce a new release only if its data-quality gate passes.

    A failed run leaves the last published files untouched. Manifest is replaced
    last and includes a Gold CSV checksum. Full cross-file atomic publishing
    still needs versioned release directories / pointer switching in production.
    """
    if not 0 <= max_reject_ratio <= 1:
        raise ValueError("reject ratio threshold must be between 0 and 1")
    output.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix=".etl-stage-", dir=output) as directory:
        staging = Path(directory)
        db_file = staging / "orders.db"
        summary = load(source, db_file)
        unique_rows = summary["input_rows"] - summary["duplicate_source_rows"]
        ratio = summary["rejected_rows"] / unique_rows if unique_rows else 0
        if ratio > max_reject_ratio:
            raise ValueError(
                f"quality_gate_failed: reject ratio {ratio:.3f} > {max_reject_ratio:.3f}"
            )

        with sqlite3.connect(db_file) as db:
            with db:
                db.execute("""
                    CREATE TABLE IF NOT EXISTS daily_kpis(
                        order_date TEXT NOT NULL, category TEXT NOT NULL,
                        paid_orders INTEGER NOT NULL,
                        revenue_cents INTEGER NOT NULL,
                        PRIMARY KEY(order_date, category)
                    )
                """)
                db.execute("DELETE FROM daily_kpis")
                db.execute("""
                    INSERT INTO daily_kpis
                    SELECT order_date, category, COUNT(*), SUM(quantity * price_cents)
                    FROM orders WHERE status='paid'
                    GROUP BY order_date, category
                """)
            records = db.execute(
                "SELECT order_date, category, paid_orders, revenue_cents "
                "FROM daily_kpis ORDER BY order_date, category"
            ).fetchall()
            newest_order_date = db.execute(
                "SELECT MAX(order_date) FROM orders"
            ).fetchone()[0]

        report = staging / "daily_kpis.csv"
        with report.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["order_date", "category", "paid_orders", "revenue_cents"])
            writer.writerows(records)

        manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_sha256": sha256_file(source),
            "source_file": source.name,
            "gold_sha256": sha256_file(report),
            "latest_order_date": newest_order_date,
            "quality_passed": True,
            "reject_ratio": round(ratio, 4),
            "kpi_rows": len(records),
            **summary,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8")

        # Publication order: data then manifest. Clients should verify checksum.
        for name in ("orders.db", "daily_kpis.csv", "manifest.json"):
            os.replace(staging / name, output / name)
    return manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", type=Path, default=SOURCE)
    p.add_argument("--output", type=Path, default=Path("output"))
    p.add_argument("--max-reject-ratio", type=float, default=0.60)
    args = p.parse_args()
    print(json.dumps(build(args.source, args.output, args.max_reject_ratio), indent=2))
