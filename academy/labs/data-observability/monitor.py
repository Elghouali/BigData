"""Local data product freshness/quality monitor.

Example:
  python monitor.py --manifest path/to/manifest.json --database path/to/orders.db
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3


def utc(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp_requires_timezone")
    return result.astimezone(timezone.utc)


def inspect(manifest_path: Path, database: Path, *,
            as_of: datetime | None = None,
            max_job_age_minutes: float = 120,
            max_data_age_days: int = 1,
            max_reject_ratio: float = 0.6) -> dict:
    if max_job_age_minutes < 0 or max_data_age_days < 0 or not 0 <= max_reject_ratio <= 1:
        raise ValueError("invalid threshold")
    now = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    issues: list[str] = []
    metrics: dict = {}
    if not manifest_path.is_file():
        issues.append("missing_manifest")
        manifest = {}
    else:
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (ValueError, UnicodeError):
            issues.append("invalid_manifest")
            manifest = {}
    generated = manifest.get("generated_at_utc")
    if not generated:
        issues.append("missing_run_timestamp")
    else:
        try:
            age = (now - utc(generated)).total_seconds() / 60
            metrics["job_age_minutes"] = round(age, 2)
            if age < -1:
                issues.append("run_timestamp_in_future")
            elif age > max_job_age_minutes:
                issues.append("stale_job")
        except ValueError:
            issues.append("invalid_run_timestamp")

    ratio = manifest.get("reject_ratio")
    if ratio is None or not isinstance(ratio, (int, float)):
        issues.append("missing_reject_ratio")
    else:
        metrics["reject_ratio"] = ratio
        if ratio > max_reject_ratio:
            issues.append("quality_reject_ratio_exceeded")
    if manifest.get("quality_passed") is not True:
        issues.append("quality_not_confirmed")
    if not isinstance(manifest.get("kpi_rows"), int) or manifest["kpi_rows"] < 1:
        issues.append("missing_published_kpis")

    if not database.is_file():
        issues.append("missing_database")
    else:
        try:
            with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as db:
                latest = db.execute("SELECT MAX(order_date) FROM orders").fetchone()[0]
            if latest is None:
                issues.append("no_valid_order_dates")
            else:
                days = (now.date() - date.fromisoformat(latest)).days
                metrics["data_age_days"] = days
                if days < 0:
                    issues.append("source_date_in_future")
                elif days > max_data_age_days:
                    issues.append("stale_data")
        except (sqlite3.Error, ValueError):
            issues.append("unreadable_orders_table")
    return {"status": "healthy" if not issues else "alert",
            "issues": issues, "metrics": metrics,
            "evaluation_utc": now.isoformat()}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--database", type=Path, required=True)
    p.add_argument("--as-of", help="ISO timestamp with timezone; for reproducible training")
    p.add_argument("--max-job-age-minutes", type=float, default=120)
    p.add_argument("--max-data-age-days", type=int, default=1)
    p.add_argument("--max-reject-ratio", type=float, default=0.6)
    args = p.parse_args()
    observation = inspect(
        args.manifest, args.database,
        as_of=utc(args.as_of) if args.as_of else None,
        max_job_age_minutes=args.max_job_age_minutes,
        max_data_age_days=args.max_data_age_days,
        max_reject_ratio=args.max_reject_ratio)
    print(json.dumps(observation, indent=2))
    return 0 if observation["status"] == "healthy" else 2


if __name__ == "__main__":
    raise SystemExit(main())
