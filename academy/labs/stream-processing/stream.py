"""Event-time stream simulation with deduplication and a late-event quarantine."""
import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path


def read_events(source):
    with Path(source).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def simulate(rows, allowed_lateness=30):
    if allowed_lateness < 0:
        raise ValueError("lateness must be nonnegative")
    seen, late, duplicates = set(), [], []
    max_seen = None
    windows = defaultdict(lambda: Decimal("0.00"))
    for row in rows:
        event_id = row["event_id"]
        if event_id in seen:
            duplicates.append(event_id)
            continue
        seen.add(event_id)
        timestamp = datetime.fromisoformat(row["event_time"])
        watermark = (max_seen - timedelta(seconds=allowed_lateness)
                     if max_seen else None)
        if watermark and timestamp < watermark:
            late.append(event_id)
        else:
            minute = timestamp.replace(second=0, microsecond=0).isoformat()
            windows[(minute, row["category"])] += Decimal(row["amount"])
        max_seen = timestamp if max_seen is None else max(max_seen, timestamp)
    return {
        "windows": [
            {"minute": minute, "category": category, "amount": f"{value:.2f}"}
            for (minute, category), value in sorted(windows.items())
        ],
        "late_ids": late,
        "duplicate_ids": duplicates,
        "unique_events": len(seen),
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/events.csv")
    p.add_argument("--lateness", type=int, default=30)
    args = p.parse_args()
    print(json.dumps(simulate(read_events(args.input), args.lateness), indent=2))
