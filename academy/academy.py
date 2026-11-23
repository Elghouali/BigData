"""Big Data + Cloud Academy local CLI; Python standard library only."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
CATALOG = ROOT / "catalog.json"
DEFAULT_PROGRESS = ROOT / ".academy-progress.json"
STATUSES = ("not-started", "in-progress", "done")


def lessons():
    return json.loads(CATALOG.read_text(encoding="utf-8"))["labs"]


def doctor():
    tools = {name: shutil.which(name) is not None
             for name in ("git", "docker", "terraform", "java")}
    return {"python": sys.version.split()[0], "python_supported": sys.version_info >= (3, 11),
            "tools": tools, "note": "Only Python is required for the first labs"}


def read_progress(path):
    if not path.exists():
        return {"schema_version": 1, "labs": {}}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data.get("labs"), dict):
        raise ValueError("Progress file must contain a labs object")
    return data


def mark(path, lab_id, status, note=""):
    if lab_id not in {item["id"] for item in lessons()}:
        raise ValueError("Unknown lab: " + lab_id)
    if status not in STATUSES:
        raise ValueError("Invalid status: " + status)
    data = read_progress(path)
    entry = {"status": status, "note": note, "self_reported": True,
             "updated_at_utc": datetime.now(timezone.utc).isoformat()}
    data["labs"][lab_id] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return entry


def run_lab(lab):
    if lab["kind"] != "python-unittest":
        print("MANUAL: " + lab["reference"], flush=True)
        return 0
    print("TESTING: " + lab["id"], flush=True)
    outcome = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=(ROOT / lab["path"]).resolve(), check=False)
    print(("PASS: " if outcome.returncode == 0 else "FAIL: ") + lab["id"], flush=True)
    return outcome.returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description="Big Data + Cloud Engineering Academy")
    sub = parser.add_subparsers(dest="cmd", required=True)
    listing = sub.add_parser("list", help="List all registered labs and projects")
    listing.add_argument("--track", choices=["big-data", "cloud-computing", "integrated"])
    listing.add_argument("--json", action="store_true")
    sub.add_parser("doctor", help="Inspect local tool availability")
    tests = sub.add_parser("test", help="Run a lab unit-test suite or all suites")
    tests.add_argument("lab", help="Registered lab ID or all")
    progress = sub.add_parser("progress", help="Show locally recorded study progress")
    progress.add_argument("--state", type=Path, default=DEFAULT_PROGRESS)
    update = sub.add_parser("mark", help="Record self-reported study progress")
    update.add_argument("lab")
    update.add_argument("--status", choices=STATUSES, required=True)
    update.add_argument("--note", default="")
    update.add_argument("--state", type=Path, default=DEFAULT_PROGRESS)
    args = parser.parse_args(argv)
    catalog = lessons()

    if args.cmd == "list":
        chosen = [x for x in catalog if args.track is None or x["track"] == args.track]
        if args.json:
            print(json.dumps(chosen, indent=2))
        else:
            for item in chosen:
                print(f"{item['id']:<20} {item['track']:<16} {item['level']:<15} {item['name']}")
        return 0
    if args.cmd == "doctor":
        print(json.dumps(doctor(), indent=2))
        return 0
    if args.cmd == "test":
        if args.lab == "all":
            chosen = [x for x in catalog if x["kind"] == "python-unittest"]
        else:
            chosen = [x for x in catalog if x["id"] == args.lab]
            if not chosen:
                parser.error("Unknown lab: " + args.lab)
        return int(any(run_lab(x) != 0 for x in chosen))
    if args.cmd == "mark":
        try:
            result = mark(args.state, args.lab, args.status, args.note)
        except ValueError as exc:
            parser.error(str(exc))
        print(json.dumps({"lab": args.lab, **result}, indent=2))
        return 0
    if args.cmd == "progress":
        data = read_progress(args.state)
        complete = 0
        for item in catalog:
            status = data["labs"].get(item["id"], {}).get("status", "not-started")
            if status == "done":
                complete += 1
            print(f"{status:<12} {item['id']}")
        print(f"Self-reported done: {complete} of {len(catalog)}")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
