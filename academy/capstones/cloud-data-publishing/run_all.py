"""End-to-end retail data product -> localhost object store."""
import argparse
import json
from pathlib import Path
import sys

THIS = Path(__file__).resolve().parent
P1 = THIS.parent / "retail-analytics-data-product"
sys.path.insert(0, str(P1))
from run import build, SOURCE  # noqa: E402
from publisher import publish_verified  # noqa: E402


def execute(endpoint: str, output: Path) -> list[dict]:
    build(SOURCE, output)
    return [
        publish_verified(endpoint, "reports/retail/daily_kpis.csv", output / "daily_kpis.csv"),
        publish_verified(endpoint, "reports/retail/manifest.json", output / "manifest.json"),
    ]


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", default="http://127.0.0.1:8000")
    p.add_argument("--output", type=Path, default=THIS / "output")
    args = p.parse_args()
    print(json.dumps(execute(args.endpoint, args.output), indent=2))
