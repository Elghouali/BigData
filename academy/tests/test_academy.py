"""Academy catalog integrity, progress and CLI tests."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "academy.py"


def invoke(*args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True, text=True, check=False,
    )


class AcademyTests(unittest.TestCase):
    def test_catalog_files_exist_and_ids_are_unique(self):
        items = json.loads((ROOT / "catalog.json").read_text())["labs"]
        ids = [item["id"] for item in items]
        self.assertEqual(len(ids), len(set(ids)))
        for item in items:
            self.assertTrue((ROOT / item["path"]).is_dir(), item["id"])
            self.assertTrue((ROOT / item["reference"]).is_file(), item["id"])
            self.assertIn(item["kind"], ("manual", "python-unittest"))

    def test_filtered_json_list(self):
        result = invoke("list", "--track", "big-data", "--json")
        self.assertEqual(result.returncode, 0, result.stderr)
        items = json.loads(result.stdout)
        self.assertGreaterEqual(len(items), 2)
        self.assertTrue(all(item["track"] == "big-data" for item in items))

    def test_doctor_does_not_modify_system(self):
        result = invoke("doctor")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("python_supported", json.loads(result.stdout))

    def test_local_progress(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "progress.json")
            result = invoke("mark", "sql-etl", "--status", "in-progress",
                            "--state", path, "--note", "Learning transactions")
            self.assertEqual(result.returncode, 0, result.stderr)
            state = json.loads(Path(path).read_text())
            self.assertEqual(state["labs"]["sql-etl"]["status"], "in-progress")
            self.assertTrue(state["labs"]["sql-etl"]["self_reported"])
            view = invoke("progress", "--state", path)
            self.assertIn("in-progress", view.stdout)

    def test_unknown_lab_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            result = invoke("mark", "no-such-lab", "--status", "done",
                            "--state", str(Path(d) / "p.json"))
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
