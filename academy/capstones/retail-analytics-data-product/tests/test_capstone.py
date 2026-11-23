import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.request import urlopen
import sys

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
from run import build, SOURCE
from api import handler_for


class RetailDataProductTests(unittest.TestCase):
    def test_end_to_end_and_rerun(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            first = build(SOURCE, out)
            second = build(SOURCE, out)
            self.assertEqual(first["loaded_unique_orders"], 4)
            self.assertEqual(first["rejected_rows"], 5)
            self.assertEqual(first["paid_revenue_cents"], 32500)
            self.assertEqual(first["source_sha256"], second["source_sha256"])
            text = (out / "daily_kpis.csv").read_text()
            self.assertIn("Electronics", text)
            self.assertEqual(first["kpi_rows"], 3)

    def test_quality_gate_blocks_publication(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            with self.assertRaisesRegex(ValueError, "quality_gate_failed"):
                build(SOURCE, out, max_reject_ratio=0.1)
            self.assertFalse((out / "daily_kpis.csv").exists())

    def test_api_readout(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            build(SOURCE, out)
            server = ThreadingHTTPServer(("127.0.0.1", 0),
                handler_for(out / "orders.db", out / "manifest.json"))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                with urlopen(base + "/metrics") as resp:
                    self.assertTrue(json.load(resp)["quality_passed"])
                with urlopen(base + "/kpis?category=Electronics") as resp:
                    rows = json.load(resp)["kpis"]
                    self.assertEqual(len(rows), 2)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
