import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from publisher import publish_verified


class StoreHandler(BaseHTTPRequestHandler):
    files = {}
    corrupt_get = False

    def do_PUT(self):
        data = self.rfile.read(int(self.headers["Content-Length"]))
        self.files[self.path] = data
        reply = json.dumps({"etag": "fake-etag"}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def do_GET(self):
        data = self.files.get(self.path)
        if data is None:
            self.send_error(404)
            return
        if self.corrupt_get:
            data += b"CORRUPTED"
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class PublisherTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), StoreHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.endpoint = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def test_upload_download_and_checksum(self):
        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "report.csv"
            source.write_text("date,total\n2026-11-01,10\n")
            result = publish_verified(self.endpoint, "reports/report.csv", source)
            self.assertEqual(result["verification"], "passed")
            self.assertEqual(result["size_bytes"], len(source.read_bytes()))

    def test_detects_corruption(self):
        StoreHandler.corrupt_get = True
        try:
            with tempfile.TemporaryDirectory() as d:
                source = Path(d) / "report.csv"
                source.write_text("a,b\n")
                with self.assertRaisesRegex(RuntimeError, "checksum_mismatch"):
                    publish_verified(self.endpoint, "reports/corrupted.csv", source)
        finally:
            StoreHandler.corrupt_get = False

    def test_rejects_remote_http(self):
        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "report.csv"
            source.write_text("x\n")
            with self.assertRaises(ValueError):
                publish_verified("http://example.org", "reports/x.csv", source)


if __name__ == "__main__":
    unittest.main()
