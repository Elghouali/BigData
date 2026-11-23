import sys
import threading
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from server import Handler, State
from client import fetch


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Handler.state = State()
        cls.s = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.s.server_address[1]
        cls.thread = threading.Thread(target=cls.s.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.s.shutdown()
        cls.s.server_close()
        cls.thread.join(timeout=2)

    def test_health(self):
        self.assertEqual(fetch(f"http://127.0.0.1:{self.port}/health")["status"], 200)

    def test_flaky_retries_once(self):
        Handler.state = State()
        self.assertEqual(fetch(f"http://127.0.0.1:{self.port}/flaky", sleep=lambda _: None)["attempts"], 2)

    def test_down_has_bounded_retries(self):
        with self.assertRaisesRegex(RuntimeError, "after 3 attempts"):
            fetch(f"http://127.0.0.1:{self.port}/down", sleep=lambda _: None)


if __name__ == "__main__":
    unittest.main()
