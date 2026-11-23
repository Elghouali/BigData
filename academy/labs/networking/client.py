"""GET retry policy: bounded attempts and exponential backoff."""
import argparse
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def fetch(url, attempts=3, timeout=2.0, sleep=time.sleep):
    if attempts <= 0:
        raise ValueError("attempts must be >= 1")
    error = None
    for n in range(attempts):
        try:
            with urlopen(url, timeout=timeout) as response:
                return {"status": response.status, "attempts": n + 1,
                        "payload": json.loads(response.read())}
        except HTTPError as exc:
            if exc.code not in (429, 502, 503, 504):
                raise
            error = exc
        except URLError as exc:
            error = exc
        if n + 1 < attempts:
            sleep(0.05 * (2 ** n))
    raise RuntimeError(f"GET failed after {attempts} attempts: {error}") from error


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:8089/flaky")
    args = p.parse_args()
    print(json.dumps(fetch(args.url), indent=2))
