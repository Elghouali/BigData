"""Publish an artifact through a localhost-only object API, then verify SHA256."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen


def publish_verified(endpoint: str, key: str, source: Path, timeout: float = 5.0) -> dict:
    parsed = urlsplit(endpoint)
    if parsed.scheme != "http" or parsed.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("reference publisher permits only local HTTP endpoints")
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise ValueError("unsafe object key")
    data = source.read_bytes()
    if len(data) > 1024 * 1024:
        raise ValueError("reference API upload limit is 1 MiB")
    url = endpoint.rstrip("/") + "/objects/" + quote(key, safe="/")
    content_type = "application/json" if source.suffix == ".json" else "text/csv"
    with urlopen(Request(url, data=data, method="PUT",
                         headers={"Content-Type": content_type}), timeout=timeout) as response:
        receipt = json.loads(response.read())
    with urlopen(url, timeout=timeout) as response:
        downloaded = response.read()
    expected = hashlib.sha256(data).hexdigest()
    actual = hashlib.sha256(downloaded).hexdigest()
    if expected != actual:
        raise RuntimeError("checksum_mismatch")
    return {"key": key, "size_bytes": len(data),
            "sha256": expected, "etag": receipt.get("etag", ""),
            "verification": "passed"}
