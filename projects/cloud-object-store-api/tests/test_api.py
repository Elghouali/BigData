"""Tests use a fake storage adapter, no AWS account needed."""
import pytest
from fastapi.testclient import TestClient
from app.main import app, get_store, MAX_UPLOAD_BYTES


class FakeStore:
    def __init__(self):
        self.objects = {}

    def upload(self, key, body, content_type):
        self.objects[key] = (body, content_type)
        return "test-etag"

    def download(self, key):
        if key not in self.objects:
            raise KeyError(key)
        return self.objects[key]

    def list_keys(self, prefix=""):
        return [{"key": key, "size": len(val[0])}
                for key, val in sorted(self.objects.items()) if key.startswith(prefix)]

    def delete(self, key):
        self.objects.pop(key, None)


@pytest.fixture
def client():
    fake = FakeStore()
    app.dependency_overrides[get_store] = lambda: fake
    with TestClient(app) as api:
        yield api
    app.dependency_overrides.clear()


def test_upload_read_list_delete(client):
    result = client.put("/objects/reports/hello.txt", content=b"cloud", headers={"content-type": "text/plain"})
    assert result.status_code == 200
    assert result.json() == {"key": "reports/hello.txt", "bytes": 5, "etag": "test-etag"}
    assert client.get("/objects?prefix=reports/").json()["objects"] == [
        {"key": "reports/hello.txt", "size": 5}
    ]
    assert client.get("/objects/reports/hello.txt").content == b"cloud"
    assert client.delete("/objects/reports/hello.txt").status_code == 204
    assert client.get("/objects/reports/hello.txt").status_code == 404


def test_upload_size_limit(client):
    response = client.put("/objects/too-large.bin", content=b"x" * (MAX_UPLOAD_BYTES + 1))
    assert response.status_code == 413


def test_empty_object_roundtrip(client):
    response = client.put("/objects/empty.txt", content=b"")
    assert response.status_code == 200
    assert response.json()["bytes"] == 0
    assert client.get("/objects/empty.txt").content == b""


def test_invalid_object_key(client):
    assert client.put("/objects/folder//bad.txt", content=b"x").status_code == 400
