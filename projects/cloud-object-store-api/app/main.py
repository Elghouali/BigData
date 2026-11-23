"""Educational object-storage REST API: localhost only, no authentication."""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from .storage import ObjectStore

app = FastAPI(
    title="Cloud Object Storage Lab",
    description="Local-only S3-compatible file API for cloud engineering practice",
    version="1.0.0",
)
MAX_UPLOAD_BYTES = 1024 * 1024


def get_store() -> ObjectStore:
    return ObjectStore()


def validate_key(key: str) -> None:
    parts = key.split("/")
    if (not key or len(key) > 256 or "\\" in key
            or any(part in (".", "..", "") for part in parts)):
        raise HTTPException(status_code=400, detail="Invalid object key")


@app.get("/health")
def health():
    return {"status": "ok", "note": "S3 connection not checked"}


@app.get("/objects")
def list_objects(prefix: str = "", store: ObjectStore = Depends(get_store)):
    if len(prefix) > 256:
        raise HTTPException(status_code=400, detail="Prefix too long")
    return {"objects": store.list_keys(prefix)}


@app.put("/objects/{key:path}")
async def upload_object(key: str, request: Request, store: ObjectStore = Depends(get_store)):
    validate_key(key)
    # This training app buffers requests: do not expose it to untrusted clients.
    data = await request.body()
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Upload exceeds 1 MiB")
    content_type = request.headers.get("content-type", "application/octet-stream")
    etag = store.upload(key, data, content_type)
    return {"key": key, "bytes": len(data), "etag": etag}


@app.get("/objects/{key:path}")
def download_object(key: str, store: ObjectStore = Depends(get_store)):
    validate_key(key)
    try:
        body, content_type = store.download(key)
    except KeyError:
        raise HTTPException(status_code=404, detail="Object not found") from None
    return Response(content=body, media_type=content_type)


@app.delete("/objects/{key:path}", status_code=204)
def delete_object(key: str, store: ObjectStore = Depends(get_store)):
    validate_key(key)
    store.delete(key)
    return Response(status_code=204)
