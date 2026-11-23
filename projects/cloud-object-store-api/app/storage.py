"""Storage adapter for both MinIO and Amazon S3."""
from __future__ import annotations

import os
from typing import Any
import boto3
from botocore.exceptions import ClientError


class ObjectStore:
    def __init__(self, client: Any = None, bucket: str | None = None) -> None:
        self.bucket = bucket or os.getenv("S3_BUCKET", "portfolio-lab")
        self.client = client or boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
        )

    def upload(self, key: str, body: bytes, content_type: str) -> str:
        response = self.client.put_object(
            Bucket=self.bucket, Key=key, Body=body,
            ContentType=content_type or "application/octet-stream",
        )
        return response.get("ETag", "").strip('"')

    def download(self, key: str) -> tuple[bytes, str]:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise KeyError(key) from error
            raise
        return (response["Body"].read(), response.get("ContentType", "application/octet-stream"))

    def list_keys(self, prefix: str = "") -> list[dict[str, Any]]:
        objects = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                objects.append({"key": item["Key"], "size": item["Size"]})
        return objects

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)
