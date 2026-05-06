from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import yaml


def is_s3_uri(path: str) -> bool:
    return path.startswith("s3://")


def split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Not a valid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def bucket_from_s3_uri(uri: str) -> str | None:
    return split_s3_uri(uri)[0] if is_s3_uri(uri) else None


def join_path(base_path: str, *parts: str) -> str:
    if is_s3_uri(base_path):
        clean_base = base_path.rstrip("/")
        clean_parts = "/".join(part.strip("/") for part in parts if part)
        return f"{clean_base}/{clean_parts}" if clean_parts else clean_base

    return str(Path(base_path).joinpath(*parts))


def read_text(path: str) -> str:
    if is_s3_uri(path):
        bucket, key = split_s3_uri(path)
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    return Path(path).read_text(encoding="utf-8")


def write_text(path: str, text: str, content_type: str = "application/json") -> None:
    if is_s3_uri(path):
        bucket, key = split_s3_uri(path)
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=text.encode("utf-8"),
            ContentType=content_type,
        )
        return

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")


def load_yaml(path: str) -> dict[str, Any]:
    return yaml.safe_load(read_text(path))


def resolve_layer_path(
    config: dict[str, Any],
    layer: str,
    storage_mode: str,
    config_path: str | None = None,
) -> str:
    storage = config.get("storage", {})

    local_key = f"local_{layer}_dir"
    prefix_key = f"{layer}_prefix"
    env_prefix_key = f"{layer.upper()}_PREFIX"

    local_path = storage.get(local_key) or config.get(local_key) or f"data/{layer}/wistia"
    if storage_mode == "local":
        return local_path

    if storage_mode != "s3":
        raise ValueError(f"Unsupported storage mode: {storage_mode}")

    bucket_name = (
        os.getenv("S3_BUCKET_NAME")
        or storage.get("s3_bucket")
        or (bucket_from_s3_uri(config_path) if config_path else None)
    )
    if not bucket_name:
        raise ValueError("S3 storage mode requires S3_BUCKET_NAME, storage.s3_bucket, or an s3:// config path.")

    layer_prefix = os.getenv(env_prefix_key) or storage.get(prefix_key) or f"{layer}/wistia"
    return f"s3://{bucket_name}/{layer_prefix.strip('/')}"
