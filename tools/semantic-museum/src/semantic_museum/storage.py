from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .hashing import canonical_json_bytes, sha256_bytes, sha256_file
from .jsonio import read_json
from .models import BuildManifest, ShardReceipt
from .paths import embedding_path, failure_path, receipt_path


def publish_release(
    *,
    output: Path,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
) -> dict[str, Any]:
    release_path = output / "release.json"
    release = read_json(release_path)
    _validate_local_release(output, release)
    release_id = str(release["release_id"])
    normalized_prefix = prefix.strip("/")
    release_prefix = f"{normalized_prefix}/releases/{release_id}".lstrip("/")
    client = _client(endpoint_url)
    objects = [*release["objects"], _object_description(output, release_path)]
    uploaded = 0
    reused = 0
    for item in objects:
        path = output / item["path"]
        key = f"{release_prefix}/{item['path']}"
        if _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
            reused += 1
            continue
        with path.open("rb") as stream:
            client.upload_fileobj(
                stream,
                bucket,
                key,
                ExtraArgs={
                    "ContentType": _content_type(path),
                    "Metadata": {"sha256": item["sha256"]},
                },
            )
        if not _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
            raise RuntimeError(f"uploaded object failed validation: {key}")
        uploaded += 1

    release_sha256 = sha256_file(release_path)
    current = {
        "schema_version": 1,
        "release_id": release_id,
        "release_manifest": f"{release_prefix}/release.json",
        "release_manifest_sha256": release_sha256,
    }
    current_bytes = canonical_json_bytes(current) + b"\n"
    current_key = f"{normalized_prefix}/current.json".lstrip("/")
    client.put_object(
        Bucket=bucket,
        Key=current_key,
        Body=current_bytes,
        ContentType="application/json",
        Metadata={"sha256": sha256_bytes(current_bytes)},
    )
    return {
        "release_id": release_id,
        "bucket": bucket,
        "prefix": release_prefix,
        "uploaded_objects": uploaded,
        "reused_objects": reused,
        "published_current": current_key,
    }


def publish_bucket(
    *,
    plan: Path,
    output: Path,
    bucket_name: str,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
) -> dict[str, Any]:
    manifest = BuildManifest.from_dict(read_json(plan / "manifest.json"))
    completed_receipt_path = receipt_path(output, bucket_name)
    receipt = ShardReceipt(**read_json(completed_receipt_path))
    if receipt.release_id != manifest.release_id:
        raise RuntimeError("bucket receipt belongs to a different release")
    paths = [
        embedding_path(output, receipt.model_id, bucket_name),
        failure_path(output, bucket_name),
        completed_receipt_path,
    ]
    client = _client(endpoint_url)
    release_prefix = _release_prefix(prefix, manifest.release_id)
    uploaded = 0
    reused = 0
    for path in paths:
        item = _object_description(output, path)
        key = f"{release_prefix}/{item['path']}"
        if _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
            reused += 1
            continue
        _upload_object(client, bucket, key, path, item)
        if not _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
            raise RuntimeError(f"uploaded object failed validation: {key}")
        uploaded += 1
    return {
        "release_id": manifest.release_id,
        "bucket": bucket_name,
        "uploaded_objects": uploaded,
        "reused_objects": reused,
    }


def publish_plan(
    *,
    plan: Path,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
) -> dict[str, Any]:
    manifest = BuildManifest.from_dict(read_json(plan / "manifest.json"))
    paths = [plan / "manifest.json"]
    paths.extend(plan / "buckets" / f"{name}.jsonl" for name in manifest.buckets)
    client = _client(endpoint_url)
    plan_prefix = f"{prefix.strip('/')}/plans/{manifest.release_id}".lstrip("/")
    objects: list[dict[str, Any]] = []
    uploaded = 0
    reused = 0
    for path in paths:
        item = _object_description(plan, path)
        key = f"{plan_prefix}/{item['path']}"
        if _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
            reused += 1
        else:
            _upload_object(client, bucket, key, path, item)
            if not _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
                raise RuntimeError(f"uploaded plan object failed validation: {key}")
            uploaded += 1
        objects.append(item)
    index = {
        "schema_version": 1,
        "release_id": manifest.release_id,
        "objects": objects,
    }
    index_bytes = canonical_json_bytes(index) + b"\n"
    index_key = f"{plan_prefix}/index.json"
    index_sha256 = sha256_bytes(index_bytes)
    client.put_object(
        Bucket=bucket,
        Key=index_key,
        Body=index_bytes,
        ContentType="application/json",
        Metadata={"sha256": index_sha256},
    )
    if not _remote_matches(client, bucket, index_key, index_sha256, len(index_bytes)):
        raise RuntimeError("uploaded plan index failed validation")
    return {
        "release_id": manifest.release_id,
        "prefix": plan_prefix,
        "uploaded_objects": uploaded,
        "reused_objects": reused,
        "published_index": index_key,
    }


def restore_bucket(
    *,
    plan: Path,
    output: Path,
    bucket_name: str,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
) -> bool:
    manifest = BuildManifest.from_dict(read_json(plan / "manifest.json"))
    client = _client(endpoint_url)
    release_prefix = _release_prefix(prefix, manifest.release_id)
    remote_receipt_key = f"{release_prefix}/receipts/{bucket_name}.json"
    try:
        response = client.get_object(Bucket=bucket, Key=remote_receipt_key)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404 or error.response.get("Error", {}).get("Code") in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return False
        raise
    receipt = ShardReceipt(**json.loads(response["Body"].read()))
    if receipt.release_id != manifest.release_id:
        raise RuntimeError("remote bucket receipt belongs to a different release")
    if receipt.model_id != manifest.model_id:
        raise RuntimeError("remote bucket receipt uses a different model")
    targets = [
        (
            embedding_path(output, receipt.model_id, bucket_name),
            receipt.output_sha256,
        ),
        (failure_path(output, bucket_name), receipt.failures_sha256),
    ]
    for path, expected_sha256 in targets:
        key = f"{release_prefix}/{path.relative_to(output).as_posix()}"
        _download_verified(client, bucket, key, path, expected_sha256)
    completed_receipt_path = receipt_path(output, bucket_name)
    completed_receipt_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = completed_receipt_path.with_name(
        f".{completed_receipt_path.name}.{os.getpid()}.part"
    )
    temporary.write_bytes(canonical_json_bytes(receipt.to_dict()) + b"\n")
    os.replace(temporary, completed_receipt_path)
    return True


def upload_terminal_receipt(
    *,
    path: Path,
    bucket: str,
    key: str,
    endpoint_url: str | None,
) -> None:
    client = _client(endpoint_url)
    item = {
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }
    normalized_key = key.lstrip("/")
    _upload_object(client, bucket, normalized_key, path, item)
    if not _remote_matches(
        client,
        bucket,
        normalized_key,
        item["sha256"],
        item["bytes"],
    ):
        raise RuntimeError("terminal receipt upload failed validation")


def upload_diagnostic_bundle(
    *,
    directory: Path,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
) -> dict[str, Any]:
    client = _client(endpoint_url)
    normalized_prefix = prefix.strip("/")
    paths = sorted(path for path in directory.rglob("*") if path.is_file())
    objects: list[dict[str, Any]] = []
    for path in paths:
        item = _object_description(directory, path)
        key = f"{normalized_prefix}/{item['path']}".lstrip("/")
        if not _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
            _upload_object(client, bucket, key, path, item)
            if not _remote_matches(client, bucket, key, item["sha256"], item["bytes"]):
                raise RuntimeError(f"uploaded diagnostic object failed validation: {key}")
        objects.append(item)
    manifest = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "objects": objects,
    }
    manifest_bytes = canonical_json_bytes(manifest) + b"\n"
    manifest_key = f"{normalized_prefix}/manifest.json".lstrip("/")
    manifest_sha256 = sha256_bytes(manifest_bytes)
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
        Metadata={"sha256": manifest_sha256},
    )
    if not _remote_matches(client, bucket, manifest_key, manifest_sha256, len(manifest_bytes)):
        raise RuntimeError("uploaded diagnostic manifest failed validation")
    return {
        "objects": len(objects),
        "bytes": sum(int(item["bytes"]) for item in objects),
        "manifest_key": manifest_key,
    }


def _validate_local_release(output: Path, release: dict[str, Any]) -> None:
    if not release.get("status", {}).get("complete"):
        raise RuntimeError("release manifest is not complete")
    for item in release.get("objects", []):
        path = output / item["path"]
        if path.stat().st_size != item["bytes"]:
            raise RuntimeError(f"release object size mismatch: {item['path']}")
        if sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"release object checksum mismatch: {item['path']}")


def _object_description(output: Path, path: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(output).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _remote_matches(client: Any, bucket: str, key: str, sha256: str, size: int) -> bool:
    try:
        result = client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404 or error.response.get("Error", {}).get("Code") in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return False
        raise
    return (
        int(result["ContentLength"]) == size and result.get("Metadata", {}).get("sha256") == sha256
    )


def _content_type(path: Path) -> str:
    if path.suffix == ".json":
        return "application/json"
    if path.suffix == ".parquet":
        return "application/vnd.apache.parquet"
    return "application/octet-stream"


def _client(endpoint_url: str | None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.environ.get("R2_ENDPOINT_URL"),
        region_name=os.environ.get("R2_REGION", "auto"),
        aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY"),
        config=Config(
            retries={"max_attempts": 8, "mode": "adaptive"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _release_prefix(prefix: str, release_id: str) -> str:
    return f"{prefix.strip('/')}/releases/{release_id}".lstrip("/")


def _upload_object(
    client: Any,
    bucket: str,
    key: str,
    path: Path,
    item: dict[str, Any],
) -> None:
    with path.open("rb") as stream:
        client.upload_fileobj(
            stream,
            bucket,
            key,
            ExtraArgs={
                "ContentType": _content_type(path),
                "Metadata": {"sha256": item["sha256"]},
            },
        )


def _download_verified(
    client: Any,
    bucket: str,
    key: str,
    path: Path,
    expected_sha256: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.part")
    with temporary.open("wb") as stream:
        client.download_fileobj(bucket, key, stream)
    if sha256_file(temporary) != expected_sha256:
        temporary.unlink()
        raise RuntimeError(f"downloaded object checksum mismatch: {key}")
    os.replace(temporary, path)
