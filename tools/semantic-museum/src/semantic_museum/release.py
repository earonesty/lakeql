from __future__ import annotations

import os
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .hashing import sha256_file
from .jsonio import read_json, read_jsonl, write_json_atomic
from .models import BuildManifest, MediaRecord, ShardReceipt
from .parquetio import validate_embeddings, write_metadata
from .paths import embedding_path, failure_path, receipt_path


def release_status(*, plan: Path, output: Path) -> dict[str, Any]:
    manifest = BuildManifest.from_dict(read_json(plan / "manifest.json"))
    completed: dict[str, ShardReceipt] = {}
    invalid: dict[str, str] = {}
    for bucket in manifest.buckets:
        completed_receipt_path = receipt_path(output, bucket)
        if not completed_receipt_path.exists():
            continue
        try:
            receipt = ShardReceipt(**read_json(completed_receipt_path))
            _validate_receipt(output, receipt, manifest)
            completed[bucket] = receipt
        except Exception as error:
            invalid[bucket] = str(error)
    embedded = sum(receipt.embedded_records for receipt in completed.values())
    failed = sum(receipt.failed_records for receipt in completed.values())
    elapsed = sum(receipt.elapsed_seconds for receipt in completed.values())
    model_fingerprints = sorted(
        {receipt.model_fingerprint for receipt in completed.values()}
    )
    if len(model_fingerprints) > 1:
        invalid["__release__"] = "buckets use different model checkpoints"
    downloaded_bytes = sum(
        receipt.downloaded_bytes for receipt in completed.values()
    )
    if downloaded_bytes > manifest.budgets.max_total_image_bytes:
        invalid["__release__"] = "global image byte budget exceeded"
    return {
        "release_id": manifest.release_id,
        "planned_buckets": len(manifest.buckets),
        "completed_buckets": len(completed),
        "missing_buckets": sorted(set(manifest.buckets) - set(completed)),
        "invalid_buckets": invalid,
        "planned_records": manifest.records,
        "processed_records": embedded + failed,
        "embedded_records": embedded,
        "failed_records": failed,
        "model_fingerprints": model_fingerprints,
        "downloaded_bytes": downloaded_bytes,
        "elapsed_worker_seconds": elapsed,
        "images_per_second": embedded / elapsed if elapsed else 0.0,
        "complete": len(completed) == len(manifest.buckets) and not invalid,
    }


def finalize_release(*, plan: Path, output: Path) -> dict[str, Any]:
    manifest = BuildManifest.from_dict(read_json(plan / "manifest.json"))
    status = release_status(plan=plan, output=output)
    if not status["complete"]:
        raise RuntimeError("release has missing or invalid buckets")
    metadata_files = _write_metadata_partitions(plan=plan, output=output, manifest=manifest)
    objects: list[dict[str, Any]] = []
    for path in sorted(_release_objects(output)):
        objects.append(
            {
                "path": path.relative_to(output).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    release = {
        "schema_version": 1,
        "release_id": manifest.release_id,
        "created_at": datetime.now(UTC).isoformat(),
        "build_manifest": manifest.to_dict(),
        "status": status,
        "metadata_files": metadata_files,
        "objects": objects,
    }
    write_json_atomic(output / "release.json", release)
    return release


def _write_metadata_partitions(
    *, plan: Path, output: Path, manifest: BuildManifest
) -> list[dict[str, Any]]:
    written: list[dict[str, Any]] = []
    for bucket in manifest.buckets:
        groups: dict[str, list[MediaRecord]] = defaultdict(list)
        for value in read_jsonl(plan / "buckets" / f"{bucket}.jsonl"):
            record = MediaRecord.from_dict(value)
            groups[record.unit or "unknown"].append(record)
        for unit, records in sorted(groups.items()):
            safe_unit = _safe_partition_value(unit)
            path = (
                output
                / "metadata"
                / f"source={manifest.source_name}"
                / f"unit={safe_unit}"
                / f"bucket={bucket}"
                / "part-00000.parquet"
            )
            temporary = path.with_name(f".{path.name}.{os.getpid()}.part")
            write_metadata(temporary, records)
            os.replace(temporary, path)
            written.append(
                {
                    "path": path.relative_to(output).as_posix(),
                    "rows": len(records),
                    "sha256": sha256_file(path),
                }
            )
    if sum(part["rows"] for part in written) != manifest.records:
        raise RuntimeError("metadata row count does not match build manifest")
    return written


def _release_objects(output: Path):
    for directory in ("embeddings", "failures", "metadata", "receipts"):
        root = output / directory
        if root.exists():
            yield from (path for path in root.rglob("*") if path.is_file())


def _validate_receipt(
    output: Path, receipt: ShardReceipt, manifest: BuildManifest
) -> None:
    if receipt.release_id != manifest.release_id:
        raise RuntimeError("receipt release identity mismatch")
    if receipt.model_id != manifest.model_id:
        raise RuntimeError("receipt model identity mismatch")
    if receipt.preprocessing_id != manifest.preprocessing_id:
        raise RuntimeError("receipt preprocessing identity mismatch")
    completed_embedding_path = embedding_path(
        output, receipt.model_id, receipt.bucket
    )
    completed_failure_path = failure_path(output, receipt.bucket)
    if sha256_file(completed_embedding_path) != receipt.output_sha256:
        raise RuntimeError("embedding checksum mismatch")
    if sha256_file(completed_failure_path) != receipt.failures_sha256:
        raise RuntimeError("failure checksum mismatch")
    validate_embeddings(
        completed_embedding_path,
        expected_rows=receipt.embedded_records,
        expected_dimension=512,
        expected_model_fingerprint=receipt.model_fingerprint,
    )
    if (
        pq.ParquetFile(completed_failure_path).metadata.num_rows
        != receipt.failed_records
    ):
        raise RuntimeError("failure row count mismatch")
    if receipt.embedded_records + receipt.failed_records != receipt.planned_records:
        raise RuntimeError("receipt does not account for every planned record")


def _safe_partition_value(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "-_." else "_"
        for character in value
    )
