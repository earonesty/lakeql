from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from math import ceil
from pathlib import Path

import numpy as np

from .embedders import ImageEmbedder
from .hashing import sha256_file
from .images import ImageFetcher, ImageFetchError
from .jsonio import read_json, read_jsonl, write_json_atomic
from .models import BuildManifest, FailureRecord, MediaRecord, ShardReceipt
from .parquetio import validate_embeddings, write_embeddings, write_failures
from .paths import embedding_path, failure_path, receipt_path


def run_bucket(
    *,
    plan: Path,
    output: Path,
    bucket: str,
    embedder: ImageEmbedder,
    batch_size: int,
    download_concurrency: int,
    force: bool = False,
) -> ShardReceipt:
    manifest = BuildManifest.from_dict(read_json(plan / "manifest.json"))
    if bucket not in manifest.buckets:
        raise ValueError(f"bucket {bucket} is not present in the build manifest")
    if embedder.model_id != manifest.model_id:
        raise RuntimeError(
            f"embedder model {embedder.model_id} does not match plan {manifest.model_id}"
        )
    if embedder.preprocessing_id != manifest.preprocessing_id:
        raise RuntimeError(
            "embedder preprocessing "
            f"{embedder.preprocessing_id} does not match plan {manifest.preprocessing_id}"
        )
    input_path = plan / "buckets" / f"{bucket}.jsonl"
    input_sha256 = sha256_file(input_path)
    completed_receipt_path = receipt_path(output, bucket)
    if completed_receipt_path.exists() and not force:
        receipt = ShardReceipt(**read_json(completed_receipt_path))
        if receipt.input_sha256 != input_sha256:
            raise RuntimeError(f"{bucket}: completed receipt refers to different input")
        if receipt.release_id != manifest.release_id:
            raise RuntimeError(f"{bucket}: completed receipt refers to a different release")
        if receipt.model_fingerprint != embedder.model_fingerprint:
            raise RuntimeError(f"{bucket}: completed receipt uses a different checkpoint")
        _validate_completed_output(output, receipt, embedder.dimension)
        return receipt

    records = [MediaRecord.from_dict(value) for value in read_jsonl(input_path)]
    if len(records) != manifest.buckets[bucket]:
        raise RuntimeError(f"{bucket}: bucket record count does not match manifest")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if download_concurrency <= 0:
        raise ValueError("download_concurrency must be positive")

    started_at = datetime.now(UTC)
    started = time.monotonic()
    failures: list[FailureRecord] = []
    embedded_records: list[MediaRecord] = []
    embedding_chunks: list[np.ndarray] = []
    downloaded_bytes = 0
    bucket_byte_budget = ceil(
        manifest.budgets.max_total_image_bytes
        * len(records)
        / manifest.records
    )
    batches = 0
    fetcher = ImageFetcher(
        max_image_bytes=manifest.budgets.max_image_bytes,
        timeout_seconds=manifest.budgets.request_timeout_seconds,
    )
    try:
        with ThreadPoolExecutor(max_workers=download_concurrency) as executor:
            for start in range(0, len(records), batch_size):
                batch_records = records[start : start + batch_size]
                results = list(
                    executor.map(
                        lambda record: _fetch_record(fetcher, record),
                        batch_records,
                    )
                )
                ready_records: list[MediaRecord] = []
                ready_images = []
                for record, result in zip(batch_records, results, strict=True):
                    if isinstance(result, FailureRecord):
                        failures.append(result)
                        continue
                    downloaded_bytes += result.bytes_read
                    if downloaded_bytes > bucket_byte_budget:
                        raise RuntimeError(
                            "bucket image byte budget exceeded "
                            f"({downloaded_bytes} > {bucket_byte_budget})"
                        )
                    ready_records.append(record)
                    ready_images.append(result.image)
                if not ready_records:
                    continue
                try:
                    vectors = embedder.encode(ready_images)
                except Exception as error:
                    failures.extend(
                        FailureRecord(
                            media_id=record.media_id,
                            stage="inference",
                            retryable=False,
                            error_type=type(error).__name__,
                            message=str(error)[:2000],
                            attempts=1,
                        )
                        for record in ready_records
                    )
                    continue
                if vectors.shape != (len(ready_records), embedder.dimension):
                    raise RuntimeError(f"embedder returned invalid shape {vectors.shape}")
                norms = np.linalg.norm(vectors, axis=1)
                if not np.isfinite(vectors).all() or not np.allclose(norms, 1.0, atol=2e-4):
                    raise RuntimeError("embedder returned non-finite or non-normalized vectors")
                embedded_records.extend(ready_records)
                embedding_chunks.append(np.asarray(vectors, dtype=np.float32))
                batches += 1
    finally:
        fetcher.close()

    embeddings = (
        np.concatenate(embedding_chunks, axis=0)
        if embedding_chunks
        else np.empty((0, embedder.dimension), dtype=np.float32)
    )
    output_path = embedding_path(output, embedder.model_id, bucket)
    failures_path = failure_path(output, bucket)
    temporary_output = output_path.with_name(f".{output_path.name}.{os.getpid()}.part")
    temporary_failures = failures_path.with_name(f".{failures_path.name}.{os.getpid()}.part")
    temporary_output.parent.mkdir(parents=True, exist_ok=True)
    temporary_failures.parent.mkdir(parents=True, exist_ok=True)
    write_embeddings(
        temporary_output,
        embedded_records,
        embeddings,
        model_id=embedder.model_id,
        model_fingerprint=embedder.model_fingerprint,
        preprocessing_id=embedder.preprocessing_id,
    )
    write_failures(temporary_failures, failures)
    validate_embeddings(
        temporary_output,
        expected_rows=len(embedded_records),
        expected_dimension=embedder.dimension,
        expected_model_fingerprint=embedder.model_fingerprint,
    )
    os.replace(temporary_output, output_path)
    os.replace(temporary_failures, failures_path)
    elapsed = time.monotonic() - started
    receipt = ShardReceipt(
        schema_version=1,
        release_id=manifest.release_id,
        bucket=bucket,
        input_sha256=input_sha256,
        output_sha256=sha256_file(output_path),
        failures_sha256=sha256_file(failures_path),
        model_id=embedder.model_id,
        model_fingerprint=embedder.model_fingerprint,
        preprocessing_id=embedder.preprocessing_id,
        device=embedder.device_name,
        started_at=started_at.isoformat(),
        completed_at=datetime.now(UTC).isoformat(),
        elapsed_seconds=elapsed,
        planned_records=len(records),
        embedded_records=len(embedded_records),
        failed_records=len(failures),
        downloaded_bytes=downloaded_bytes,
        batches=batches,
        images_per_second=len(embedded_records) / elapsed if elapsed else 0.0,
        metadata={"bucket_image_byte_budget": bucket_byte_budget},
    )
    write_json_atomic(completed_receipt_path, receipt.to_dict())
    return receipt


def _fetch_record(fetcher: ImageFetcher, record: MediaRecord):
    try:
        return fetcher.fetch(record.image_url)
    except ImageFetchError as error:
        stage = "decode" if str(error).startswith("image decode failed") else "download"
        return FailureRecord(
            media_id=record.media_id,
            stage=stage,
            retryable=error.retryable,
            error_type=type(error).__name__,
            message=str(error)[:2000],
            attempts=error.attempts,
        )
    except Exception as error:
        return FailureRecord(
            media_id=record.media_id,
            stage="download",
            retryable=False,
            error_type=type(error).__name__,
            message=str(error)[:2000],
            attempts=1,
        )


def _validate_completed_output(
    output: Path,
    receipt: ShardReceipt,
    expected_dimension: int,
) -> None:
    output_path = embedding_path(output, receipt.model_id, receipt.bucket)
    failures_path = failure_path(output, receipt.bucket)
    if sha256_file(output_path) != receipt.output_sha256:
        raise RuntimeError(f"{receipt.bucket}: embedding output checksum mismatch")
    if sha256_file(failures_path) != receipt.failures_sha256:
        raise RuntimeError(f"{receipt.bucket}: failure output checksum mismatch")
    validate_embeddings(
        output_path,
        expected_rows=receipt.embedded_records,
        expected_dimension=expected_dimension,
        expected_model_fingerprint=receipt.model_fingerprint,
    )
