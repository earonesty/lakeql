from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .discovery import SMITHSONIAN_INDEX, discover_source_objects
from .embedders import MobileClip2S0Embedder
from .jsonio import write_json_atomic
from .models import BuildBudgets
from .planner import build_plan
from .release import finalize_release, release_status
from .storage import publish_bucket, publish_plan, publish_release
from .worker import run_bucket

CALIBRATION_INDEX = (
    "https://smithsonian-open-access.s3-us-west-2.amazonaws.com/"
    "metadata/edan/saam/index.txt"
)


def run_cloud_job(
    *,
    work: Path,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
    device: str,
    batch_size: int,
    download_concurrency: int,
    calibration_records: int,
    max_records: int,
) -> dict[str, Any]:
    work.mkdir(parents=True, exist_ok=True)
    state_path = work / "state.json"
    _write_state(state_path, phase="calibration_source_discovery")
    calibration_sources = discover_source_objects(
        [CALIBRATION_INDEX],
        max_bytes=16 * 1024**2,
        timeout_seconds=30,
    )
    _write_state(state_path, phase="calibration_plan")
    calibration_plan = work / "calibration-plan"
    calibration_output = work / "calibration-release"
    calibration = build_plan(
        output=calibration_plan,
        sources=calibration_sources,
        budgets=BuildBudgets(
            max_records=calibration_records,
            max_source_bytes=2 * 1024**3,
            max_image_bytes=4 * 1024**2,
            max_total_image_bytes=2 * 1024**3,
            request_timeout_seconds=30,
        ),
        thumbnail_size=256,
        bucket_bits=4,
        media_policy="primary",
        selection_policy="prefix",
    )
    publish_plan(
        plan=calibration_plan,
        bucket=bucket,
        prefix=f"{prefix}/calibrations",
        endpoint_url=endpoint_url,
    )
    embedder = MobileClip2S0Embedder(device=device)
    _write_state(
        state_path,
        phase="calibration_embed",
        calibration_release_id=calibration.release_id,
    )
    _embed_all(
        plan=calibration_plan,
        output=calibration_output,
        manifest_buckets=calibration.buckets,
        embedder=embedder,
        remote_bucket=bucket,
        remote_prefix=f"{prefix}/calibrations",
        endpoint_url=endpoint_url,
        batch_size=batch_size,
        download_concurrency=download_concurrency,
    )
    calibration_status = release_status(
        plan=calibration_plan, output=calibration_output
    )
    minimum_embedded = max(1, int(calibration.records * 0.9))
    if (
        not calibration_status["complete"]
        or calibration_status["embedded_records"] < minimum_embedded
        or calibration_status["images_per_second"] < 5
    ):
        raise RuntimeError(
            "cloud calibration rejected: "
            f"complete={calibration_status['complete']}, "
            f"embedded={calibration_status['embedded_records']}/{calibration.records}, "
            f"images_per_second={calibration_status['images_per_second']:.2f}"
        )

    _write_state(
        state_path,
        phase="full_source_discovery",
        calibration_release_id=calibration.release_id,
        calibration_status=calibration_status,
    )
    sources = discover_source_objects(
        [SMITHSONIAN_INDEX],
        max_bytes=16 * 1024**2,
        timeout_seconds=30,
    )
    _write_state(
        state_path,
        phase="full_plan",
        calibration_release_id=calibration.release_id,
        calibration_status=calibration_status,
    )
    plan = work / "plan"
    output = work / "release"
    manifest = build_plan(
        output=plan,
        sources=sources,
        budgets=BuildBudgets(
            max_records=max_records,
            max_source_bytes=64 * 1024**3,
            max_image_bytes=4 * 1024**2,
            max_total_image_bytes=256 * 1024**3,
            request_timeout_seconds=30,
        ),
        thumbnail_size=256,
        bucket_bits=8,
        media_policy="primary",
        selection_policy="bottom-k",
    )
    publish_plan(
        plan=plan,
        bucket=bucket,
        prefix=prefix,
        endpoint_url=endpoint_url,
    )
    _write_state(
        state_path,
        phase="full_embed",
        calibration_release_id=calibration.release_id,
        calibration_status=calibration_status,
        release_id=manifest.release_id,
    )
    _embed_all(
        plan=plan,
        output=output,
        manifest_buckets=manifest.buckets,
        embedder=embedder,
        remote_bucket=bucket,
        remote_prefix=prefix,
        endpoint_url=endpoint_url,
        batch_size=batch_size,
        download_concurrency=download_concurrency,
    )
    _write_state(state_path, phase="finalize", release_id=manifest.release_id)
    release = finalize_release(plan=plan, output=output)
    published = publish_release(
        output=output,
        bucket=bucket,
        prefix=prefix,
        endpoint_url=endpoint_url,
    )
    result = {
        "release_id": manifest.release_id,
        "calibration_status": calibration_status,
        "release_status": release["status"],
        "published": published,
    }
    _write_state(state_path, phase="complete", **result)
    return result


def _embed_all(
    *,
    plan: Path,
    output: Path,
    manifest_buckets: dict[str, int],
    embedder: MobileClip2S0Embedder,
    remote_bucket: str,
    remote_prefix: str,
    endpoint_url: str | None,
    batch_size: int,
    download_concurrency: int,
) -> None:
    for bucket_name in sorted(manifest_buckets):
        receipt = run_bucket(
            plan=plan,
            output=output,
            bucket=bucket_name,
            embedder=embedder,
            batch_size=batch_size,
            download_concurrency=download_concurrency,
        )
        publish_bucket(
            plan=plan,
            output=output,
            bucket_name=bucket_name,
            bucket=remote_bucket,
            prefix=remote_prefix,
            endpoint_url=endpoint_url,
        )
        if receipt.embedded_records + receipt.failed_records != receipt.planned_records:
            raise RuntimeError(f"bucket {bucket_name} is not fully accounted for")


def _write_state(path: Path, *, phase: str, **values: Any) -> None:
    write_json_atomic(
        path,
        {
            "schema_version": 1,
            "job_id": os.environ.get("SEMANTIC_MUSEUM_JOB_ID"),
            "phase": phase,
            **values,
        },
    )
