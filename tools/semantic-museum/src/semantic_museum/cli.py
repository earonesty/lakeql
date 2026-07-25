from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import find_dotenv, load_dotenv

from .diagnostics import DiagnosticRecorder
from .discovery import discover_source_objects
from .embedders import DeterministicImageEmbedder, MobileClip2S0Embedder
from .models import BuildBudgets
from .planner import build_plan
from .release import finalize_release, release_status
from .storage import (
    publish_bucket,
    publish_release,
    restore_bucket,
    upload_diagnostic_bundle,
    upload_terminal_receipt,
)
from .worker import run_bucket


def main(argv: list[str] | None = None) -> int:
    environment_path = find_dotenv(usecwd=True)
    if environment_path:
        load_dotenv(environment_path, override=False)
    parser = _parser()
    args = parser.parse_args(argv)
    recorder = _diagnostic_recorder(args)
    args.diagnostic_recorder = recorder
    if recorder is not None:
        recorder.event("command_started")
    try:
        result = args.handler(args)
    except (Exception, KeyboardInterrupt) as error:
        if recorder is not None:
            try:
                recorder.record_exception(error)
                _capture_failure_artifacts(args, recorder)
                _upload_failure_artifacts(args, recorder)
            except BaseException as diagnostic_error:
                print(
                    "semantic-museum diagnostic capture failed: "
                    f"{type(diagnostic_error).__name__}: {diagnostic_error}",
                    file=sys.stderr,
                )
        raise
    if recorder is not None:
        recorder.event("command_completed")
    if result is not None:
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="semantic-museum",
        description="Build reproducible historical-image embedding releases.",
    )
    parser.add_argument(
        "--diagnostics-dir",
        type=Path,
        default=(
            Path(value) if (value := os.environ.get("SEMANTIC_MUSEUM_DIAGNOSTICS_DIR")) else None
        ),
        help="durable structured event and exception output directory",
    )
    commands = parser.add_subparsers(required=True, dest="command_name")

    plan = commands.add_parser("plan", help="normalize source JSONL into immutable buckets")
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--source", action="append", default=[])
    plan.add_argument(
        "--source-index",
        action="append",
        default=[],
        help="newline URL index; nested Smithsonian unit indexes are resolved",
    )
    plan.add_argument("--max-source-index-bytes", type=_positive_int, default=16 * 1024**2)
    plan.add_argument("--max-records", type=_positive_int, required=True)
    plan.add_argument("--max-source-bytes", type=_positive_int, default=64 * 1024**3)
    plan.add_argument("--max-image-bytes", type=_positive_int, default=4 * 1024**2)
    plan.add_argument("--max-total-image-bytes", type=_positive_int, required=True)
    plan.add_argument("--request-timeout-seconds", type=float, default=30)
    plan.add_argument("--thumbnail-size", type=_positive_int, default=256)
    plan.add_argument("--bucket-bits", type=int, default=8)
    plan.add_argument(
        "--selection-policy",
        choices=("bottom-k", "prefix"),
        default="bottom-k",
    )
    plan.add_argument("--media-policy", choices=("primary", "all"), default="primary")
    plan.set_defaults(handler=_plan)

    embed = commands.add_parser("embed", help="process one deterministic bucket")
    embed.add_argument("--plan", type=Path, required=True)
    embed.add_argument("--output", type=Path, required=True)
    selection = embed.add_mutually_exclusive_group(required=True)
    selection.add_argument("--bucket", action="append")
    selection.add_argument(
        "--all",
        action="store_true",
        help="process every bucket sequentially with one model load",
    )
    embed.add_argument(
        "--embedder",
        choices=("mobileclip2-s0", "deterministic"),
        default="mobileclip2-s0",
    )
    embed.add_argument("--device", choices=("auto", "cuda", "cpu"), default="auto")
    embed.add_argument("--checkpoint", type=Path)
    embed.add_argument("--batch-size", type=_positive_int, default=32)
    embed.add_argument("--download-concurrency", type=_positive_int, default=16)
    embed.add_argument("--remote-bucket", default=os.environ.get("R2_BUCKET"))
    embed.add_argument("--remote-prefix", default="semantic-museum")
    embed.add_argument("--endpoint-url")
    embed.add_argument("--force", action="store_true")
    embed.set_defaults(handler=_embed)

    status = commands.add_parser("status", help="validate receipts and report progress")
    _release_paths(status)
    status.set_defaults(handler=_status)

    finalize = commands.add_parser(
        "finalize", help="write canonical metadata and a validated release manifest"
    )
    _release_paths(finalize)
    finalize.set_defaults(handler=_finalize)

    publish = commands.add_parser(
        "publish", help="upload a finalized immutable release and replace current.json"
    )
    publish.add_argument("--output", type=Path, required=True)
    publish.add_argument(
        "--bucket",
        default=os.environ.get("R2_BUCKET"),
        required=os.environ.get("R2_BUCKET") is None,
    )
    publish.add_argument("--prefix", default="semantic-museum")
    publish.add_argument("--endpoint-url")
    publish.set_defaults(handler=_publish)

    cloud_job = commands.add_parser(
        "cloud-job",
        help="calibrate, build, checkpoint, finalize, and publish a full release",
    )
    cloud_job.add_argument("--work", type=Path, required=True)
    cloud_job.add_argument(
        "--bucket",
        default=os.environ.get("R2_BUCKET"),
        required=os.environ.get("R2_BUCKET") is None,
    )
    cloud_job.add_argument("--prefix", default="semantic-museum")
    cloud_job.add_argument("--endpoint-url")
    cloud_job.add_argument("--device", choices=("cuda",), default="cuda")
    cloud_job.add_argument("--batch-size", type=_positive_int, default=32)
    cloud_job.add_argument("--download-concurrency", type=_positive_int, default=32)
    cloud_job.add_argument("--calibration-records", type=_positive_int, default=1000)
    cloud_job.add_argument("--max-records", type=_positive_int, default=1_000_000)
    cloud_job.set_defaults(handler=_cloud_job)

    supervise = commands.add_parser(
        "supervise",
        help="run a worker under a hard deadline and destroy its GPU resource",
    )
    supervise.add_argument("--provider", choices=("auto", "runpod", "vast", "none"), default="auto")
    supervise.add_argument("--max-runtime-seconds", type=_positive_int, required=True)
    supervise.add_argument("--checkpoint-grace-seconds", type=_positive_int, default=30)
    supervise.add_argument("--receipt", type=Path, required=True)
    supervise.add_argument(
        "--diagnostics-dir",
        type=Path,
        dest="supervisor_diagnostics_dir",
    )
    supervise.add_argument("--receipt-remote-bucket", default=os.environ.get("R2_BUCKET"))
    supervise.add_argument("--receipt-remote-key")
    supervise.add_argument("--diagnostics-remote-prefix")
    supervise.add_argument("--endpoint-url")
    supervise.add_argument("command", nargs=argparse.REMAINDER)
    supervise.set_defaults(handler=_supervise)
    return parser


def _release_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)


def _plan(args: argparse.Namespace) -> dict[str, Any]:
    if args.bucket_bits != 0 and (
        args.bucket_bits < 4 or args.bucket_bits > 24 or args.bucket_bits % 4
    ):
        raise ValueError("bucket-bits must be zero or a multiple of four between 4 and 24")
    sources = list(args.source)
    if args.source_index:
        sources.extend(
            discover_source_objects(
                args.source_index,
                max_bytes=args.max_source_index_bytes,
                timeout_seconds=args.request_timeout_seconds,
            )
        )
    sources = sorted(set(sources))
    if not sources:
        raise ValueError("at least one --source or --source-index is required")
    manifest = build_plan(
        output=args.output,
        sources=sources,
        budgets=BuildBudgets(
            max_records=args.max_records,
            max_source_bytes=args.max_source_bytes,
            max_image_bytes=args.max_image_bytes,
            max_total_image_bytes=args.max_total_image_bytes,
            request_timeout_seconds=args.request_timeout_seconds,
        ),
        thumbnail_size=args.thumbnail_size,
        bucket_bits=args.bucket_bits,
        media_policy=args.media_policy,
        selection_policy=args.selection_policy,
        progress=(
            (lambda event, values: args.diagnostic_recorder.event(event, **values))
            if args.diagnostic_recorder is not None
            else None
        ),
    )
    return manifest.to_dict()


def _embed(args: argparse.Namespace) -> dict[str, Any]:
    _require_cloud_checkpointing(args.remote_bucket)
    embedder = (
        DeterministicImageEmbedder()
        if args.embedder == "deterministic"
        else MobileClip2S0Embedder(device=args.device, checkpoint=args.checkpoint)
    )
    if args.all:
        from .jsonio import read_json
        from .models import BuildManifest

        manifest = BuildManifest.from_dict(read_json(args.plan / "manifest.json"))
        buckets = sorted(manifest.buckets)
    else:
        buckets = args.bucket
    receipts = []
    for bucket in buckets:
        if args.remote_bucket and not args.force:
            restore_bucket(
                plan=args.plan,
                output=args.output,
                bucket_name=bucket,
                bucket=args.remote_bucket,
                prefix=args.remote_prefix,
                endpoint_url=args.endpoint_url,
            )
        receipt = run_bucket(
            plan=args.plan,
            output=args.output,
            bucket=bucket,
            embedder=embedder,
            batch_size=args.batch_size,
            download_concurrency=args.download_concurrency,
            force=args.force,
        )
        if args.remote_bucket:
            publish_bucket(
                plan=args.plan,
                output=args.output,
                bucket_name=bucket,
                bucket=args.remote_bucket,
                prefix=args.remote_prefix,
                endpoint_url=args.endpoint_url,
            )
        receipts.append(receipt.to_dict())
    return {
        "buckets": len(receipts),
        "embedded_records": sum(receipt["embedded_records"] for receipt in receipts),
        "failed_records": sum(receipt["failed_records"] for receipt in receipts),
        "elapsed_seconds": sum(receipt["elapsed_seconds"] for receipt in receipts),
        "receipts": receipts,
    }


def _status(args: argparse.Namespace) -> dict[str, Any]:
    return release_status(plan=args.plan, output=args.output)


def _finalize(args: argparse.Namespace) -> dict[str, Any]:
    return finalize_release(plan=args.plan, output=args.output)


def _publish(args: argparse.Namespace) -> dict[str, Any]:
    return publish_release(
        output=args.output,
        bucket=args.bucket,
        prefix=args.prefix,
        endpoint_url=args.endpoint_url,
    )


def _cloud_job(args: argparse.Namespace) -> dict[str, Any]:
    from .cloud_job import run_cloud_job

    return run_cloud_job(
        work=args.work,
        bucket=args.bucket,
        prefix=args.prefix,
        endpoint_url=args.endpoint_url,
        device=args.device,
        batch_size=args.batch_size,
        download_concurrency=args.download_concurrency,
        calibration_records=args.calibration_records,
        max_records=args.max_records,
        diagnostics=args.diagnostic_recorder,
    )


def _supervise(args: argparse.Namespace) -> None:
    from .lifecycle import run_supervised

    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    cloud_provider = args.provider in {"runpod", "vast"} or (
        args.provider == "auto"
        and bool(os.environ.get("RUNPOD_POD_ID") or os.environ.get("CONTAINER_ID"))
    )
    if cloud_provider and not args.receipt_remote_bucket:
        raise RuntimeError("cloud supervision requires a remote terminal-receipt bucket")
    if bool(args.receipt_remote_bucket) != bool(args.receipt_remote_key):
        raise ValueError("receipt-remote-bucket and receipt-remote-key must be provided together")
    diagnostics_dir = (
        args.supervisor_diagnostics_dir
        or args.diagnostics_dir
        or args.receipt.parent / "diagnostics"
    )
    diagnostics_remote_prefix = args.diagnostics_remote_prefix
    if args.receipt_remote_key and diagnostics_remote_prefix is None:
        receipt_parent = args.receipt_remote_key.rpartition("/")[0]
        diagnostics_remote_prefix = (
            f"{receipt_parent}/diagnostics" if receipt_parent else "diagnostics"
        )
    if bool(args.receipt_remote_bucket) != bool(diagnostics_remote_prefix):
        raise ValueError("remote receipt bucket and diagnostics prefix must be provided together")
    os.environ["SEMANTIC_MUSEUM_DIAGNOSTICS_DIR"] = str(diagnostics_dir)
    os.environ["SEMANTIC_MUSEUM_SUPERVISED"] = "1"
    if args.receipt_remote_bucket and diagnostics_remote_prefix is not None:
        os.environ["SEMANTIC_MUSEUM_DIAGNOSTICS_REMOTE_BUCKET"] = args.receipt_remote_bucket
        os.environ["SEMANTIC_MUSEUM_DIAGNOSTICS_REMOTE_PREFIX"] = diagnostics_remote_prefix
        if args.endpoint_url:
            os.environ["SEMANTIC_MUSEUM_DIAGNOSTICS_ENDPOINT_URL"] = args.endpoint_url
    if args.receipt_remote_bucket:

        def uploader(path: Path) -> None:
            upload_terminal_receipt(
                path=path,
                bucket=args.receipt_remote_bucket,
                key=args.receipt_remote_key,
                endpoint_url=args.endpoint_url,
            )
    else:
        uploader = None
    if args.receipt_remote_bucket:

        def diagnostics_uploader(path: Path) -> dict[str, Any]:
            return upload_diagnostic_bundle(
                directory=path,
                bucket=args.receipt_remote_bucket,
                prefix=diagnostics_remote_prefix,
                endpoint_url=args.endpoint_url,
            )
    else:
        diagnostics_uploader = None
    exit_code = run_supervised(
        command=command,
        max_runtime_seconds=args.max_runtime_seconds,
        checkpoint_grace_seconds=args.checkpoint_grace_seconds,
        receipt_path=args.receipt,
        provider=args.provider,
        diagnostics_path=diagnostics_dir,
        upload_receipt=uploader,
        upload_diagnostics=diagnostics_uploader,
    )
    raise SystemExit(exit_code)


def _require_cloud_checkpointing(remote_bucket: str | None) -> None:
    provider_identity = os.environ.get("RUNPOD_POD_ID") or os.environ.get("CONTAINER_ID")
    if provider_identity and not remote_bucket:
        raise RuntimeError(
            "cloud embedding requires R2 bucket checkpointing; set R2_BUCKET "
            "or pass --remote-bucket"
        )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _diagnostic_recorder(args: argparse.Namespace) -> DiagnosticRecorder | None:
    directory = args.diagnostics_dir
    if directory is None and args.command_name == "cloud-job":
        directory = args.work / "diagnostics"
    if directory is None and hasattr(args, "output"):
        directory = args.output / "diagnostics"
    if directory is None:
        return None
    return DiagnosticRecorder(directory, command=args.command_name)


def _capture_failure_artifacts(args: argparse.Namespace, recorder: DiagnosticRecorder) -> None:
    if args.command_name != "cloud-job":
        return
    recorder.capture_artifact(args.work / "state.json", name="cloud-job-state.json")
    recorder.capture_artifact(
        args.work / "plan" / "plan-state.sqlite3",
        name="full-plan-state.sqlite3",
    )
    recorder.capture_artifact(
        args.work / "calibration-plan" / "plan-state.sqlite3",
        name="calibration-plan-state.sqlite3",
    )


def _upload_failure_artifacts(args: argparse.Namespace, recorder: DiagnosticRecorder) -> None:
    if args.command_name != "cloud-job" or os.environ.get("SEMANTIC_MUSEUM_SUPERVISED"):
        return
    bucket = os.environ.get("SEMANTIC_MUSEUM_DIAGNOSTICS_REMOTE_BUCKET") or args.bucket
    remote_prefix = os.environ.get("SEMANTIC_MUSEUM_DIAGNOSTICS_REMOTE_PREFIX")
    if remote_prefix is None:
        job_id = os.environ.get("SEMANTIC_MUSEUM_JOB_ID") or (
            f"job-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}"
        )
        remote_prefix = f"{args.prefix.strip('/')}/jobs/{job_id}/diagnostics"
    endpoint_url = os.environ.get("SEMANTIC_MUSEUM_DIAGNOSTICS_ENDPOINT_URL") or args.endpoint_url
    try:
        result = upload_diagnostic_bundle(
            directory=recorder.directory,
            bucket=bucket,
            prefix=remote_prefix,
            endpoint_url=endpoint_url,
        )
    except Exception as upload_error:
        recorder.event(
            "diagnostic_upload_failed",
            exception_type=(f"{type(upload_error).__module__}.{type(upload_error).__qualname__}"),
            exception_message=str(upload_error),
            remote_prefix=remote_prefix,
        )
    else:
        recorder.event(
            "diagnostic_upload_completed",
            remote_prefix=remote_prefix,
            **result,
        )


if __name__ == "__main__":
    raise SystemExit(main())
