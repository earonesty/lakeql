from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from PIL import Image

from semantic_museum import storage
from semantic_museum.embedders import DeterministicImageEmbedder
from semantic_museum.models import BuildBudgets
from semantic_museum.planner import build_plan
from semantic_museum.release import finalize_release, release_status
from semantic_museum.smithsonian import SourceBudgetExceeded
from semantic_museum.storage import (
    publish_bucket,
    publish_plan,
    restore_bucket,
    upload_diagnostic_bundle,
)
from semantic_museum.worker import run_bucket


def test_r2_client_uses_explicit_auto_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arguments: dict[str, Any] = {}

    def capture_client(service: str, **kwargs: Any) -> object:
        arguments["service"] = service
        arguments.update(kwargs)
        return object()

    monkeypatch.setattr(storage.boto3, "client", capture_client)
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://account.r2.cloudflarestorage.com")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    monkeypatch.delenv("R2_REGION", raising=False)

    storage._client(None)

    assert arguments["service"] == "s3"
    assert arguments["region_name"] == "auto"


def test_diagnostic_bundle_is_committed_with_manifest_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    diagnostics = tmp_path / "diagnostics"
    diagnostics.mkdir()
    (diagnostics / "error.json").write_text('{"error":"boom"}\n')
    (diagnostics / "stderr.log").write_text("traceback\n")
    remote = _FakeObjectStore()
    monkeypatch.setattr(storage, "_client", lambda _endpoint: remote)

    result = upload_diagnostic_bundle(
        directory=diagnostics,
        bucket="museum",
        prefix="semantic-museum/leases/job/diagnostics",
        endpoint_url=None,
    )

    assert result["objects"] == 2
    assert result["bytes"] == 27
    manifest_key = result["manifest_key"]
    manifest = json.loads(remote.objects[("museum", manifest_key)][0])
    assert [item["path"] for item in manifest["objects"]] == [
        "error.json",
        "stderr.log",
    ]


def test_bounded_release_is_reproducible_and_resumable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.jsonl"
    images = tmp_path / "images"
    images.mkdir()
    rows = []
    for index in range(6):
        image = images / f"{index}.jpg"
        Image.new("RGB", (32, 24), (index * 20, 80, 160)).save(image)
        rows.append(_smithsonian_record(index, image))
    source.write_text("".join(json.dumps(row) + "\n" for row in rows))
    plan = tmp_path / "plan"
    budgets = BuildBudgets(
        max_records=4,
        max_source_bytes=1024 * 1024,
        max_image_bytes=64 * 1024,
        max_total_image_bytes=256 * 1024,
        request_timeout_seconds=2,
    )
    progress: list[tuple[str, dict[str, Any]]] = []

    first = build_plan(
        output=plan,
        sources=[str(source)],
        budgets=budgets,
        thumbnail_size=256,
        bucket_bits=4,
        media_policy="primary",
        model_id=DeterministicImageEmbedder.model_id,
        preprocessing_id=DeterministicImageEmbedder.preprocessing_id,
        progress=lambda event, values: progress.append((event, values)),
    )
    second = build_plan(
        output=plan,
        sources=[str(source)],
        budgets=budgets,
        thumbnail_size=256,
        bucket_bits=4,
        media_policy="primary",
        model_id=DeterministicImageEmbedder.model_id,
        preprocessing_id=DeterministicImageEmbedder.preprocessing_id,
    )
    assert first.release_id == second.release_id
    assert first.records == 4
    assert [event for event, _values in progress] == [
        "planner_started",
        "planner_source_started",
        "planner_source_completed",
        "planner_selection_completed",
        "planner_published",
    ]
    assert progress[-1][1]["records"] == 4

    output = tmp_path / "release"
    embedder = DeterministicImageEmbedder()
    for bucket in first.buckets:
        receipt = run_bucket(
            plan=plan,
            output=output,
            bucket=bucket,
            embedder=embedder,
            batch_size=2,
            download_concurrency=2,
        )
        resumed = run_bucket(
            plan=plan,
            output=output,
            bucket=bucket,
            embedder=embedder,
            batch_size=2,
            download_concurrency=2,
        )
        assert resumed == receipt
        assert receipt.embedded_records == receipt.planned_records

    status = release_status(plan=plan, output=output)
    assert status["complete"]
    assert status["embedded_records"] == 4

    remote = _FakeObjectStore()
    monkeypatch.setattr(storage, "_client", lambda _endpoint: remote)
    published_plan = publish_plan(
        plan=plan,
        bucket="museum",
        prefix="semantic-museum",
        endpoint_url=None,
    )
    assert published_plan["uploaded_objects"] == len(first.buckets) + 1
    assert (
        "museum",
        f"semantic-museum/plans/{first.release_id}/index.json",
    ) in remote.objects
    for bucket in first.buckets:
        publish_bucket(
            plan=plan,
            output=output,
            bucket_name=bucket,
            bucket="museum",
            prefix="semantic-museum",
            endpoint_url=None,
        )
    restored = tmp_path / "restored"
    for bucket in first.buckets:
        assert restore_bucket(
            plan=plan,
            output=restored,
            bucket_name=bucket,
            bucket="museum",
            prefix="semantic-museum",
            endpoint_url=None,
        )
    assert release_status(plan=plan, output=restored)["complete"]

    release = finalize_release(plan=plan, output=output)
    assert release["release_id"] == first.release_id
    assert sum(part["rows"] for part in release["metadata_files"]) == 4
    assert (
        sum(
            pq.ParquetFile(output / part["path"]).metadata.num_rows
            for part in release["metadata_files"]
        )
        == 4
    )


def test_failure_rows_account_for_unreadable_images(tmp_path: Path) -> None:
    missing = tmp_path / "missing.jpg"
    source = tmp_path / "source.jsonl"
    source.write_text(json.dumps(_smithsonian_record(1, missing)) + "\n")
    plan = tmp_path / "plan"
    manifest = build_plan(
        output=plan,
        sources=[str(source)],
        budgets=BuildBudgets(
            max_records=1,
            max_source_bytes=1024 * 1024,
            max_image_bytes=1024,
            max_total_image_bytes=1024,
            request_timeout_seconds=2,
        ),
        thumbnail_size=256,
        bucket_bits=4,
        media_policy="primary",
        model_id=DeterministicImageEmbedder.model_id,
        preprocessing_id=DeterministicImageEmbedder.preprocessing_id,
    )
    receipt = run_bucket(
        plan=plan,
        output=tmp_path / "release",
        bucket=next(iter(manifest.buckets)),
        embedder=DeterministicImageEmbedder(),
        batch_size=1,
        download_concurrency=1,
    )
    assert receipt.embedded_records == 0
    assert receipt.failed_records == 1


def test_source_byte_budget_is_global_and_persisted(tmp_path: Path) -> None:
    image = tmp_path / "image.jpg"
    Image.new("RGB", (8, 8), (10, 20, 30)).save(image)
    sources = []
    for index in range(2):
        source = tmp_path / f"source-{index}.jsonl"
        source.write_text(json.dumps(_smithsonian_record(index, image)) + "\n")
        sources.append(str(source))
    first_size = Path(sources[0]).stat().st_size
    plan = tmp_path / "plan"
    budgets = BuildBudgets(
        max_records=2,
        max_source_bytes=first_size + 1,
        max_image_bytes=1024,
        max_total_image_bytes=2048,
        request_timeout_seconds=1,
    )

    with pytest.raises(SourceBudgetExceeded, match="global source byte budget"):
        build_plan(
            output=plan,
            sources=sources,
            budgets=budgets,
            thumbnail_size=256,
            bucket_bits=4,
            media_policy="primary",
            selection_policy="bottom-k",
            model_id=DeterministicImageEmbedder.model_id,
            preprocessing_id=DeterministicImageEmbedder.preprocessing_id,
        )

    with pytest.raises(SourceBudgetExceeded, match="global source byte budget"):
        build_plan(
            output=plan,
            sources=sources,
            budgets=budgets,
            thumbnail_size=256,
            bucket_bits=4,
            media_policy="primary",
            selection_policy="bottom-k",
            model_id=DeterministicImageEmbedder.model_id,
            preprocessing_id=DeterministicImageEmbedder.preprocessing_id,
        )


def test_bottom_k_selection_is_independent_of_source_order(tmp_path: Path) -> None:
    image = tmp_path / "image.jpg"
    Image.new("RGB", (8, 8), (10, 20, 30)).save(image)
    sources = []
    for group in range(2):
        source = tmp_path / f"{group}.jsonl"
        source.write_text(
            "".join(
                json.dumps(_smithsonian_record(group * 5 + index, image)) + "\n"
                for index in range(5)
            )
        )
        sources.append(str(source))
    budgets = BuildBudgets(
        max_records=4,
        max_source_bytes=1024 * 1024,
        max_image_bytes=1024,
        max_total_image_bytes=4096,
        request_timeout_seconds=1,
    )
    manifests = [
        build_plan(
            output=tmp_path / f"plan-{order}",
            sources=ordered,
            budgets=budgets,
            thumbnail_size=256,
            bucket_bits=0,
            media_policy="primary",
            selection_policy="bottom-k",
        )
        for order, ordered in enumerate((sources, list(reversed(sources))))
    ]
    assert manifests[0].release_id == manifests[1].release_id
    assert manifests[0].records == 4


def _smithsonian_record(index: int, image: Path) -> dict[str, object]:
    return {
        "url": f"edanmdm:test_{index}",
        "unitCode": "TEST",
        "title": f"Object {index}",
        "hash": f"hash-{index}",
        "lastTimeUpdated": 20260723,
        "content": {
            "descriptiveNonRepeating": {
                "record_ID": f"test_{index}",
                "record_link": f"https://example.test/{index}",
                "metadata_usage": {"access": "CC0"},
                "title": {"content": f"Object {index}"},
                "online_media": {
                    "media": [
                        {
                            "type": "Images",
                            "idsId": f"media-{index}",
                            "content": image.as_uri(),
                            "usage": {"access": "CC0"},
                        }
                    ]
                },
            },
            "freetext": {
                "name": [{"content": "Ada Artist"}],
                "date": [{"content": "1900"}],
                "description": [{"content": "A test historical image."}],
            },
            "indexedStructured": {
                "object_type": ["Photographs"],
                "topic": ["History"],
                "place": ["Washington"],
            },
        },
    }


class _FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        try:
            body, metadata = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            ) from error
        return {"ContentLength": len(body), "Metadata": metadata}

    def upload_fileobj(
        self,
        stream: Any,
        bucket: str,
        key: str,
        *,
        ExtraArgs: dict[str, Any],
    ) -> None:
        self.objects[(bucket, key)] = (
            stream.read(),
            dict(ExtraArgs["Metadata"]),
        )

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        Metadata: dict[str, str],
        **_kwargs: Any,
    ) -> None:
        self.objects[(Bucket, Key)] = (Body, dict(Metadata))

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        try:
            body, _metadata = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "GetObject",
            ) from error
        return {"Body": BytesIO(body)}

    def download_fileobj(self, bucket: str, key: str, stream: Any) -> None:
        stream.write(self.objects[(bucket, key)][0])
