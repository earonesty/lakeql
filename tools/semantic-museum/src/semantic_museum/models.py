from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class MediaRecord:
    item_id: str
    media_id: str
    source: str
    unit: str
    title: str
    description: str
    creators: tuple[str, ...]
    dates: tuple[str, ...]
    media: tuple[str, ...]
    object_types: tuple[str, ...]
    subjects: tuple[str, ...]
    places: tuple[str, ...]
    record_url: str
    image_url: str
    display_url: str
    record_rights: str
    media_rights: str
    source_hash: str
    source_updated_at: int | None

    def to_dict(self) -> dict[str, Any]:
        output = asdict(self)
        for name in ("creators", "dates", "media", "object_types", "subjects", "places"):
            output[name] = list(output[name])
        return output

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> MediaRecord:
        copied = dict(value)
        for name in ("creators", "dates", "media", "object_types", "subjects", "places"):
            copied[name] = tuple(copied.get(name, ()))
        return cls(**copied)


@dataclass(frozen=True)
class BuildBudgets:
    max_records: int
    max_source_bytes: int
    max_image_bytes: int
    max_total_image_bytes: int
    request_timeout_seconds: float


@dataclass(frozen=True)
class BuildManifest:
    schema_version: int
    release_id: str
    created_at: str
    source_name: str
    source_objects: tuple[str, ...]
    source_fingerprint: str
    selection_policy: Literal["bottom-k", "prefix"]
    media_policy: Literal["primary", "all"]
    thumbnail_size: int
    bucket_bits: int
    records: int
    buckets: dict[str, int]
    budgets: BuildBudgets
    model_id: str
    preprocessing_id: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["source_objects"] = list(self.source_objects)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> BuildManifest:
        copied = dict(value)
        copied["source_objects"] = tuple(copied["source_objects"])
        copied["budgets"] = BuildBudgets(**copied["budgets"])
        return cls(**copied)


@dataclass(frozen=True)
class FailureRecord:
    media_id: str
    stage: Literal["download", "decode", "inference", "validation", "write"]
    retryable: bool
    error_type: str
    message: str
    attempts: int


@dataclass(frozen=True)
class ShardReceipt:
    schema_version: int
    release_id: str
    bucket: str
    input_sha256: str
    output_sha256: str
    failures_sha256: str
    model_id: str
    model_fingerprint: str
    preprocessing_id: str
    device: str
    started_at: str
    completed_at: str
    elapsed_seconds: float
    planned_records: int
    embedded_records: int
    failed_records: int
    downloaded_bytes: int
    batches: int
    images_per_second: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
