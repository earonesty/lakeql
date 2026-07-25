from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from .hashing import bucket_for, canonical_json_bytes, fingerprint_strings
from .jsonio import write_json_atomic, write_jsonl_atomic
from .models import BuildBudgets, BuildManifest, MediaRecord
from .smithsonian import (
    SMITHSONIAN_SOURCE,
    SourceBudgetExceeded,
    iter_source_records,
    normalize_smithsonian_record,
)

SCHEMA_VERSION = 1
DEFAULT_MODEL_ID = "MobileCLIP2-S0:apple/MobileCLIP2-S0:mobileclip2_s0.pt"
DEFAULT_PREPROCESSING_ID = (
    "open_clip-3.3.0:MobileCLIP2-S0:resize-shortest-bicubic:center-crop-256:rgb:mean-0:std-1"
)
PlannerProgress = Callable[[str, dict[str, Any]], None]
PROGRESS_BYTE_INTERVAL = 256 * 1024**2
PROGRESS_TIME_INTERVAL_SECONDS = 60


def build_plan(
    *,
    output: Path,
    sources: list[str],
    budgets: BuildBudgets,
    thumbnail_size: int,
    bucket_bits: int,
    media_policy: str,
    selection_policy: str = "bottom-k",
    model_id: str = DEFAULT_MODEL_ID,
    preprocessing_id: str = DEFAULT_PREPROCESSING_ID,
    progress: PlannerProgress | None = None,
) -> BuildManifest:
    if not sources:
        raise ValueError("at least one source object is required")
    sources = sorted(set(sources))
    if thumbnail_size <= 0 or thumbnail_size > 2048:
        raise ValueError("thumbnail_size must be between 1 and 2048")
    if media_policy not in {"primary", "all"}:
        raise ValueError("media_policy must be primary or all")
    if selection_policy not in {"bottom-k", "prefix"}:
        raise ValueError("selection_policy must be bottom-k or prefix")
    if (
        budgets.max_records <= 0
        or budgets.max_source_bytes <= 0
        or budgets.max_image_bytes <= 0
        or budgets.max_total_image_bytes <= 0
        or budgets.request_timeout_seconds <= 0
    ):
        raise ValueError("all planner budgets must be positive")
    output.mkdir(parents=True, exist_ok=True)
    state_path = output / "plan-state.sqlite3"
    connection = sqlite3.connect(state_path)
    try:
        _initialize_state(connection)
        _bind_state_config(
            connection,
            canonical_json_bytes(
                {
                    "schema_version": SCHEMA_VERSION,
                    "sources": sources,
                    "max_records": budgets.max_records,
                    "thumbnail_size": thumbnail_size,
                    "bucket_bits": bucket_bits,
                    "media_policy": media_policy,
                    "selection_policy": selection_policy,
                }
            ),
        )
        accepted = _completed_record_count(connection)
        consumed_source_bytes = _completed_source_bytes(connection)
        _report(
            progress,
            "planner_started",
            source_objects=len(sources),
            completed_source_objects=_completed_source_count(connection),
            accepted_records=accepted,
            consumed_source_bytes=consumed_source_bytes,
            selection_policy=selection_policy,
        )
        budget_reached = selection_policy == "prefix" and accepted >= budgets.max_records
        for source_index, source in enumerate(sources):
            if budget_reached:
                break
            if _source_complete(connection, source):
                continue
            current_source_bytes = 0
            last_reported_bytes = consumed_source_bytes
            last_reported_at = time.monotonic()
            source_identity = _source_identity(source)
            _report(
                progress,
                "planner_source_started",
                source_index=source_index,
                source_objects=len(sources),
                **source_identity,
            )

            def consume_source_bytes(
                size: int,
                _source_index: int = source_index,
                _source_identity: dict[str, Any] = source_identity,
            ) -> None:
                nonlocal consumed_source_bytes, current_source_bytes
                nonlocal last_reported_at, last_reported_bytes
                consumed_source_bytes += size
                current_source_bytes += size
                if consumed_source_bytes > budgets.max_source_bytes:
                    raise SourceBudgetExceeded(
                        "global source byte budget exceeded "
                        f"({consumed_source_bytes} > {budgets.max_source_bytes})"
                    )
                now = time.monotonic()
                if (
                    consumed_source_bytes - last_reported_bytes >= PROGRESS_BYTE_INTERVAL
                    or now - last_reported_at >= PROGRESS_TIME_INTERVAL_SECONDS
                ):
                    _report(
                        progress,
                        "planner_progress",
                        source_index=_source_index,
                        source_objects=len(sources),
                        accepted_records=_completed_record_count(connection),
                        consumed_source_bytes=consumed_source_bytes,
                        current_source_bytes=current_source_bytes,
                        **_source_identity,
                    )
                    last_reported_bytes = consumed_source_bytes
                    last_reported_at = now

            for raw in iter_source_records(
                source,
                max_bytes=budgets.max_source_bytes,
                timeout_seconds=budgets.request_timeout_seconds,
                consume_bytes=consume_source_bytes,
            ):
                for record in normalize_smithsonian_record(
                    raw,
                    thumbnail_size=thumbnail_size,
                    media_policy=media_policy,
                ):
                    inserted = _insert_record(connection, record, bucket_bits)
                    if inserted:
                        accepted += 1
                        if selection_policy == "prefix" and accepted >= budgets.max_records:
                            budget_reached = True
                            break
                        if (
                            selection_policy == "bottom-k"
                            and accepted > budgets.max_records + 10_000
                        ):
                            _retain_bottom_k(connection, budgets.max_records)
                            accepted = _completed_record_count(connection)
                if budget_reached:
                    break
            if budget_reached:
                connection.commit()
                break
            connection.execute(
                "INSERT OR REPLACE INTO completed_sources(source, source_bytes) VALUES (?, ?)",
                (source, current_source_bytes),
            )
            connection.commit()
            _report(
                progress,
                "planner_source_completed",
                source_index=source_index,
                source_objects=len(sources),
                accepted_records=accepted,
                consumed_source_bytes=consumed_source_bytes,
                current_source_bytes=current_source_bytes,
                **source_identity,
            )
        if selection_policy == "bottom-k":
            _retain_bottom_k(connection, budgets.max_records)
            accepted = _completed_record_count(connection)
            _report(
                progress,
                "planner_selection_completed",
                accepted_records=accepted,
                consumed_source_bytes=consumed_source_bytes,
            )
        if _completed_record_count(connection) == 0:
            raise RuntimeError("source selection produced no eligible image records")
        manifest = _publish_plan(
            connection=connection,
            output=output,
            sources=sources,
            budgets=budgets,
            thumbnail_size=thumbnail_size,
            bucket_bits=bucket_bits,
            media_policy=media_policy,
            selection_policy=selection_policy,
            model_id=model_id,
            preprocessing_id=preprocessing_id,
        )
        _report(
            progress,
            "planner_published",
            release_id=manifest.release_id,
            records=manifest.records,
            buckets=len(manifest.buckets),
            consumed_source_bytes=consumed_source_bytes,
        )
        return manifest
    finally:
        connection.close()


def _initialize_state(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS records (
          media_id TEXT PRIMARY KEY,
          bucket TEXT NOT NULL,
          selection_rank TEXT NOT NULL,
          record_json BLOB NOT NULL
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS records_by_bucket ON records(bucket, media_id)")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS completed_sources "
        "(source TEXT PRIMARY KEY, source_bytes INTEGER NOT NULL DEFAULT 0)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS state_config (singleton INTEGER PRIMARY KEY, value BLOB)"
    )
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(records)")}
    if "selection_rank" not in columns:
        raise RuntimeError("plan state uses an incompatible schema; choose a new plan directory")
    completed_source_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(completed_sources)")
    }
    if "source_bytes" not in completed_source_columns:
        connection.execute(
            "ALTER TABLE completed_sources ADD COLUMN source_bytes INTEGER NOT NULL DEFAULT 0"
        )
    connection.commit()


def _bind_state_config(connection: sqlite3.Connection, value: bytes) -> None:
    row = connection.execute("SELECT value FROM state_config WHERE singleton = 1").fetchone()
    if row is not None and bytes(row[0]) != value:
        raise RuntimeError(
            "plan configuration differs from existing state; choose a new plan directory"
        )
    connection.execute(
        "INSERT OR IGNORE INTO state_config(singleton, value) VALUES (1, ?)",
        (value,),
    )
    connection.commit()


def _completed_record_count(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) FROM records").fetchone()
    return int(row[0]) if row else 0


def _source_complete(connection: sqlite3.Connection, source: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM completed_sources WHERE source = ?",
            (source,),
        ).fetchone()
        is not None
    )


def _completed_source_bytes(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COALESCE(SUM(source_bytes), 0) FROM completed_sources"
    ).fetchone()
    return int(row[0]) if row else 0


def _completed_source_count(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) FROM completed_sources").fetchone()
    return int(row[0]) if row else 0


def _source_identity(source: str) -> dict[str, Any]:
    from urllib.parse import urlparse

    parsed = urlparse(source)
    return {
        "source_name": Path(parsed.path).name,
        "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
    }


def _report(progress: PlannerProgress | None, event: str, **values: Any) -> None:
    if progress is not None:
        progress(event, values)


def _insert_record(
    connection: sqlite3.Connection,
    record: MediaRecord,
    bucket_bits: int,
) -> bool:
    cursor = connection.execute(
        "INSERT OR IGNORE INTO records"
        "(media_id, bucket, selection_rank, record_json) VALUES (?, ?, ?, ?)",
        (
            record.media_id,
            bucket_for(record.media_id, bucket_bits),
            hashlib.sha256(record.media_id.encode()).hexdigest(),
            canonical_json_bytes(record.to_dict()),
        ),
    )
    return cursor.rowcount > 0


def _retain_bottom_k(connection: sqlite3.Connection, limit: int) -> None:
    connection.execute(
        """
        DELETE FROM records
        WHERE media_id IN (
          SELECT media_id
          FROM records
          ORDER BY selection_rank DESC, media_id DESC
          LIMIT MAX((SELECT COUNT(*) FROM records) - ?, 0)
        )
        """,
        (limit,),
    )
    connection.commit()


def _publish_plan(
    *,
    connection: sqlite3.Connection,
    output: Path,
    sources: list[str],
    budgets: BuildBudgets,
    thumbnail_size: int,
    bucket_bits: int,
    media_policy: str,
    selection_policy: str,
    model_id: str,
    preprocessing_id: str,
) -> BuildManifest:
    digest = hashlib.sha256()
    bucket_counts: Counter[str] = Counter()
    current_bucket = ""
    current_rows: list[dict[str, Any]] = []

    def flush() -> None:
        if not current_rows:
            return
        write_jsonl_atomic(output / "buckets" / f"{current_bucket}.jsonl", current_rows)

    for bucket, raw in connection.execute(
        "SELECT bucket, record_json FROM records ORDER BY bucket, media_id"
    ):
        if current_bucket and bucket != current_bucket:
            flush()
            current_rows = []
        current_bucket = str(bucket)
        raw_bytes = bytes(raw)
        digest.update(len(raw_bytes).to_bytes(8, "big"))
        digest.update(raw_bytes)
        current_rows.append(json.loads(raw_bytes))
        bucket_counts[current_bucket] += 1
    flush()
    source_fingerprint = fingerprint_strings(sources)
    digest.update(source_fingerprint.encode())
    digest.update(model_id.encode())
    digest.update(preprocessing_id.encode())
    digest.update(
        canonical_json_bytes(
            {
                "thumbnail_size": thumbnail_size,
                "bucket_bits": bucket_bits,
                "media_policy": media_policy,
                "selection_policy": selection_policy,
            }
        )
    )
    release_id = digest.hexdigest()[:24]
    manifest = BuildManifest(
        schema_version=SCHEMA_VERSION,
        release_id=release_id,
        created_at=datetime.now(UTC).isoformat(),
        source_name=SMITHSONIAN_SOURCE,
        source_objects=tuple(sources),
        source_fingerprint=source_fingerprint,
        selection_policy=cast(Literal["bottom-k", "prefix"], selection_policy),
        media_policy=cast(Literal["primary", "all"], media_policy),
        thumbnail_size=thumbnail_size,
        bucket_bits=bucket_bits,
        records=sum(bucket_counts.values()),
        buckets=dict(sorted(bucket_counts.items())),
        budgets=budgets,
        model_id=model_id,
        preprocessing_id=preprocessing_id,
    )
    write_json_atomic(output / "manifest.json", manifest.to_dict())
    return manifest
