from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from .models import MediaRecord

SMITHSONIAN_SOURCE = "smithsonian-open-access"


class SourceBudgetExceeded(RuntimeError):
    pass


def iter_source_records(
    source: str,
    *,
    max_bytes: int,
    timeout_seconds: float,
    consume_bytes: Callable[[int], None] | None = None,
) -> Iterator[dict[str, Any]]:
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout_seconds),
            headers={"User-Agent": "lakeql-semantic-museum/0.1"},
        ) as client, client.stream("GET", source) as response:
            response.raise_for_status()
            yield from _iter_json_lines(
                response.iter_bytes(),
                source,
                max_bytes,
                consume_bytes=consume_bytes,
            )
        return
    path = Path(source.removeprefix("file://"))
    with path.open("rb") as stream:
        yield from _iter_json_lines(
            _iter_file_chunks(stream),
            str(path),
            max_bytes,
            consume_bytes=consume_bytes,
        )


def _iter_file_chunks(stream: BinaryIO, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
    while chunk := stream.read(chunk_size):
        yield chunk


def _iter_json_lines(
    chunks: Iterator[bytes],
    source: str,
    max_bytes: int,
    *,
    consume_bytes: Callable[[int], None] | None = None,
) -> Iterator[dict[str, Any]]:
    total = 0
    pending = bytearray()
    line_number = 0
    for chunk in chunks:
        if consume_bytes is not None:
            consume_bytes(len(chunk))
        total += len(chunk)
        if total > max_bytes:
            raise SourceBudgetExceeded(
                f"{source}: source byte budget exceeded ({total} > {max_bytes})"
            )
        pending.extend(chunk)
        while True:
            newline = pending.find(b"\n")
            if newline < 0:
                if len(pending) > 64 * 1024 * 1024:
                    raise ValueError(f"{source}: JSONL row exceeds 64 MiB")
                break
            raw = bytes(pending[:newline])
            del pending[: newline + 1]
            line_number += 1
            if raw.strip():
                yield _parse_json_object(raw, source, line_number)
    if pending.strip():
        line_number += 1
        yield _parse_json_object(bytes(pending), source, line_number)


def _parse_json_object(raw: bytes, source: str, line_number: int) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{source}:{line_number}: invalid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{source}:{line_number}: row must be an object")
    return value


def normalize_smithsonian_record(
    record: dict[str, Any],
    *,
    thumbnail_size: int,
    media_policy: str,
) -> list[MediaRecord]:
    content = _object(record.get("content"))
    descriptive = _object(content.get("descriptiveNonRepeating"))
    metadata_usage = _object(descriptive.get("metadata_usage"))
    if _text(metadata_usage.get("access")).upper() != "CC0":
        return []
    online_media = _object(descriptive.get("online_media"))
    candidates = online_media.get("media")
    if not isinstance(candidates, list):
        return []

    item_id = _text(descriptive.get("record_ID")) or _text(record.get("url"))
    if not item_id:
        return []
    unit = _text(record.get("unitCode")) or _text(descriptive.get("unit_code"))
    freetext = _object(content.get("freetext"))
    indexed = _object(content.get("indexedStructured"))
    title_value = _object(descriptive.get("title"))
    output: list[MediaRecord] = []
    for candidate in candidates:
        media = _object(candidate)
        if _text(media.get("type")).lower() != "images":
            continue
        usage = _object(media.get("usage"))
        media_rights = _text(usage.get("access"))
        if media_rights.upper() != "CC0":
            continue
        media_id = _text(media.get("idsId")) or _text(media.get("id")) or _text(media.get("guid"))
        image_url = _text(media.get("content")) or _text(media.get("thumbnail"))
        if not media_id or not image_url:
            continue
        output.append(
            MediaRecord(
                item_id=item_id,
                media_id=media_id,
                source=SMITHSONIAN_SOURCE,
                unit=unit,
                title=_text(title_value.get("content")) or _text(record.get("title")),
                description=_join_freetext(freetext, ("description", "notes")),
                creators=_freetext_values(freetext, "name"),
                dates=_freetext_values(freetext, "date"),
                media=_freetext_values(freetext, "physicalDescription"),
                object_types=_strings(indexed.get("object_type")),
                subjects=_strings(indexed.get("topic")),
                places=_strings(indexed.get("place")),
                record_url=_text(descriptive.get("record_link")),
                image_url=_bounded_image_url(image_url, thumbnail_size),
                display_url=_bounded_image_url(image_url, max(512, thumbnail_size)),
                record_rights=_text(metadata_usage.get("access")),
                media_rights=media_rights,
                source_hash=_text(record.get("hash")) or _text(record.get("docSignature")),
                source_updated_at=_optional_int(record.get("lastTimeUpdated")),
            )
        )
        if media_policy == "primary":
            break
    return output


def _bounded_image_url(value: str, max_size: int) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return value
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["max"] = str(max_size)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _strings(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(text for item in value if (text := _text(item)))


def _freetext_values(freetext: dict[str, Any], key: str) -> tuple[str, ...]:
    values = freetext.get(key)
    if not isinstance(values, list):
        return ()
    output: list[str] = []
    for value in values:
        text = _text(_object(value).get("content"))
        if text:
            output.append(text)
    return tuple(output)


def _join_freetext(freetext: dict[str, Any], keys: tuple[str, ...]) -> str:
    values = [value for key in keys for value in _freetext_values(freetext, key)]
    return "\n".join(values)


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None
