from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx

SMITHSONIAN_INDEX = (
    "https://smithsonian-open-access.s3-us-west-2.amazonaws.com/"
    "metadata/edan/index.txt"
)


def discover_source_objects(
    indexes: list[str],
    *,
    max_bytes: int,
    timeout_seconds: float,
    max_depth: int = 2,
) -> list[str]:
    if max_bytes <= 0:
        raise ValueError("index byte budget must be positive")
    pending = [(index, 0) for index in indexes]
    visited: set[str] = set()
    objects: set[str] = set()
    consumed = 0
    with httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(timeout_seconds),
        headers={"User-Agent": "lakeql-semantic-museum/0.1"},
    ) as client:
        while pending:
            index, depth = pending.pop(0)
            if index in visited:
                continue
            visited.add(index)
            raw = _read_index(client, index, max_bytes - consumed)
            consumed += len(raw)
            for entry in _index_entries(raw, index):
                if entry.endswith("/index.txt") or entry.endswith("index.txt"):
                    if depth >= max_depth:
                        raise RuntimeError(
                            f"source index nesting exceeds maximum depth at {entry}"
                        )
                    pending.append((entry, depth + 1))
                else:
                    objects.add(entry)
    if not objects:
        raise RuntimeError("source indexes contained no data objects")
    return sorted(objects)


def _read_index(client: httpx.Client, source: str, remaining: int) -> bytes:
    if remaining <= 0:
        raise RuntimeError("source index byte budget exceeded")
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        with client.stream("GET", source) as response:
            response.raise_for_status()
            output = bytearray()
            for chunk in response.iter_bytes():
                output.extend(chunk)
                if len(output) > remaining:
                    raise RuntimeError("source index byte budget exceeded")
            return bytes(output)
    path = Path(source.removeprefix("file://"))
    if path.stat().st_size > remaining:
        raise RuntimeError("source index byte budget exceeded")
    return path.read_bytes()


def _index_entries(raw: bytes, source: str) -> Iterator[str]:
    for line_number, raw_line in enumerate(raw.splitlines(), start=1):
        try:
            entry = raw_line.decode().strip()
        except UnicodeDecodeError as error:
            raise ValueError(f"{source}:{line_number}: invalid UTF-8") from error
        if not entry or entry.startswith("#"):
            continue
        parsed = urlparse(entry)
        if parsed.scheme not in {"http", "https", "file", ""}:
            raise ValueError(f"{source}:{line_number}: unsupported source URL")
        if parsed.scheme:
            yield entry
        elif urlparse(source).scheme in {"http", "https"}:
            yield urljoin(source, entry)
        else:
            resolved = (
                Path(source.removeprefix("file://")).parent / entry
            ).resolve()
            yield resolved.as_uri() if urlparse(source).scheme == "file" else str(resolved)
