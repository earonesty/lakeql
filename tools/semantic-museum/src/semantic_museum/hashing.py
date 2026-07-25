from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, BinaryIO


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        update_digest(digest, stream)
    return digest.hexdigest()


def update_digest(digest: Any, stream: BinaryIO, chunk_size: int = 1024 * 1024) -> None:
    while chunk := stream.read(chunk_size):
        digest.update(chunk)


def fingerprint_strings(values: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = value.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def bucket_for(media_id: str, bucket_bits: int) -> str:
    if bucket_bits == 0:
        return "0"
    if bucket_bits < 4 or bucket_bits > 24 or bucket_bits % 4 != 0:
        raise ValueError("bucket_bits must be zero or a multiple of four between 4 and 24")
    return hashlib.sha256(media_id.encode()).hexdigest()[: bucket_bits // 4]
