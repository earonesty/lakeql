from __future__ import annotations

from pathlib import Path

from semantic_museum.discovery import discover_source_objects


def test_nested_source_indexes_are_sorted_and_deduplicated(tmp_path: Path) -> None:
    shard_a = tmp_path / "a.txt"
    shard_b = tmp_path / "b.txt"
    unit = tmp_path / "unit" / "index.txt"
    root = tmp_path / "root-index.txt"
    unit.parent.mkdir()
    unit.write_text(f"{shard_b.as_uri()}\n../a.txt\n")
    root.write_text(f"{unit.as_uri()}\n{shard_a.as_uri()}\n")

    sources = discover_source_objects(
        [root.as_uri()],
        max_bytes=4096,
        timeout_seconds=1,
    )
    assert sources == sorted([shard_a.as_uri(), shard_b.as_uri()])
