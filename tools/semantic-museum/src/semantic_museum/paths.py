from __future__ import annotations

from pathlib import Path


def embedding_path(output: Path, model_id: str, bucket: str) -> Path:
    return (
        output
        / "embeddings"
        / f"model={partition_value(model_id)}"
        / f"bucket={bucket}"
        / "part-00000.parquet"
    )


def failure_path(output: Path, bucket: str) -> Path:
    return output / "failures" / f"bucket={bucket}" / "part-00000.parquet"


def receipt_path(output: Path, bucket: str) -> Path:
    return output / "receipts" / f"{bucket}.json"


def partition_value(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "-_." else "_"
        for character in value
    )
