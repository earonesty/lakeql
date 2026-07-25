from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .models import FailureRecord, MediaRecord


def write_embeddings(
    path: Path,
    records: Sequence[MediaRecord],
    embeddings: np.ndarray,
    *,
    model_id: str,
    model_fingerprint: str,
    preprocessing_id: str,
) -> None:
    if embeddings.ndim != 2 or embeddings.shape[0] != len(records):
        raise ValueError("embedding rows do not match records")
    dimension = int(embeddings.shape[1])
    flattened = pa.array(
        np.ascontiguousarray(embeddings, dtype=np.float32).reshape(-1),
        type=pa.float32(),
    )
    vectors = pa.FixedSizeListArray.from_arrays(flattened, dimension)
    table = pa.table(
        {
            "item_id": pa.array([record.item_id for record in records], type=pa.string()),
            "media_id": pa.array([record.media_id for record in records], type=pa.string()),
            "model_id": pa.array([model_id] * len(records), type=pa.string()),
            "model_fingerprint": pa.array(
                [model_fingerprint] * len(records),
                type=pa.string(),
            ),
            "preprocessing_id": pa.array(
                [preprocessing_id] * len(records),
                type=pa.string(),
            ),
            "embedding": vectors,
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=3,
        use_dictionary=["model_id", "model_fingerprint", "preprocessing_id"],
        write_statistics=True,
        row_group_size=4096,
    )


def write_failures(path: Path, failures: Sequence[FailureRecord]) -> None:
    table = pa.table(
        {
            "media_id": pa.array([failure.media_id for failure in failures], type=pa.string()),
            "stage": pa.array([failure.stage for failure in failures], type=pa.string()),
            "retryable": pa.array(
                [failure.retryable for failure in failures],
                type=pa.bool_(),
            ),
            "error_type": pa.array(
                [failure.error_type for failure in failures],
                type=pa.string(),
            ),
            "message": pa.array([failure.message for failure in failures], type=pa.string()),
            "attempts": pa.array([failure.attempts for failure in failures], type=pa.int16()),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd", write_statistics=True)


def write_metadata(path: Path, records: Sequence[MediaRecord]) -> None:
    table = pa.table(
        {
            "item_id": pa.array([record.item_id for record in records], type=pa.string()),
            "media_id": pa.array([record.media_id for record in records], type=pa.string()),
            "source": pa.array([record.source for record in records], type=pa.string()),
            "unit": pa.array([record.unit for record in records], type=pa.string()),
            "title": pa.array([record.title for record in records], type=pa.string()),
            "description": pa.array(
                [record.description for record in records], type=pa.string()
            ),
            "creators": pa.array(
                [list(record.creators) for record in records], type=pa.list_(pa.string())
            ),
            "dates": pa.array(
                [list(record.dates) for record in records], type=pa.list_(pa.string())
            ),
            "media": pa.array(
                [list(record.media) for record in records], type=pa.list_(pa.string())
            ),
            "object_types": pa.array(
                [list(record.object_types) for record in records],
                type=pa.list_(pa.string()),
            ),
            "subjects": pa.array(
                [list(record.subjects) for record in records], type=pa.list_(pa.string())
            ),
            "places": pa.array(
                [list(record.places) for record in records], type=pa.list_(pa.string())
            ),
            "record_url": pa.array(
                [record.record_url for record in records], type=pa.string()
            ),
            "image_url": pa.array(
                [record.image_url for record in records], type=pa.string()
            ),
            "display_url": pa.array(
                [record.display_url for record in records], type=pa.string()
            ),
            "record_rights": pa.array(
                [record.record_rights for record in records], type=pa.string()
            ),
            "media_rights": pa.array(
                [record.media_rights for record in records], type=pa.string()
            ),
            "source_hash": pa.array(
                [record.source_hash for record in records], type=pa.string()
            ),
            "source_updated_at": pa.array(
                [record.source_updated_at for record in records], type=pa.int64()
            ),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=3,
        use_dictionary=["source", "unit", "record_rights", "media_rights"],
        write_statistics=True,
        row_group_size=4096,
    )


def validate_embeddings(
    path: Path,
    *,
    expected_rows: int,
    expected_dimension: int,
    expected_model_fingerprint: str,
) -> None:
    file = pq.ParquetFile(path)
    if file.metadata.num_rows != expected_rows:
        raise RuntimeError(
            f"{path}: row count {file.metadata.num_rows} != expected {expected_rows}"
        )
    table = file.read(columns=["model_fingerprint", "embedding"])
    fingerprints = table.column("model_fingerprint").unique().to_pylist()
    expected_fingerprints = [] if expected_rows == 0 else [expected_model_fingerprint]
    if fingerprints != expected_fingerprints:
        raise RuntimeError(f"{path}: unexpected model fingerprints {fingerprints}")
    vector_type = table.schema.field("embedding").type
    if not pa.types.is_fixed_size_list(vector_type) or vector_type.list_size != expected_dimension:
        raise RuntimeError(f"{path}: unexpected embedding type {vector_type}")
