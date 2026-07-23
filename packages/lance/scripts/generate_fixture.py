"""Generate the checked-in Lance 2.0 compatibility fixture with official tooling."""

from __future__ import annotations

import hashlib
import json
import shutil
from base64 import b64encode
from datetime import date, datetime, timezone
from pathlib import Path

import lance
import pyarrow as pa


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures" / "take-v2.0.lance"
TYPE_FIXTURE = ROOT / "fixtures" / "types-v2.0.lance"
DELETION_FIXTURE = ROOT / "fixtures" / "deletions-v2.0.lance"
WORKERD_MODULE = ROOT / "src" / "fixture.generated.ts"


def rows() -> list[dict[str, object]]:
    return [
        {
            "serial": 10_000_000 + index,
            "mark_text": None if index in {9, 41} else f"MARK {index:03d}",
            "owner_name": f"Owner {index % 7}",
            "status": ("LIVE", "DEAD", "PENDING")[index % 3],
            "source_url": f"https://example.test/marks/{10_000_000 + index}",
            "score": index * 0.25,
            "active": index % 2 == 0,
        }
        for index in range(64)
    ]


def main() -> None:
    if FIXTURE.exists():
        shutil.rmtree(FIXTURE)
    table = pa.Table.from_pylist(
        rows(),
        schema=pa.schema(
            [
                pa.field("serial", pa.int64(), nullable=False),
                pa.field("mark_text", pa.string()),
                pa.field("owner_name", pa.string()),
                pa.field("status", pa.string()),
                pa.field("source_url", pa.string()),
                pa.field("score", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        ),
    )
    dataset = lance.write_dataset(
        table,
        FIXTURE,
        mode="create",
        data_storage_version="2.0",
        enable_stable_row_ids=True,
        enable_v2_manifest_paths=True,
        max_rows_per_file=16,
        max_rows_per_group=8,
    )
    expected = {
        "producer": {"package": "pylance", "version": lance.__version__},
        "storageVersion": dataset.data_storage_version,
        "datasetVersion": dataset.version,
        "rowCount": len(rows()),
        "rowIds": [
            str(value)
            for value in dataset.scanner(columns=["serial"], with_row_id=True)
            .to_table()
            .column("_rowid")
            .to_pylist()
        ],
        "sampleProjection": {
            "rowIds": ["31", "0", "31", "47", "9"],
            "select": ["serial", "mark_text", "owner_name", "status", "source_url"],
            "rows": [rows()[index] for index in [31, 0, 31, 47, 9]],
        },
    }
    expected["sampleProjection"]["rows"] = [
        {column: row[column] for column in expected["sampleProjection"]["select"]}
        for row in expected["sampleProjection"]["rows"]
    ]
    expected["sha256"] = {
        str(path.relative_to(FIXTURE)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(FIXTURE.rglob("*"))
        if path.is_file()
    }
    (FIXTURE / "expected.json").write_text(
        json.dumps(expected, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_type_fixture()
    write_deletion_fixture()
    write_workerd_module()


def write_type_fixture() -> None:
    if TYPE_FIXTURE.exists():
        shutil.rmtree(TYPE_FIXTURE)
    schema = pa.schema(
        [
            pa.field("i8", pa.int8(), nullable=False),
            pa.field("u8", pa.uint8(), nullable=False),
            pa.field("i16", pa.int16(), nullable=False),
            pa.field("u16", pa.uint16(), nullable=False),
            pa.field("i32", pa.int32(), nullable=False),
            pa.field("u32", pa.uint32(), nullable=False),
            pa.field("i64", pa.int64(), nullable=False),
            pa.field("u64", pa.uint64(), nullable=False),
            pa.field("f32", pa.float32(), nullable=False),
            pa.field("f64", pa.float64(), nullable=False),
            pa.field("flag", pa.bool_(), nullable=False),
            pa.field("plain_text", pa.string(), nullable=False),
            pa.field("payload", pa.binary(), nullable=False),
            pa.field("event_date", pa.date32(), nullable=False),
            pa.field("utc_millis", pa.timestamp("ms", tz="UTC"), nullable=False),
            pa.field("local_micros", pa.timestamp("us"), nullable=False),
            pa.field("maybe_i32", pa.int32()),
        ]
    )
    typed_rows = [
        {
            "i8": -8 + index,
            "u8": 250 - index,
            "i16": -1_600 + index,
            "u16": 65_000 - index,
            "i32": -2_000_000 + index,
            "u32": 4_000_000_000 + index,
            "i64": -9_007_199_254_740_993 + index,
            "u64": 18_000_000_000_000_000_000 + index,
            "f32": 1.25 + index,
            "f64": -3.5 - index,
            "flag": index % 2 == 0,
            "plain_text": f"plain-{index}",
            "payload": bytes([0, 255 - index, index]),
            "event_date": date(2026, 7, 23 + index),
            "utc_millis": datetime(
                2026, 7, 23, 1, 2, 3, 456_000 + index * 1_000, tzinfo=timezone.utc
            ),
            "local_micros": datetime(2026, 7, 23, 1, 2, 3, 456_789 + index),
            "maybe_i32": None if index == 1 else index,
        }
        for index in range(3)
    ]
    dataset = lance.write_dataset(
        pa.Table.from_pylist(typed_rows, schema=schema),
        TYPE_FIXTURE,
        mode="create",
        data_storage_version="2.0",
        enable_stable_row_ids=True,
        enable_v2_manifest_paths=True,
        max_rows_per_group=2,
    )
    expected = {
        "producer": {"package": "pylance", "version": lance.__version__},
        "storageVersion": dataset.data_storage_version,
        "datasetVersion": dataset.version,
        "rowCount": len(typed_rows),
        "rowIds": [
            str(value)
            for value in dataset.scanner(columns=["i8"], with_row_id=True)
            .to_table()
            .column("_rowid")
            .to_pylist()
        ],
    }
    expected["sha256"] = fixture_hashes(TYPE_FIXTURE)
    (TYPE_FIXTURE / "expected.json").write_text(
        json.dumps(expected, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def fixture_hashes(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "expected.json"
    }


def write_deletion_fixture() -> None:
    if DELETION_FIXTURE.exists():
        shutil.rmtree(DELETION_FIXTURE)
    table = pa.table(
        {
            "serial": pa.array(range(100, 116), type=pa.int64()),
            "label": pa.array([f"row-{index}" for index in range(16)], type=pa.string()),
        }
    )
    dataset = lance.write_dataset(
        table,
        DELETION_FIXTURE,
        mode="create",
        data_storage_version="2.0",
        enable_stable_row_ids=True,
        enable_v2_manifest_paths=True,
        max_rows_per_group=8,
    )
    original_row_ids = [
        str(value)
        for value in dataset.scanner(columns=["serial"], with_row_id=True)
        .to_table()
        .column("_rowid")
        .to_pylist()
    ]
    deleted_offsets = [2, 7, 13]
    dataset.delete("serial IN (102, 107, 113)")
    expected = {
        "producer": {"package": "pylance", "version": lance.__version__},
        "storageVersion": dataset.data_storage_version,
        "datasetVersion": dataset.version,
        "rowCount": 13,
        "originalRowIds": original_row_ids,
        "deletedOffsets": deleted_offsets,
        "deletedRowIds": [original_row_ids[offset] for offset in deleted_offsets],
    }
    expected["sha256"] = fixture_hashes(DELETION_FIXTURE)
    (DELETION_FIXTURE / "expected.json").write_text(
        json.dumps(expected, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_workerd_module() -> None:
    files = {
        f"fixtures/{root.name}/{path.relative_to(root)}": b64encode(path.read_bytes()).decode(
            "ascii"
        )
        for root in [FIXTURE, TYPE_FIXTURE, DELETION_FIXTURE]
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "expected.json"
    }
    entries = "\n".join(
        f"  {json.dumps(path)}: {json.dumps(encoded)},"
        for path, encoded in files.items()
    )
    WORKERD_MODULE.write_text(
        "/* Generated by scripts/generate_fixture.py. */\n"
        "export const WORKERD_FIXTURE_BASE64: Readonly<Record<string, string>> = {\n"
        f"{entries}\n"
        "};\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
