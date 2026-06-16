#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class SnapshotExpectation:
    snapshot_id: int | None = None
    as_of_timestamp_ms: int | None = None
    expected_record_count: int | None = None
    expected_files: list[str] | None = None


def main() -> None:
    args = parse_args()
    output = Path(args.output).resolve()
    if args.case is None and output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)

    cases: list[tuple[str, Callable[[Path], dict[str, Any]]]] = [
        ("spark/v1-table", generate_spark_v1_table),
        ("spark/v2-table", generate_spark_v2_table),
        ("spark/v2-position-deletes", generate_spark_position_deletes),
        ("pyiceberg/v2-equality-deletes", generate_pyiceberg_equality_deletes),
        ("spark/partition-evolution", generate_spark_partition_evolution),
        ("spark/schema-evolution", generate_spark_schema_evolution),
        ("spark/snapshot-history", generate_spark_snapshot_history),
    ]

    selected_cases = (
        [(relative_dir, generator) for relative_dir, generator in cases if relative_dir == args.case]
        if args.case is not None
        else cases
    )
    if args.case is not None and len(selected_cases) != 1:
        raise ValueError(f"unknown Iceberg fixture case: {args.case}")

    for relative_dir, generator in selected_cases:
        if args.case is None and relative_dir.startswith("spark/"):
            subprocess.run(
                [sys.executable, __file__, "--output", str(output), "--case", relative_dir],
                check=True,
            )
            continue
        case_dir = output / relative_dir
        if case_dir.exists():
            shutil.rmtree(case_dir)
        case_dir.mkdir(parents=True, exist_ok=True)
        manifest = generator(case_dir)
        manifest["files"] = case_file_checksums(case_dir)
        write_json(case_dir / "manifest.json", manifest)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate external Iceberg reference fixtures")
    parser.add_argument("--output", required=True, help="Output directory mounted from fixtures/external/iceberg-reference")
    parser.add_argument("--case", help="Generate one case, used internally for isolated Spark subprocesses")
    return parser.parse_args()


def generate_spark_v1_table(case_dir: Path) -> dict[str, Any]:
    return generate_spark_case(
        case_dir,
        case_name="v1-table",
        format_version=1,
        operations=[
            ("create", [(1, "US"), (2, "CA"), (3, "US")]),
        ],
        partitioned=False,
    )


def generate_spark_v2_table(case_dir: Path) -> dict[str, Any]:
    return generate_spark_case(
        case_dir,
        case_name="v2-table",
        format_version=2,
        operations=[
            ("create", [(1, "US"), (2, "CA"), (3, "US"), (4, "MX")]),
        ],
        partitioned=True,
    )


def generate_spark_position_deletes(case_dir: Path) -> dict[str, Any]:
    return generate_spark_case(
        case_dir,
        case_name="v2-position-deletes",
        format_version=2,
        operations=[
            ("create", [(1, "US"), (2, "CA"), (3, "US")]),
            ("delete_where", "id = 2"),
        ],
        partitioned=True,
    )


def generate_pyiceberg_equality_deletes(case_dir: Path) -> dict[str, Any]:
    try:
        return generate_pyiceberg_case(case_dir)
    except Exception as exc:  # pragma: no cover - exercised inside Docker only
        raise RuntimeError(
            "PyIceberg equality-delete fixture generation failed. Keep this case explicit so "
            "Spark-only fixture generation cannot silently satisfy the equality-delete requirement."
        ) from exc


def generate_spark_partition_evolution(case_dir: Path) -> dict[str, Any]:
    return generate_spark_case(
        case_dir,
        case_name="partition-evolution",
        format_version=2,
        operations=[
            ("create", [(1, "US"), (2, "CA")]),
            ("partition_add_country", None),
            ("append", [(3, "US"), (4, "MX")]),
        ],
        partitioned=False,
    )


def generate_spark_schema_evolution(case_dir: Path) -> dict[str, Any]:
    return generate_spark_case(
        case_dir,
        case_name="schema-evolution",
        format_version=2,
        operations=[
            ("create", [(1, "US"), (2, "CA")]),
            ("add_column", None),
            ("append_with_city", [(3, "US", "los-angeles"), (4, "MX", "mexico-city")]),
        ],
        partitioned=True,
    )


def generate_spark_snapshot_history(case_dir: Path) -> dict[str, Any]:
    return generate_spark_case(
        case_dir,
        case_name="snapshot-history",
        format_version=2,
        operations=[
            ("create", [(1, "US"), (2, "CA")]),
            ("append", [(3, "US")]),
            ("append", [(4, "MX")]),
        ],
        partitioned=True,
    )


def generate_spark_case(
    case_dir: Path,
    *,
    case_name: str,
    format_version: int,
    operations: list[tuple[str, Any]],
    partitioned: bool,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    warehouse = Path("warehouse")
    warehouse_path = case_dir / warehouse
    catalog_name = f"local_{case_name.replace('-', '_')}"
    table = f"{catalog_name}.db.places"
    previous_cwd = Path.cwd()
    os.chdir(case_dir)
    spark = None
    try:
        spark = (
            SparkSession.builder.appName(f"laql-{case_name}")
            .master("local[2]")
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", str(warehouse))
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.jars", iceberg_spark_runtime_jar())
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sql(f"create namespace if not exists {catalog_name}.db")
        spark.sql(f"drop table if exists {table}")
        for operation, payload in operations:
            if operation == "create":
                create_spark_table(spark, table, payload, format_version, partitioned)
            elif operation == "append":
                append_spark_rows(spark, table, payload)
            elif operation == "append_with_city":
                append_spark_rows_with_city(spark, table, payload)
            elif operation == "delete_where":
                spark.sql(f"delete from {table} where {payload}")
            elif operation == "partition_add_country":
                spark.sql(f"alter table {table} add partition field country")
            elif operation == "add_column":
                spark.sql(f"alter table {table} add column city string")
            else:
                raise ValueError(f"unknown Spark fixture operation: {operation}")

        snapshots = snapshot_expectations_from_spark(spark, table, case_dir)
        metadata_path = latest_metadata_path(warehouse_path / "db" / "places" / "metadata")
        return case_manifest(
            engine="spark",
            case_name=case_name,
            engine_version=spark.version,
            metadata_path=relative_posix(case_dir, metadata_path),
            snapshots=snapshots,
        )
    finally:
        if spark is not None:
            spark.stop()
            SparkSession._instantiatedSession = None
            SparkSession._activeSession = None
        os.chdir(previous_cwd)


def iceberg_spark_runtime_jar() -> str:
    return os.environ.get(
        "LAQL_ICEBERG_SPARK_RUNTIME_JAR",
        "/opt/iceberg/jars/iceberg-spark-runtime.jar",
    )


def create_spark_table(
    spark: Any,
    table: str,
    rows: list[tuple[int, str]],
    format_version: int,
    partitioned: bool,
) -> None:
    values = ",".join(f"({id_value}, '{country}')" for id_value, country in rows)
    partition_clause = "partitioned by (country)" if partitioned else ""
    spark.sql(
        f"""
        create table {table}
        using iceberg
        {partition_clause}
        tblproperties ('format-version'='{format_version}')
        as select * from values {values} as t(id, country)
        """
    )


def append_spark_rows(spark: Any, table: str, rows: list[tuple[int, str]]) -> None:
    values = ",".join(f"({id_value}, '{country}')" for id_value, country in rows)
    spark.sql(f"insert into {table} select * from values {values} as t(id, country)")


def append_spark_rows_with_city(spark: Any, table: str, rows: list[tuple[int, str, str]]) -> None:
    values = ",".join(f"({id_value}, '{country}', '{city}')" for id_value, country, city in rows)
    spark.sql(f"insert into {table} select * from values {values} as t(id, country, city)")


def snapshot_expectations_from_spark(spark: Any, table: str, case_dir: Path) -> list[SnapshotExpectation]:
    snapshot_rows = spark.sql(f"select snapshot_id, committed_at from {table}.snapshots order by committed_at").collect()
    expectations: list[SnapshotExpectation] = []
    for row in snapshot_rows:
        snapshot_id = int(row["snapshot_id"])
        record_count = int(
            spark.sql(f"select count(*) as row_count from {table} version as of {snapshot_id}")
            .collect()[0]["row_count"]
        )
        file_rows = spark.sql(
            f"select distinct input_file_name() as file_path from {table} version as of {snapshot_id}"
        ).collect()
        expectations.append(
            SnapshotExpectation(
                snapshot_id=snapshot_id,
                as_of_timestamp_ms=timestamp_ms(row["committed_at"]),
                expected_record_count=record_count,
                expected_files=sorted(
                    fixture_relative_path(case_dir, row["file_path"]) for row in file_rows
                ),
            )
        )
    return expectations


def timestamp_ms(value: Any) -> int:
    if hasattr(value, "tzinfo") and value.tzinfo is not None:
        return int(value.timestamp() * 1000)
    if hasattr(value, "replace"):
        return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)
    raise TypeError(f"unsupported Spark timestamp value: {value!r}")


def fixture_relative_path(case_dir: Path, value: str) -> str:
    parsed = urlparse(value)
    path = unquote(parsed.path if parsed.scheme == "file" else value)
    parsed_path = Path(path)
    if not parsed_path.is_absolute():
        return parsed_path.as_posix()
    return parsed_path.relative_to(case_dir).as_posix()


def generate_pyiceberg_case(case_dir: Path) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.types import NestedField, StringType, IntegerType

    warehouse = case_dir / "warehouse"
    catalog = load_catalog(
        "laql",
        type="sql",
        uri=f"sqlite:///{case_dir / 'catalog.db'}",
        warehouse=str(warehouse),
        **{"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"},
    )
    catalog.create_namespace("db")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="country", field_type=StringType(), required=False),
    )
    table = catalog.create_table(
        "db.places",
        schema=schema,
        partition_spec=PartitionSpec(),
        properties={"format-version": "2"},
    )
    arrow_table = pa.Table.from_pylist(
        [
            {"id": 1, "country": "US"},
            {"id": 2, "country": "CA"},
            {"id": 3, "country": "US"},
        ],
        schema=pa.schema([pa.field("id", pa.int32(), nullable=False), ("country", pa.string())]),
    )
    table.append(arrow_table)
    table_dir = warehouse / "db.db" / "places"
    data_files = sorted((table_dir / "data").glob("*.parquet"))
    if not data_files:
        raise RuntimeError("PyIceberg table did not produce a data Parquet file")
    data_file_path = relative_posix(case_dir, data_files[0])
    delete_path = table_dir / "deletes" / "country-ca.eq.parquet"
    delete_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist([{"country": "CA"}], schema=pa.schema([("country", pa.string())])),
        delete_path,
    )
    data_manifest = {
            "path": "warehouse/db.db/places/metadata/laql-data-manifest.json",
        "files": [
            {
                "path": data_file_path,
                "sequenceNumber": 1,
                "partition": {},
                "recordCount": 3,
                "fileSizeInBytes": data_files[0].stat().st_size,
            }
        ],
    }
    delete_manifest = {
        "path": "warehouse/db.db/places/metadata/laql-delete-manifest.json",
        "files": [],
        "deleteFiles": [
            {
                "content": "equality-delete",
                "path": relative_posix(case_dir, delete_path),
                "partition": {},
            }
        ],
    }
    metadata_dir = table_dir / "metadata"
    write_json(metadata_dir / "laql-data-manifest.json", data_manifest)
    write_json(metadata_dir / "laql-delete-manifest.json", delete_manifest)
    metadata_path = metadata_dir / "laql-equality.metadata.json"
    write_json(
        metadata_path,
        {
            "format-version": 2,
            "table-uuid": "pyiceberg-equality-delete-reference",
            "location": "warehouse/db.db/places",
            "current-snapshot-id": 2,
            "refs": {"main": {"type": "branch", "snapshot-id": 2}},
            "schemas": [
                {
                    "schema-id": 1,
                    "fields": [
                        {"id": 1, "name": "id", "type": "int", "required": True},
                        {"id": 2, "name": "country", "type": "string", "required": False},
                    ],
                }
            ],
            "snapshots": [
                {
                    "snapshot-id": 2,
                    "timestamp-ms": 1_767_225_600_000,
                    "schema-id": 1,
                    "manifests": [
                        {"path": data_manifest["path"]},
                        {"path": delete_manifest["path"]},
                    ],
                }
            ],
        },
    )
    return case_manifest(
        engine="pyiceberg",
        case_name="v2-equality-deletes",
        engine_version=pyiceberg_version(),
        metadata_path=relative_posix(case_dir, metadata_path),
        snapshots=[
            SnapshotExpectation(
                snapshot_id=2,
                as_of_timestamp_ms=1_767_225_600_000,
                expected_record_count=3,
                expected_files=[data_file_path],
            )
        ],
    )


def pyiceberg_version() -> str:
    try:
        from pyiceberg import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def latest_metadata_path(metadata_dir: Path) -> Path:
    candidates = sorted(metadata_dir.glob("*.metadata.json"))
    if not candidates:
        raise RuntimeError(f"no Iceberg metadata files found in {metadata_dir}")
    return candidates[-1]


def case_manifest(
    *,
    engine: str,
    case_name: str,
    engine_version: str,
    metadata_path: str,
    snapshots: list[SnapshotExpectation],
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "engine": engine,
        "engineVersion": engine_version,
        "case": case_name,
        "metadataPath": metadata_path,
        "snapshots": [snapshot_json(snapshot) for snapshot in snapshots],
    }
    if snapshots and snapshots[-1].expected_record_count is not None:
        manifest["expectedRecordCount"] = snapshots[-1].expected_record_count
    return manifest


def snapshot_json(snapshot: SnapshotExpectation) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if snapshot.snapshot_id is not None:
        out["snapshotId"] = snapshot.snapshot_id
    if snapshot.as_of_timestamp_ms is not None:
        out["asOfTimestampMs"] = snapshot.as_of_timestamp_ms
    if snapshot.expected_record_count is not None:
        out["expectedRecordCount"] = snapshot.expected_record_count
    if snapshot.expected_files is not None:
        out["expectedFiles"] = snapshot.expected_files
    return out


def case_file_checksums(case_dir: Path) -> list[dict[str, str]]:
    files: list[dict[str, str]] = []
    for path in sorted(case_dir.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        files.append({"path": relative_posix(case_dir, path), "sha256": sha256_file(path)})
    return files


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def relative_posix(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf8")


if __name__ == "__main__":
    main()
