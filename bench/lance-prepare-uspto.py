"""Convert the public MarkWatch USPTO Parquet fixture into a Lance benchmark dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import urllib.request
from pathlib import Path

import lance
import pyarrow.parquet as pq


DEFAULT_SOURCE = (
    "https://pub-cc21dc8afbef4216b7b0e3e63213bfb9.r2.dev/"
    "markwatch/july-v3/serials/marks.parquet"
)
SELECT = ["serial", "mark_text", "owner_name", "status", "source_url"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    temporary: tempfile.TemporaryDirectory[str] | None = None
    source = args.source
    if source.startswith(("http://", "https://")):
        temporary = tempfile.TemporaryDirectory(prefix="lakeql-lance-uspto-")
        local_source = Path(temporary.name) / "marks.parquet"
        urllib.request.urlretrieve(source, local_source)
    else:
        local_source = Path(source)

    try:
        table = pq.read_table(local_source, columns=SELECT)
        if args.output.exists():
            shutil.rmtree(args.output)
        dataset = lance.write_dataset(
            table,
            args.output,
            mode="create",
            data_storage_version="2.0",
            enable_stable_row_ids=True,
            enable_v2_manifest_paths=True,
            max_rows_per_file=65_536,
            max_rows_per_group=4_096,
        )
        row_count = table.num_rows
        offsets = [round(index * (row_count - 1) / 31) for index in range(32)]
        scanned = dataset.scanner(columns=SELECT, with_row_id=True).to_table()
        rows = scanned.select(SELECT).take(offsets).to_pylist()
        row_ids = [
            str(value)
            for value in scanned.column("_rowid").take(offsets).to_pylist()
        ]
        manifest = {
            "source": source,
            "sourceSha256": hashlib.sha256(local_source.read_bytes()).hexdigest(),
            "producer": {"package": "pylance", "version": lance.__version__},
            "storageVersion": dataset.data_storage_version,
            "datasetVersion": dataset.version,
            "rowCount": row_count,
            "rowIds": row_ids,
            "select": SELECT,
            "rows": rows,
        }
        manifest["sha256"] = {
            str(path.relative_to(args.output)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(args.output.rglob("*"))
            if path.is_file()
        }
        (args.output / "benchmark.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    finally:
        if temporary is not None:
            temporary.cleanup()


if __name__ == "__main__":
    main()
