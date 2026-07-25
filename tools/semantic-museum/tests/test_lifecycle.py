from __future__ import annotations

import sys
from pathlib import Path

from semantic_museum.jsonio import read_json
from semantic_museum.lifecycle import run_supervised


def test_supervisor_records_child_exit_without_cloud_destruction(
    tmp_path: Path,
) -> None:
    receipt = tmp_path / "terminal.json"
    uploaded: list[Path] = []
    exit_code = run_supervised(
        command=[sys.executable, "-c", "raise SystemExit(7)"],
        max_runtime_seconds=5,
        checkpoint_grace_seconds=1,
        receipt_path=receipt,
        provider="none",
        upload_receipt=uploaded.append,
    )
    assert exit_code == 7
    value = read_json(receipt)
    assert value["child_exit_code"] == 7
    assert not value["timed_out"]
    assert not value["destruction_required"]
    assert uploaded == [receipt]
