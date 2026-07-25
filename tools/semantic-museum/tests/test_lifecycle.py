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
    uploaded_diagnostics: list[Path] = []

    def upload_diagnostics(path: Path) -> dict[str, object]:
        uploaded_diagnostics.append(path)
        assert (path / "stdout.log").read_text() == "standard output\n"
        assert (path / "stderr.log").read_text() == "standard error\n"
        return {"manifest_key": "leases/job/diagnostics/manifest.json", "objects": 3}

    exit_code = run_supervised(
        command=[
            sys.executable,
            "-c",
            (
                "import sys; "
                "print('standard output'); "
                "print('standard error', file=sys.stderr); "
                "raise SystemExit(7)"
            ),
        ],
        max_runtime_seconds=5,
        checkpoint_grace_seconds=1,
        receipt_path=receipt,
        provider="none",
        upload_receipt=uploaded.append,
        upload_diagnostics=upload_diagnostics,
    )
    assert exit_code == 7
    value = read_json(receipt)
    assert value["child_exit_code"] == 7
    assert not value["timed_out"]
    assert not value["destruction_required"]
    assert value["diagnostics"]["objects"] == 3
    assert value["diagnostics_upload_error"] is None
    assert uploaded_diagnostics == [tmp_path / "diagnostics"]
    assert uploaded == [receipt]
