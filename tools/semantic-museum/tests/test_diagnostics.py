from __future__ import annotations

import json
from pathlib import Path

import pytest

from semantic_museum import cli, cloud_job
from semantic_museum.cli import main
from semantic_museum.diagnostics import DiagnosticRecorder
from semantic_museum.jsonio import read_json


def test_exception_record_contains_traceback_and_cause(tmp_path: Path) -> None:
    recorder = DiagnosticRecorder(tmp_path / "diagnostics", command="test")
    try:
        try:
            raise ValueError("source row is malformed")
        except ValueError as cause:
            raise RuntimeError("planning failed") from cause
    except RuntimeError as error:
        recorder.record_exception(error)

    diagnostic = read_json(tmp_path / "diagnostics" / "error.json")
    assert diagnostic["exception"]["type"] == "builtins.RuntimeError"
    assert diagnostic["exception"]["message"] == "planning failed"
    assert "raise RuntimeError" in diagnostic["exception"]["traceback"]
    assert diagnostic["exception"]["cause"]["type"] == "builtins.ValueError"
    assert diagnostic["runtime"]["disk"]["free_bytes"] > 0


def test_cli_records_failure_for_plan_entry_point(tmp_path: Path) -> None:
    diagnostics = tmp_path / "diagnostics"
    with pytest.raises(ValueError, match="at least one"):
        main(
            [
                "--diagnostics-dir",
                str(diagnostics),
                "plan",
                "--output",
                str(tmp_path / "plan"),
                "--max-records",
                "1",
                "--max-total-image-bytes",
                "1",
            ]
        )

    error = read_json(diagnostics / "error.json")
    assert error["command"] == "plan"
    events = [json.loads(line) for line in (diagnostics / "events.jsonl").read_text().splitlines()]
    assert [event["event"] for event in events] == [
        "command_started",
        "command_failed",
    ]


def test_capture_artifact_preserves_partial_planner_database(tmp_path: Path) -> None:
    source = tmp_path / "plan-state.sqlite3"
    source.write_bytes(b"planner checkpoint")
    recorder = DiagnosticRecorder(tmp_path / "diagnostics", command="cloud-job")

    captured = recorder.capture_artifact(
        source,
        name="full-plan-state.sqlite3",
    )

    assert captured is not None
    assert captured.read_bytes() == b"planner checkpoint"


def test_cloud_job_failure_uploads_error_and_planner_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = tmp_path / "work"
    uploaded: list[set[str]] = []
    monkeypatch.delenv("SEMANTIC_MUSEUM_SUPERVISED", raising=False)

    def fail_cloud_job(**_kwargs: object) -> dict[str, object]:
        (work / "plan").mkdir(parents=True)
        (work / "state.json").write_text('{"phase":"full_plan"}\n')
        (work / "plan" / "plan-state.sqlite3").write_bytes(b"resumable state")
        raise RuntimeError("corpus scan failed")

    def upload_bundle(**kwargs: object) -> dict[str, object]:
        directory = kwargs["directory"]
        assert isinstance(directory, Path)
        uploaded.append(
            {
                path.relative_to(directory).as_posix()
                for path in directory.rglob("*")
                if path.is_file()
            }
        )
        return {"objects": len(uploaded[-1]), "manifest_key": "diagnostics/manifest.json"}

    monkeypatch.setattr(cloud_job, "run_cloud_job", fail_cloud_job)
    monkeypatch.setattr(cli, "upload_diagnostic_bundle", upload_bundle)

    with pytest.raises(RuntimeError, match="corpus scan failed"):
        main(
            [
                "cloud-job",
                "--work",
                str(work),
                "--bucket",
                "museum",
            ]
        )

    assert uploaded
    assert {
        "error.json",
        "artifacts/cloud-job-state.json",
        "artifacts/full-plan-state.sqlite3",
    } <= uploaded[0]
