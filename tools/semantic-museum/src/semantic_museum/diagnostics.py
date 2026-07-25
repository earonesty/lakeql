from __future__ import annotations

import os
import platform
import shutil
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .hashing import canonical_json_bytes
from .jsonio import write_json_atomic


class DiagnosticRecorder:
    def __init__(self, directory: Path, *, command: str) -> None:
        self.directory = directory
        self.command = command
        self.directory.mkdir(parents=True, exist_ok=True)

    def event(self, name: str, **values: Any) -> None:
        entry = {
            "schema_version": 1,
            "recorded_at": datetime.now(UTC).isoformat(),
            "command": self.command,
            "event": name,
            **values,
        }
        path = self.directory / "events.jsonl"
        with path.open("ab") as stream:
            stream.write(canonical_json_bytes(entry))
            stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        write_json_atomic(self.directory / "latest.json", entry)

    def record_exception(self, error: BaseException) -> Path:
        value = {
            "schema_version": 1,
            "recorded_at": datetime.now(UTC).isoformat(),
            "command": self.command,
            "exception": _exception_description(error),
            "runtime": runtime_snapshot(self.directory),
        }
        path = self.directory / "error.json"
        write_json_atomic(path, value)
        self.event(
            "command_failed",
            exception_type=f"{type(error).__module__}.{type(error).__qualname__}",
            exception_message=str(error),
        )
        return path

    def capture_artifact(self, source: Path, *, name: str) -> Path | None:
        if not source.is_file():
            return None
        destination = self.directory / "artifacts" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            destination.unlink()
        try:
            os.link(source, destination)
        except OSError:
            shutil.copy2(source, destination)
        self.event(
            "artifact_captured",
            artifact=destination.relative_to(self.directory).as_posix(),
            bytes=destination.stat().st_size,
        )
        return destination


def runtime_snapshot(path: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(path)
    snapshot: dict[str, Any] = {
        "python": sys.version,
        "platform": platform.platform(),
        "pid": os.getpid(),
        "disk": {
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
        },
    }
    memory = _linux_memory()
    if memory:
        snapshot["memory"] = memory
    return snapshot


def _exception_description(error: BaseException) -> dict[str, Any]:
    description: dict[str, Any] = {
        "type": f"{type(error).__module__}.{type(error).__qualname__}",
        "message": str(error),
        "traceback": "".join(traceback.TracebackException.from_exception(error).format(chain=True)),
    }
    if error.__cause__ is not None:
        description["cause"] = _exception_description(error.__cause__)
    elif error.__context__ is not None and not error.__suppress_context__:
        description["context"] = _exception_description(error.__context__)
    return description


def _linux_memory() -> dict[str, int]:
    path = Path("/proc/meminfo")
    if not path.is_file():
        return {}
    wanted = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    values: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        name, separator, raw = line.partition(":")
        if separator and name in wanted:
            fields = raw.split()
            if fields and fields[0].isdigit():
                values[f"{name}_bytes"] = int(fields[0]) * 1024
    return values
