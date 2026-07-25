from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import httpx

from .diagnostics import runtime_snapshot
from .hashing import canonical_json_bytes, sha256_bytes
from .jsonio import write_json_atomic

Provider = Literal["auto", "runpod", "vast", "none"]


def run_supervised(
    *,
    command: Sequence[str],
    max_runtime_seconds: int,
    checkpoint_grace_seconds: int,
    receipt_path: Path,
    provider: Provider,
    destroy: Callable[[str], None] | None = None,
    upload_receipt: Callable[[Path], None] | None = None,
    diagnostics_path: Path | None = None,
    upload_diagnostics: Callable[[Path], dict[str, Any]] | None = None,
) -> int:
    if not command:
        raise ValueError("supervised command is required")
    if max_runtime_seconds <= 0:
        raise ValueError("maximum runtime must be positive")
    if checkpoint_grace_seconds <= 0:
        raise ValueError("checkpoint grace must be positive")
    selected_provider = _select_provider(provider)
    started_at = datetime.now(UTC)
    started = time.monotonic()
    timed_out = False
    forwarded_signal: int | None = None
    process: subprocess.Popen[Any] | None = None
    exit_code: int | None = None
    terminal_error: BaseException | None = None
    previous_handlers: dict[int, Any] = {}
    diagnostics_path = diagnostics_path or receipt_path.parent / "diagnostics"
    diagnostics_path.mkdir(parents=True, exist_ok=True)
    stdout_path = diagnostics_path / "stdout.log"
    stderr_path = diagnostics_path / "stderr.log"
    stdout_stream = stdout_path.open("ab")
    stderr_stream = stderr_path.open("ab")
    diagnostics_upload: dict[str, Any] | None = None
    diagnostics_upload_error: BaseException | None = None
    output_threads: list[threading.Thread] = []
    output_capture_errors: list[BaseException] = []

    def forward(signum: int, _frame: Any) -> None:
        nonlocal forwarded_signal
        forwarded_signal = signum
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)

    try:
        process = subprocess.Popen(
            list(command),
            start_new_session=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        output_threads = [
            _start_tee(
                process.stdout,
                stdout_stream,
                getattr(sys.stdout, "buffer", None),
                output_capture_errors,
            ),
            _start_tee(
                process.stderr,
                stderr_stream,
                getattr(sys.stderr, "buffer", None),
                output_capture_errors,
            ),
        ]
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, forward)
        try:
            exit_code = process.wait(timeout=max_runtime_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                exit_code = process.wait(timeout=checkpoint_grace_seconds)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                exit_code = process.wait()
    except BaseException as error:
        terminal_error = error
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=checkpoint_grace_seconds)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    finally:
        for thread in output_threads:
            thread.join()
        if output_capture_errors and terminal_error is None:
            first_error = output_capture_errors[0]
            terminal_error = RuntimeError(
                f"failed to capture child output: {type(first_error).__name__}: {first_error}"
            )
        stdout_stream.close()
        stderr_stream.close()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        supervisor_state = {
            "schema_version": 1,
            "provider": selected_provider,
            "command_executable": command[0],
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": time.monotonic() - started,
            "child_exit_code": exit_code,
            "timed_out": timed_out,
            "forwarded_signal": forwarded_signal,
            "runtime": runtime_snapshot(diagnostics_path),
        }
        write_json_atomic(diagnostics_path / "supervisor.json", supervisor_state)
        if upload_diagnostics is not None:
            try:
                diagnostics_upload = upload_diagnostics(diagnostics_path)
            except BaseException as error:
                diagnostics_upload_error = error
        receipt = {
            "schema_version": 1,
            "provider": selected_provider,
            "command_executable": command[0],
            "command_sha256": sha256_bytes(canonical_json_bytes(list(command))),
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": time.monotonic() - started,
            "max_runtime_seconds": max_runtime_seconds,
            "checkpoint_grace_seconds": checkpoint_grace_seconds,
            "timed_out": timed_out,
            "forwarded_signal": forwarded_signal,
            "child_exit_code": exit_code,
            "terminal_error": (
                f"{type(terminal_error).__name__}: {terminal_error}"
                if terminal_error is not None
                else None
            ),
            "diagnostics": diagnostics_upload,
            "diagnostics_upload_error": (
                f"{type(diagnostics_upload_error).__name__}: {diagnostics_upload_error}"
                if diagnostics_upload_error is not None
                else None
            ),
            "destruction_required": selected_provider != "none",
        }
        try:
            write_json_atomic(receipt_path, receipt)
            if upload_receipt is not None:
                upload_receipt(receipt_path)
        finally:
            if selected_provider != "none":
                (destroy or destroy_current_resource)(selected_provider)
    if terminal_error is not None:
        raise terminal_error
    if exit_code is None:
        raise RuntimeError("supervised process ended without an exit code")
    return 124 if timed_out else exit_code


def destroy_current_resource(provider: str) -> None:
    if provider == "runpod":
        resource_id = _required_environment("RUNPOD_POD_ID")
        api_key = _required_environment("RUNPOD_API_KEY")
        url = f"https://rest.runpod.io/v1/pods/{resource_id}"
    elif provider == "vast":
        resource_id = _required_environment("CONTAINER_ID")
        api_key = _required_environment("CONTAINER_API_KEY")
        url = f"https://console.vast.ai/api/v0/instances/{resource_id}/"
    else:
        raise ValueError(f"unsupported destruction provider: {provider}")
    try:
        response = httpx.delete(
            url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        if response.status_code not in {200, 202, 204, 404}:
            raise RuntimeError(f"{provider} destruction returned HTTP {response.status_code}")
    except httpx.TransportError:
        # Teardown can remove networking before the accepted response arrives.
        # The independent lease reaper remains authoritative in this case.
        return


def _select_provider(provider: Provider) -> Literal["runpod", "vast", "none"]:
    if provider != "auto":
        return cast(Literal["runpod", "vast", "none"], provider)
    detected: list[Literal["runpod", "vast"]] = []
    if os.environ.get("RUNPOD_POD_ID"):
        detected.append("runpod")
    if os.environ.get("CONTAINER_ID"):
        detected.append("vast")
    if len(detected) != 1:
        raise RuntimeError(
            "provider auto-detection requires exactly one provider resource identity"
        )
    return detected[0]


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required provider environment variable is missing: {name}")
    return value


def _start_tee(
    source: Any,
    capture: Any,
    mirror: Any,
    errors: list[BaseException],
) -> threading.Thread:
    def copy() -> None:
        nonlocal mirror
        capture_failed = False
        while chunk := source.read(1024 * 1024):
            if not capture_failed:
                try:
                    capture.write(chunk)
                    capture.flush()
                except BaseException as error:
                    errors.append(error)
                    capture_failed = True
            if mirror is not None:
                try:
                    mirror.write(chunk)
                    mirror.flush()
                except (BrokenPipeError, OSError):
                    mirror = None

    thread = threading.Thread(target=copy, daemon=True)
    thread.start()
    return thread
