from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import httpx

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

    def forward(signum: int, _frame: Any) -> None:
        nonlocal forwarded_signal
        forwarded_signal = signum
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)

    try:
        process = subprocess.Popen(list(command), start_new_session=True)
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
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        receipt = {
            "schema_version": 1,
            "provider": selected_provider,
            "command_executable": command[0],
            "command_sha256": sha256_bytes(
                canonical_json_bytes(list(command))
            ),
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
            raise RuntimeError(
                f"{provider} destruction returned HTTP {response.status_code}"
            )
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
