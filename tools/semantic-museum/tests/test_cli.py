from __future__ import annotations

import pytest

from semantic_museum.cli import _parser, _require_cloud_checkpointing


def test_plan_default_source_budget_covers_current_smithsonian_corpus() -> None:
    args = _parser().parse_args(
        [
            "plan",
            "--output",
            "plan",
            "--source",
            "source.jsonl",
            "--max-records",
            "1",
            "--max-total-image-bytes",
            "1",
        ]
    )

    assert args.max_source_bytes == 64 * 1024**3


def test_cloud_embedding_requires_remote_checkpointing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNPOD_POD_ID", "pod")
    with pytest.raises(RuntimeError, match="requires R2 bucket checkpointing"):
        _require_cloud_checkpointing(None)
    _require_cloud_checkpointing("museum")
