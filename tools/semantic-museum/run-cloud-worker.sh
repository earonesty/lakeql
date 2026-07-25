#!/usr/bin/env bash
set -euo pipefail

: "${R2_BUCKET:?R2_BUCKET is required}"
: "${R2_ENDPOINT_URL:?R2_ENDPOINT_URL is required}"
: "${RUNPOD_POD_ID:?RUNPOD_POD_ID is required}"
: "${RUNPOD_API_KEY:?RUNPOD_API_KEY is required}"

export SEMANTIC_MUSEUM_JOB_ID="${SEMANTIC_MUSEUM_JOB_ID:-$RUNPOD_POD_ID}"
max_runtime_seconds="${SEMANTIC_MUSEUM_MAX_RUNTIME_SECONDS:-108000}"
checkpoint_grace_seconds="${SEMANTIC_MUSEUM_CHECKPOINT_GRACE_SECONDS:-120}"
work_directory="${SEMANTIC_MUSEUM_WORK_DIRECTORY:-/workspace/semantic-museum-work}"
receipt_path="${SEMANTIC_MUSEUM_RECEIPT_PATH:-/workspace/terminal.json}"
diagnostics_directory="${SEMANTIC_MUSEUM_DIAGNOSTICS_DIR:-/workspace/diagnostics}"
receipt_key="semantic-museum/leases/$SEMANTIC_MUSEUM_JOB_ID/terminal.json"

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

uv sync --extra ml

exec .venv/bin/semantic-museum supervise \
  --provider runpod \
  --max-runtime-seconds "$max_runtime_seconds" \
  --checkpoint-grace-seconds "$checkpoint_grace_seconds" \
  --receipt "$receipt_path" \
  --diagnostics-dir "$diagnostics_directory" \
  --receipt-remote-bucket "$R2_BUCKET" \
  --receipt-remote-key "$receipt_key" \
  --endpoint-url "$R2_ENDPOINT_URL" \
  -- \
  .venv/bin/semantic-museum cloud-job \
  --work "$work_directory" \
  --bucket "$R2_BUCKET" \
  --endpoint-url "$R2_ENDPOINT_URL" \
  --device cuda \
  --batch-size 32 \
  --download-concurrency 32 \
  --calibration-records 1000 \
  --max-records 1000000
