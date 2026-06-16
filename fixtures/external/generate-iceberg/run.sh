#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../../.." && pwd)
IMAGE=${LAQL_ICEBERG_FIXTURE_IMAGE:-laql-iceberg-fixtures}
OUT_DIR=${LAQL_ICEBERG_FIXTURE_OUT:-$REPO_ROOT/fixtures/external/iceberg-reference}

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required to generate Iceberg reference fixtures, but the daemon is not reachable." >&2
  exit 1
fi

docker build -t "$IMAGE" "$SCRIPT_DIR"
mkdir -p "$OUT_DIR"
docker run --rm \
  -v "$OUT_DIR:/out" \
  "$IMAGE" \
  --output /out
