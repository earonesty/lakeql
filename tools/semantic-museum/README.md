# Semantic Museum release harness

This harness turns bounded Smithsonian Open Access metadata inputs into
reproducible historical-image embedding releases. The same planner, image
budgets, MobileCLIP2 worker, receipts, Parquet writers, and release validator are
used for laptop fixtures and million-image builds.

## Install

```bash
cd tools/semantic-museum
uv sync --extra ml --extra test
```

## Build a bounded release

Plan records into stable hash buckets. Every byte and record limit is a hard
budget; reaching `--max-records` publishes the deterministic prefix without
marking the partially consumed source object complete.

```bash
uv run semantic-museum plan \
  --output work/plan \
  --source /path/to/smithsonian-shard.jsonl \
  --max-records 64 \
  --max-total-image-bytes 268435456 \
  --selection-policy prefix
```

For a full collection, pass the public root index. The resolved, sorted shard
URLs are frozen into the build manifest:

```bash
uv run semantic-museum plan \
  --output work/plan \
  --source-index https://smithsonian-open-access.s3-us-west-2.amazonaws.com/metadata/edan/index.txt \
  --max-records 1000000 \
  --max-source-bytes 68719476736 \
  --max-total-image-bytes 274877906944 \
  --selection-policy bottom-k
```

`bottom-k` scans every source object and retains the million lowest stable media
ID hashes, avoiding collection-order bias. The 64 GiB source budget is above the
current 48.23 GB Smithsonian metadata corpus while remaining a hard bound on
weekly source growth. `prefix` is an explicit fast-fixture policy; it stops once
the record budget is reached.

Process each bucket. A completed receipt makes the operation idempotent and
checksum validation catches partial or altered outputs.

```bash
uv run semantic-museum embed \
  --plan work/plan \
  --output work/release \
  --all \
  --device cuda \
  --batch-size 16
```

`--all` keeps one model instance loaded while processing all buckets. Distributed
workers use repeatable `--bucket <id>` arguments to claim an explicit subset.
Pass `--remote-bucket`, `--remote-prefix`, and `--endpoint-url` on ephemeral GPU
workers. Each bucket's embedding and failure Parquet are verified in R2 before
its receipt is uploaded as the commit marker. A replacement worker restores and
validates remote completed buckets before deciding what remains.

Use `--embedder deterministic` to exercise all I/O and release contracts without
loading the model. Production embeddings use the official
`apple/MobileCLIP2-S0` checkpoint and record its SHA-256 fingerprint.

Validate progress and finalize only after every planned record is represented by
either an embedding or a typed failure:

```bash
uv run semantic-museum status --plan work/plan --output work/release
uv run semantic-museum finalize --plan work/plan --output work/release
```

The finalized directory contains canonical metadata Parquet, fixed-size
512-float embedding Parquet, failure Parquet, receipts, and `release.json`.
`work/` is intentionally local build state; only finalized, validated objects
belong in the durable release prefix.

The CLI automatically loads the nearest `.env`. It accepts
`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and `R2_ENDPOINT_URL`, so repository
credentials do not need to be copied into AWS-named shell variables. Set
`R2_BUCKET` once to make the destination implicit. Publish a release with:

```bash
uv run semantic-museum publish \
  --output work/release
```

Publishing validates every local checksum, reuses matching immutable remote
objects, verifies every upload with `HEAD`, and replaces `current.json` only
after the complete release is durable.

Finalized releases are served read-only from
`https://museum-data.lakeql.com`. Browser access permits `GET`, `HEAD`, and
single-range requests from `lakeql.com`, `www.lakeql.com`, and the documented
local development origins. Writes remain authenticated.

## GPU lifecycle

Run dedicated RunPod or Vast workers under the lifecycle supervisor, with the
provider's native expiration configured independently:

```bash
uv run semantic-museum supervise \
  --provider auto \
  --max-runtime-seconds 14400 \
  --checkpoint-grace-seconds 30 \
  --receipt /workspace/terminal.json \
  --receipt-remote-bucket semantic-museum \
  --receipt-remote-key semantic-museum/leases/$LEASE_ID/terminal.json \
  --endpoint-url "$R2_ENDPOINT_URL" \
  -- semantic-museum embed --plan /workspace/plan --output /workspace/release \
     --bucket 00 --device cuda --remote-bucket semantic-museum \
     --endpoint-url "$R2_ENDPOINT_URL"
```

The supervisor owns the child process, forwards termination, enforces the hard
deadline, writes a terminal receipt, and permanently deletes the current Pod or
instance after every child exit. When configured, the terminal receipt is
checksum-verified in R2 before destruction. RunPod uses its injected
`RUNPOD_POD_ID`/`RUNPOD_API_KEY`; Vast uses
`CONTAINER_ID`/`CONTAINER_API_KEY`. Provider-native expiration and the
independent lease reaper described in the WebGPU plan remain required because no
in-container process can survive host loss or `SIGKILL`.

Cloud embedding refuses to start without `R2_BUCKET` or `--remote-bucket`.
Embedding and failure Parquet are uploaded and verified after every completed
bucket; the receipt is uploaded last as its commit marker. With the full build's
default 8-bit partitioning, one million records produce 256 independently
resumable buckets—about 3.3 minutes each at the measured laptop reference rate.
An interruption can therefore discard only the active bucket, while a
replacement worker restores committed receipts and continues.

## Fixture tiers

Use stable source-object ordering and these record budgets:

- 8 records: command and model-loading smoke test
- 64 records: correctness and restart conformance
- 512 records: laptop throughput and memory measurement
- 512–1,000 records: validate the cheapest eligible provider offer before the
  million-image build

Increase `bucket_bits` for distributed work. Each bucket remains an immutable,
independently retryable worker input. Use zero bits for a single-bucket bounded
fixture and 8–12 bits for a distributed full build, chosen from measured shard
duration and output size.

The first local CUDA gate is recorded in
[`benchmarks/2026-07-23-rtx-3050-ti.json`](benchmarks/2026-07-23-rtx-3050-ti.json):
42 real SAAM thumbnails, zero failures, and 19.60 end-to-end images/second at
batch size 16 on an RTX 3050 Ti Laptop GPU. This validates the code path. The
million-image runner should be the cheapest eligible interruptible RunPod or
Vast offer available at launch time. The bounded provider calibration is a
rejection gate for poor bandwidth or reliability, not a multi-GPU bakeoff.
