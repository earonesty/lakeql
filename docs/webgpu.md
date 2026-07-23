# WebGPU Execution

`lakeql-webgpu` is an optional physical execution backend for LakeQL queries.
It accelerates supported columnar fragments while preserving the CPU backend as
the semantic baseline and fallback. Applications opt in explicitly; CPU-only
bundles do not load WebGPU or a native GPU runtime.

## Install

```sh
npm install lakeql lakeql-webgpu
```

The browser must expose WebGPU. The adapter receives the runtime and constants
explicitly rather than reading or mutating globals:

```ts
import { createLake, gt, httpStore } from "lakeql/fetch";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";

if (navigator.gpu === undefined) {
  throw new Error("This browser does not expose WebGPU");
}

const runtime = browserWebGpuRuntime(navigator, { GPUBufferUsage, GPUMapMode });
const webgpu = new WebGpuPhysicalBackend(() => runtime);

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data/" }),
  physicalExecution: {
    backends: [webgpu],
    acceleratorPolicy: "auto",
    replayOnCpu: true,
  },
  budget: {
    maxAcceleratorMemoryBytes: 64 * 1024 * 1024,
    maxAcceleratorUploadBytes: 64 * 1024 * 1024,
    maxAcceleratorReadbackBytes: 4 * 1024 * 1024,
    maxAcceleratorDispatches: 32,
  },
});

try {
  const result = lake
    .path("scores.parquet")
    .select(["item_id", "score"])
    .where(gt("score", 0.5))
    .limit(100)
    .run();

  console.log(await result.toArray());
  console.log((await result.explain()).text);
} finally {
  webgpu.close();
}
```

Calling `close()` destroys the active device and clears generation-bound
compilation and residency state.

## Placement Policies

`acceleratorPolicy: "auto"` lets the physical planner compare complete CPU and
WebGPU costs. The estimate includes fixed setup, compilation, upload, compute,
synchronization, and readback rather than assuming a GPU is faster. A supported
fragment may therefore stay on CPU.

`acceleratorPolicy: "required"` rejects a fragment when no installed
accelerator can preserve its complete operator and vector-shape contract. Use
it for tests or applications where CPU placement is an error, not as a
performance switch.

With `replayOnCpu: true`, LakeQL may replay a bounded fragment only when the
backend reports that a failure happened before any result was published.
Unsupported fragments are handled during placement; they are not partially run
on WebGPU.

`result.explain()` reports the selected backend, physical operators,
executions, input/selected/output rows, transfer bytes, dispatches, elapsed
time, and whether CPU replay occurred.

## Supported Physical Shapes

| Fragment | Current WebGPU contract |
| --- | --- |
| Selection | Nullable predicates over `bool`, `u8`, `i32`, `u32`, and `f32` vectors |
| Reduction | Selection fused with exact `count`, `min`, and `max` partials |
| Grouped reduction | One non-dictionary scalar key, at most 32 groups, and at most 16 `count`/`min`/`max` aggregates |
| Vector scoring | Exact float32 dot, squared-L2, or cosine-distance scoring |
| Vector top-k | Stable ordering, paired 64-bit row-ID words, and `k <= 32` |
| Residency | Bounded immutable float32 candidate blocks tied to source identity and device generation |

LakeQL rejects numeric literals that WebGPU cannot compare with CPU-equivalent
precision. Unsigned 64-bit vectors remain on CPU unless an accelerator
advertises an exact representation.

General resident columns, dictionary-key grouping, `sum`/`average`, joins,
window functions, general sorting, quantized vector encodings, and
render-resident output are not currently WebGPU capabilities. With `auto`, the
planner retains those fragments on CPU; with `required`, placement fails with a
typed error.

## Budgets, Cancellation, And Device Loss

Accelerator work is governed by:

- `maxAcceleratorMemoryBytes`;
- `maxAcceleratorUploadBytes`;
- `maxAcceleratorReadbackBytes`;
- `maxAcceleratorDispatches`;
- the query's ordinary row, memory, elapsed-time, and cancellation budgets.

The backend checks cancellation around acquisition, submission,
synchronization, mapping, and publication. Device loss invalidates compiled
pipelines and resident buffers by advancing a device generation. Stale resident
descriptors fail instead of reading unrelated allocations.

Every dispatch uses scoped WebGPU validation, out-of-memory, and internal error
handling. Transient buffers are destroyed deterministically.

## Reusing Immutable Vector Candidates

Repeated queries can retain a bounded candidate block on the device:

```ts
const webgpu = new WebGpuPhysicalBackend(() => runtime, {
  maxResidentBytes: 256 * 1024 * 1024,
});

const resident = await webgpu.cacheVectorCandidates("catalog-embeddings", block, {
  sourceIdentity: snapshotId,
});

try {
  const fragment = { ...vectorFragment, input: resident.descriptor };
  const result = await webgpu.execute(
    await webgpu.compile(fragment),
    resident.input,
  );
  console.log(result.output);
} finally {
  resident.release();
  webgpu.close();
}
```

The cache uses reference-counted leases and LRU eviction. Reusing a key with a
different immutable identity or shape is a validation error. Active leases are
never evicted.

## Runtime Support

- Browsers and Web Workers can use `browserWebGpuRuntime`.
- Another WebGPU host can provide the small `WebGpuRuntime` contract directly.
- Cloudflare/workerd packaging is tested without acquiring a device or
  introducing a native query-time dependency.
- Node.js development tests and benchmarks inject Dawn's `webgpu` package.
  Dawn is not a dependency of `lakeql-webgpu`.

WebGPU availability and limits remain host-specific. Applications should test
for a runtime before constructing the backend and retain a CPU-capable path
unless accelerator execution is a hard requirement.

## Benchmark Snapshot

The checked-in harness compares identical CPU and Dawn WebGPU fragments on the
development host:

| Fragment | Shape | CPU warm median | WebGPU warm median | WebGPU transfer |
| --- | ---: | ---: | ---: | ---: |
| Selection + count/min/max | 1,000,000 rows | 46.7 ms | 30.9 ms | 16.0 MB upload, 35.2 KB readback |
| Selection + 16-group count/min/max | 1,000,000 rows | 77.1 ms | 65.2 ms | 24.0 MB upload, 574.5 KB readback |
| Resident dot + top-16 | 100,000 × 128 | 33.9 ms | 54.0 ms | 536 B query upload, 125.1 KB readback |

The resident vector lane paid about 31.9 ms once to upload and retain 52.4 MB.
The resident Dawn query was still slower than CPU on that host, demonstrating
why `auto` uses end-to-end placement cost. These measurements validate kernel
behavior and transfer shape; they are not browser or hardware-performance
claims.

Reproduce them with:

```sh
pnpm bench:webgpu
pnpm bench:webgpu-vector
```

See the [compatibility matrix](./compatibility.md) for tested behavior, the
[runnable browser example](../examples/webgpu-browser.ts) for application
wiring, and the
[WebGPU contributor plan](./webgpu-accelerated-execution.md) for architecture
and future work.
