# lakeql-webgpu

`lakeql-webgpu` installs a WebGPU implementation of LakeQL's accelerator-neutral
physical execution contract. It has no native runtime dependency and does not
read or mutate browser globals.

The current backend executes bounded selection and fused reduction fragments
over `bool`, `u8`, `i32`, `u32`, and `f32` vectors. Reductions support exact
counts and order-preserving `min`/`max` partials; grouped reductions support one
scalar key and an explicit limit of up to 32 groups. Exact `f32` candidate
blocks support dot product, squared L2 distance, cosine distance, paired 64-bit
row-ID words, and stable tiled top-k up to 32 candidates. It preserves LakeQL
null semantics, rejects numeric literals that WebGPU cannot compare with
CPU-equivalent precision, packs input columns into a fixed binding layout,
enforces accelerator budgets, caches compiled pipelines by device generation,
and destroys transient GPU resources deterministically.

<!-- source: examples/browser.ts -->
```ts
import { createLake, gt, httpStore } from "lakeql/fetch";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";

export async function queryScores() {
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
  });

  try {
    return await lake
      .path("scores.parquet")
      .select(["item_id", "score"])
      .where(gt("score", 0.5))
      .toArray();
  } finally {
    webgpu.close();
  }
}
```

Repeated vector queries can keep an immutable candidate block on the device.
The cache has an explicit byte capacity, uses reference-counted leases and LRU
eviction, and binds every descriptor to both an immutable source identity and
the current device generation:

```ts
const webgpu = new WebGpuPhysicalBackend(() => runtime, {
  maxResidentBytes: 256 * 1024 * 1024,
});
const resident = await webgpu.cacheVectorCandidates("catalog-embeddings", block, {
  sourceIdentity: snapshotId,
});

try {
  const fragment = {
    ...vectorFragment,
    input: resident.descriptor,
  };
  const result = await webgpu.execute(
    await webgpu.compile(fragment),
    resident.input,
  );
} finally {
  resident.release();
}
```

Reusing a cache key with a different snapshot identity or shape is a validation
error. Released entries remain reusable until the bounded cache needs space;
active leases are never evicted. Device loss invalidates descriptors, so stale
handles fail with a typed backend-unavailable error instead of reading unrelated
buffers.

The browser adapter takes `navigator`, `GPUBufferUsage`, and `GPUMapMode`
explicitly. A Worker or another WebGPU host can instead provide the same
`WebGpuRuntime` contract directly.

For Node tests and benchmarks, use Dawn's `webgpu` package as a development
dependency and pass `create([])` plus its exported constants. Dawn is not a
dependency of this package and is never loaded by query-time code.

## Execution and failure behavior

- `auto` lets LakeQL compare complete CPU and accelerator costs.
- `required` returns a typed placement error when the fragment or device is not
  supported.
- Device loss invalidates pipelines and advances the device generation.
- A device failure is replayable only when loss occurred before a result was
  published. LakeQL may then replay the bounded fragment on its CPU backend.
- Cancellation is checked before acquisition, before submission, after GPU
  synchronization, and before publishing mapped results.
- `close()` destroys the active device and clears compilation state.

Decoded batch input, CPU selection-mask output, aggregate and grouped-aggregate
snapshots, format-neutral exact-vector candidate blocks, and bounded resident
exact-vector blocks are supported. Multi-key and dictionary-key grouped
reductions, general resident columns, and quantized vector encodings remain
governed by the generic physical contract and are added as backend capabilities
as their semantic and resource contracts are implemented.

Row-producing queries place the selection fragment independently, then perform
projected row materialization on the host. Accelerator upload, readback, and
dispatch limits are spent across the complete query rather than reset for each
decoded batch. If unpublished work replays on CPU, explain statistics retain
the failed accelerator backend and its completed transfer stages.
