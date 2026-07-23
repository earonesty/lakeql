# lakeql-webgpu

`lakeql-webgpu` installs a WebGPU implementation of LakeQL's accelerator-neutral
physical execution contract. It has no native runtime dependency and does not
read or mutate browser globals.

The current backend executes bounded selection and fused reduction fragments
over `bool`, `u8`, `i32`, `u32`, and `f32` vectors. Reductions support exact
counts and order-preserving `min`/`max` partials. Exact `f32` candidate blocks
support dot product, squared L2 distance, cosine distance, paired 64-bit row-ID
words, and stable tiled top-k up to 32 candidates. It preserves LakeQL null
semantics, rejects numeric literals that WebGPU cannot compare with
CPU-equivalent precision, packs input columns into a fixed binding layout,
enforces accelerator budgets, caches compiled pipelines by device generation,
and destroys transient GPU resources deterministically.

```ts
import { gt, query } from "lakeql";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";

const runtime = browserWebGpuRuntime(
  navigator,
  { GPUBufferUsage, GPUMapMode },
);
const webgpu = new WebGpuPhysicalBackend(() => runtime);

const rows = await query(source, {
  where: gt("score", 0.5),
  physicalExecution: {
    backends: [webgpu],
    policy: "auto",
    replayOnCpu: true,
  },
});

webgpu.close();
```

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

Decoded batch input, CPU selection-mask output, aggregate snapshots, and
format-neutral exact-vector candidate blocks are supported. Grouped reductions,
resident columns, and quantized vector encodings remain governed by the generic
physical contract and are added as backend capabilities as their semantic and
resource contracts are implemented.
