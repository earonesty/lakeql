import { create, globals } from "webgpu";
import {
  batchFromVectors,
  CpuPhysicalBackend,
  gte,
  physicalInputFromBatch,
} from "../packages/core/dist/index.js";
import { WebGpuPhysicalBackend } from "../packages/webgpu/dist/index.js";

const rows = positiveInteger(process.env.LAKEQL_WEBGPU_ROWS, 1_000_000);
const warmRuns = positiveInteger(process.env.LAKEQL_WEBGPU_WARM_RUNS, 8);
const ids = new Uint32Array(rows);
const scores = new Float32Array(rows);
const valid = new Uint8Array(rows);
for (let index = 0; index < rows; index += 1) {
  ids[index] = index;
  scores[index] = (index % 1024) / 1024;
  valid[index] = index % 97 === 0 ? 0 : 1;
}
const batch = batchFromVectors({
  id: { type: "u32", values: ids },
  score: { type: "f32", values: scores, valid },
});
const fragment = {
  id: `webgpu-benchmark-${rows}`,
  input: physicalInputFromBatch(batch),
  operators: [
    { kind: "select", predicate: gte("score", 0.5) },
    {
      kind: "reduce",
      aggregates: {
        rows: { op: "count" },
        values: { op: "count", column: "score" },
        firstId: { op: "min", column: "id" },
        highScore: { op: "max", column: "score" },
      },
    },
  ],
  output: { kind: "aggregate-snapshot" },
  estimates: {
    rowCount: rows,
    inputBytes: ids.byteLength + scores.byteLength + valid.byteLength,
    outputBytes: 128,
    dispatchCount: 1,
  },
};

const gpu = create([]);
const runtime = {
  gpu,
  constants: {
    bufferUsage: globals.GPUBufferUsage,
    mapMode: globals.GPUMapMode,
  },
};
const webgpu = new WebGpuPhysicalBackend(() => runtime);
const cpu = new CpuPhysicalBackend();

try {
  const cpuCompiled = await cpu.compile(fragment);
  const gpuCompiled = await webgpu.compile(fragment);
  const input = { kind: "batch", batch };

  const cpuCold = await timed(() => cpu.execute(cpuCompiled, input, {}));
  const gpuCold = await timed(() => webgpu.execute(gpuCompiled, input, {}));
  assertSameOutput(cpuCold.value.output, gpuCold.value.output);

  const cpuWarm = await repeated(warmRuns, () => cpu.execute(cpuCompiled, input, {}));
  const gpuWarm = await repeated(warmRuns, () => webgpu.execute(gpuCompiled, input, {}));
  assertSameOutput(cpuWarm.last.output, gpuWarm.last.output);

  console.log(
    JSON.stringify(
      {
        benchmark: "fused selection + count/min/max",
        adapter: "Dawn Node development adapter",
        rows,
        warmRuns,
        note: "Dawn results validate kernels and cost shape; they are not browser performance claims.",
        cpu: {
          coldMs: cpuCold.elapsedMs,
          warmMedianMs: median(cpuWarm.times),
          metrics: cpuWarm.last.metrics,
        },
        webgpu: {
          coldMs: gpuCold.elapsedMs,
          warmMedianMs: median(gpuWarm.times),
          metrics: gpuWarm.last.metrics,
        },
        output: gpuWarm.last.output,
      },
      null,
      2,
    ),
  );
} finally {
  webgpu.close();
}

async function timed(operation) {
  const startedAt = performance.now();
  const value = await operation();
  return { value, elapsedMs: performance.now() - startedAt };
}

async function repeated(count, operation) {
  const times = [];
  let last;
  for (let index = 0; index < count; index += 1) {
    const result = await timed(operation);
    times.push(result.elapsedMs);
    last = result.value;
  }
  return { times, last };
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function assertSameOutput(left, right) {
  if (JSON.stringify(left) !== JSON.stringify(right)) {
    throw new Error(`CPU/WebGPU mismatch:\n${JSON.stringify(left)}\n${JSON.stringify(right)}`);
  }
}

function positiveInteger(value, fallback) {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer, received ${value}`);
  }
  return parsed;
}
