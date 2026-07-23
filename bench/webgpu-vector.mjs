import { create, globals } from "webgpu";
import { CpuPhysicalBackend } from "../packages/core/dist/index.js";
import { WebGpuPhysicalBackend } from "../packages/webgpu/dist/index.js";

const rows = positiveInteger(process.env.LAKEQL_WEBGPU_VECTOR_ROWS, 100_000);
const dimensions = positiveInteger(process.env.LAKEQL_WEBGPU_VECTOR_DIMENSIONS, 128);
const warmRuns = positiveInteger(process.env.LAKEQL_WEBGPU_VECTOR_WARM_RUNS, 5);
const limit = positiveInteger(process.env.LAKEQL_WEBGPU_VECTOR_K, 16);
if (limit > 32) throw new Error("LAKEQL_WEBGPU_VECTOR_K must be at most 32");

const vectors = new Float32Array(rows * dimensions);
const rowIdsLow = new Uint32Array(rows);
const rowIdsHigh = new Uint32Array(rows);
const query = new Array(dimensions);
for (let dimension = 0; dimension < dimensions; dimension += 1) {
  query[dimension] = Math.fround(((dimension * 19) % 127) / 64 - 1);
}
for (let row = 0; row < rows; row += 1) {
  rowIdsLow[row] = row;
  rowIdsHigh[row] = 1;
  for (let dimension = 0; dimension < dimensions; dimension += 1) {
    vectors[row * dimensions + dimension] = Math.fround(
      ((row * 17 + dimension * 13) % 257) / 128 - 1,
    );
  }
}
const block = { rowCount: rows, dimensions, vectors, rowIdsLow, rowIdsHigh };
const fragment = {
  id: `webgpu-vector-benchmark-${rows}-${dimensions}-${limit}`,
  input: {
    kind: "vector-candidates",
    rowCount: rows,
    dimensions,
    encoding: "f32",
    sourceIdentity: "benchmark:v1",
  },
  operators: [
    { kind: "vector-distance", query, metric: "dot" },
    { kind: "bounded-top-k", limit },
  ],
  output: { kind: "vector-candidates" },
  estimates: {
    rowCount: rows,
    inputBytes: vectors.byteLength + rowIdsLow.byteLength + rowIdsHigh.byteLength,
    outputBytes: limit * 16,
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
const residentBytes =
  vectors.byteLength + rowIdsLow.byteLength + rowIdsHigh.byteLength + rowIdsLow.byteLength;
const webgpu = new WebGpuPhysicalBackend(() => runtime, {
  maxResidentBytes: residentBytes,
});
const cpu = new CpuPhysicalBackend();
let resident;

try {
  const input = { kind: "vector-candidates", block, sourceIdentity: "benchmark:v1" };
  const cpuCompiled = await cpu.compile(fragment);
  const gpuCompiled = await webgpu.compile(fragment);
  const cpuCold = await timed(() => cpu.execute(cpuCompiled, input, {}));
  const gpuRawCold = await timed(() => webgpu.execute(gpuCompiled, input, {}));
  assertSameOutput(cpuCold.value.output, gpuRawCold.value.output);
  const residency = await timed(() =>
    webgpu.cacheVectorCandidates("benchmark:vectors", block, {
      sourceIdentity: "benchmark:v1",
    }),
  );
  resident = residency.value;
  const residentFragment = { ...fragment, input: resident.descriptor };
  const residentCompiled = await webgpu.compile(residentFragment);
  const cpuWarm = await repeated(warmRuns, () => cpu.execute(cpuCompiled, input, {}));
  const gpuWarm = await repeated(warmRuns, () =>
    webgpu.execute(residentCompiled, resident.input, {}),
  );
  assertSameOutput(cpuWarm.last.output, gpuWarm.last.output);
  console.log(
    JSON.stringify(
      {
        benchmark: "exact f32 dot distance + bounded top-k",
        adapter: "Dawn Node development adapter",
        rows,
        dimensions,
        limit,
        warmRuns,
        note: "Dawn results validate kernels and cost shape; they are not browser performance claims.",
        cpu: {
          coldMs: cpuCold.elapsedMs,
          warmMedianMs: median(cpuWarm.times),
          metrics: cpuWarm.last.metrics,
        },
        webgpu: {
          rawColdMs: gpuRawCold.elapsedMs,
          residencyUploadMs: residency.elapsedMs,
          residentWarmMedianMs: median(gpuWarm.times),
          residentQueryMetrics: gpuWarm.last.metrics,
          residentBytes,
        },
      },
      null,
      2,
    ),
  );
} finally {
  resident?.release();
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
    throw new Error(
      `CPU and WebGPU vector candidates differ:\n${JSON.stringify(left)}\n${JSON.stringify(right)}`,
    );
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
