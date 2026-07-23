import {
  CpuPhysicalBackend,
  type PhysicalFragment,
  type PhysicalFragmentInput,
  type PhysicalResidentVectorCandidateInput,
  type PhysicalVectorCandidateBlock,
  type PhysicalVectorMetric,
} from "lakeql-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { create, globals } from "webgpu";
import { WebGpuPhysicalBackend } from "./backend.js";
import type { WebGpuRuntime } from "./runtime.js";
import { compileWebGpuVectorTopK, webGpuVectorMetricCode } from "./vector.js";

describe("WebGPU bounded vector top-k with Dawn", () => {
  let backend: WebGpuPhysicalBackend;
  let gpu: GPU;

  beforeAll(() => {
    gpu = create([]);
    const constants = globals as unknown as {
      GPUBufferUsage: typeof GPUBufferUsage;
      GPUMapMode: typeof GPUMapMode;
    };
    const runtime: WebGpuRuntime = {
      gpu,
      constants: {
        bufferUsage: constants.GPUBufferUsage,
        mapMode: constants.GPUMapMode,
      },
    };
    backend = new WebGpuPhysicalBackend(() => runtime, { maxResidentBytes: 1024 * 1024 });
  });

  afterAll(() => backend.close());

  it.each([
    "dot",
    "l2",
    "cosine-distance",
  ] satisfies PhysicalVectorMetric[])("matches the CPU f32 contract across tiles for %s", async (metric) => {
    const block = candidateBlock(2051, 8);
    const target = fragment(block, metric, [1, 0.5, -0.25, 0, 0.75, -1, 0.125, 0.25], 12);
    const cpu = new CpuPhysicalBackend();
    const [actual, expected] = await Promise.all([
      execute(backend, target, block),
      execute(cpu, target, block),
    ]);
    expect(actual.output).toEqual(expected.output);
    expect(actual.metrics).toMatchObject({
      inputRows: 2051,
      selectedRows: 2037,
      outputRows: 12,
      dispatches: 1,
    });
    expect(actual.metrics.readbackBytes).toBe(3 * 12 * 5 * 4);
  });

  it("uses parameterized queries with one reusable shader", async () => {
    const block = candidateBlock(64, 2);
    const left = fragment(block, "dot", [1, 0], 4);
    const right = fragment(block, "dot", [0, 1], 4);
    const first = await execute(backend, left, block);
    const second = await execute(backend, right, block);
    expect(first.output).not.toEqual(second.output);
    expect(backend.assess(right, {}).compiledResident).toBe(true);
  });

  it("matches CPU cosine semantics for zero query and candidate vectors", async () => {
    const block: PhysicalVectorCandidateBlock = {
      rowCount: 3,
      dimensions: 2,
      vectors: Float32Array.of(0, 0, 1, 0, 0, 1),
      rowIdsLow: Uint32Array.of(1, 2, 3),
      rowIdsHigh: Uint32Array.of(0, 0, 0),
    };
    for (const query of [
      [0, 0],
      [1, 0],
    ]) {
      const target = fragment(block, "cosine-distance", query, 3);
      const [actual, expected] = await Promise.all([
        execute(backend, target, block),
        execute(new CpuPhysicalBackend(), target, block),
      ]);
      expect(actual.output).toEqual(expected.output);
    }
  });

  it("reuses immutable resident candidates without charging query uploads for them", async () => {
    const block = candidateBlock(2051, 8);
    const lease = await backend.cacheVectorCandidates("vectors:resident", block, {
      sourceIdentity: "vectors:snapshot-7",
    });
    try {
      const target = residentFragment(
        lease.descriptor,
        block,
        "dot",
        [1, 0.5, -0.25, 0, 0.75, -1, 0.125, 0.25],
        12,
      );
      const actual = await executeInput(backend, target, lease.input);
      const expected = await execute(
        new CpuPhysicalBackend(),
        fragment(block, "dot", [1, 0.5, -0.25, 0, 0.75, -1, 0.125, 0.25], 12),
        block,
      );
      expect(actual.output).toEqual(expected.output);
      expect(actual.metrics).toMatchObject({
        inputRows: 2051,
        selectedRows: 2037,
        outputRows: 12,
        uploadBytes: 56,
      });
      expect(backend.assess(target, {})).toMatchObject({
        supported: true,
        inputResident: true,
      });
    } finally {
      lease.release();
    }
  });

  it("shares matching leases and rejects changed cache identity", async () => {
    const block = candidateBlock(8, 2);
    const [first, second] = await Promise.all([
      backend.cacheVectorCandidates("vectors:shared", block, {
        sourceIdentity: "snapshot:a",
      }),
      backend.cacheVectorCandidates("vectors:shared", block, {
        sourceIdentity: "snapshot:a",
      }),
    ]);
    expect(second.descriptor).toEqual(first.descriptor);
    await expect(
      backend.cacheVectorCandidates("vectors:shared", block, {
        sourceIdentity: "snapshot:b",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    first.release();
    first.release();
    second.release();
  });

  it("handles zero candidates and zero limit without dispatch", async () => {
    const empty = candidateBlock(0, 2);
    const emptyResult = await execute(backend, fragment(empty, "l2", [0, 0], 4), empty);
    expect(emptyResult.output).toEqual({
      kind: "vector-candidates",
      candidates: {
        rowIdsLow: new Uint32Array(),
        rowIdsHigh: new Uint32Array(),
        scores: new Float32Array(),
        sourceIndices: new Uint32Array(),
      },
    });
    expect(emptyResult.metrics.dispatches).toBe(0);

    const block = candidateBlock(4, 2);
    const zero = await execute(backend, fragment(block, "dot", [1, 0], 0), block);
    expect(zero.output.kind === "vector-candidates" && zero.output.candidates.scores).toHaveLength(
      0,
    );
    expect(zero.metrics.dispatches).toBe(0);
  });

  it("evicts only released entries and rejects stale descriptors", async () => {
    const constants = globals as unknown as {
      GPUBufferUsage: typeof GPUBufferUsage;
      GPUMapMode: typeof GPUMapMode;
    };
    const local = new WebGpuPhysicalBackend(
      () => ({
        gpu,
        constants: {
          bufferUsage: constants.GPUBufferUsage,
          mapMode: constants.GPUMapMode,
        },
      }),
      { maxResidentBytes: 80 },
    );
    const block = candidateBlock(4, 2);
    try {
      const first = await local.cacheVectorCandidates("vectors:first", block, {
        sourceIdentity: "snapshot:first",
      });
      await expect(
        local.cacheVectorCandidates("vectors:blocked", block, {
          sourceIdentity: "snapshot:blocked",
        }),
      ).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { resource: "resident accelerator bytes" },
      });
      first.release();
      const second = await local.cacheVectorCandidates("vectors:second", block, {
        sourceIdentity: "snapshot:second",
      });
      expect(
        local.assess(residentFragment(first.descriptor, block, "dot", [1, 0], 2), {}).supported,
      ).toBe(false);
      await expect(
        local.compile(residentFragment(first.descriptor, block, "dot", [1, 0], 2)),
      ).rejects.toMatchObject({ code: "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED" });
      second.release();
    } finally {
      local.close();
    }
  });
});

describe("compileWebGpuVectorTopK", () => {
  const block = candidateBlock(2, 2);

  it("enforces the portable bounded top-k shape", () => {
    expect(compileWebGpuVectorTopK(fragment(block, "dot", [1, 0], 32)).supported).toBe(true);
    expect(compileWebGpuVectorTopK(fragment(block, "dot", [1, 0], 33))).toEqual({
      supported: false,
      reason: "WebGPU bounded top-k supports at most 32 candidates",
    });
    expect(
      compileWebGpuVectorTopK({
        ...fragment(block, "dot", [1, 0], 2),
        output: { kind: "selection" },
      }),
    ).toEqual({
      supported: false,
      reason: expect.stringContaining("vector-candidates input"),
    });
  });

  it("encodes every metric explicitly", () => {
    expect(webGpuVectorMetricCode("dot")).toBe(0);
    expect(webGpuVectorMetricCode("l2")).toBe(1);
    expect(webGpuVectorMetricCode("cosine-distance")).toBe(2);
  });
});

async function execute(
  backend: WebGpuPhysicalBackend | CpuPhysicalBackend,
  target: PhysicalFragment,
  block: PhysicalVectorCandidateBlock,
) {
  const compiled = await backend.compile(target);
  return backend.execute(compiled, {
    kind: "vector-candidates",
    block,
    sourceIdentity: "vectors:test",
  });
}

async function executeInput(
  backend: WebGpuPhysicalBackend,
  target: PhysicalFragment,
  input: PhysicalFragmentInput,
) {
  const compiled = await backend.compile(target);
  return backend.execute(compiled, input);
}

function fragment(
  block: PhysicalVectorCandidateBlock,
  metric: PhysicalVectorMetric,
  query: number[],
  limit: number,
): PhysicalFragment {
  return {
    id: `vector-${metric}-${limit}-${query.join(",")}`,
    input: {
      kind: "vector-candidates",
      rowCount: block.rowCount,
      dimensions: block.dimensions,
      encoding: "f32",
      sourceIdentity: "vectors:test",
    },
    operators: [
      { kind: "vector-distance", query, metric },
      { kind: "bounded-top-k", limit },
    ],
    output: { kind: "vector-candidates" },
    estimates: {
      rowCount: block.rowCount,
      inputBytes:
        block.vectors.byteLength +
        block.rowIdsLow.byteLength +
        block.rowIdsHigh.byteLength +
        (block.valid?.byteLength ?? 0),
      outputBytes: limit * 16,
      dispatchCount: block.rowCount === 0 || limit === 0 ? 0 : 1,
    },
  };
}

function residentFragment(
  input: PhysicalResidentVectorCandidateInput,
  block: PhysicalVectorCandidateBlock,
  metric: PhysicalVectorMetric,
  query: number[],
  limit: number,
): PhysicalFragment {
  return {
    ...fragment(block, metric, query, limit),
    input,
  };
}

function candidateBlock(rowCount: number, dimensions: number): PhysicalVectorCandidateBlock {
  const vectors = new Float32Array(rowCount * dimensions);
  const rowIdsLow = new Uint32Array(rowCount);
  const rowIdsHigh = new Uint32Array(rowCount);
  const valid = new Uint8Array(rowCount);
  for (let row = 0; row < rowCount; row += 1) {
    rowIdsLow[row] = 0xffff_0000 + row;
    rowIdsHigh[row] = 7 + (row % 3);
    valid[row] = row % 157 === 0 ? 0 : 1;
    for (let dimension = 0; dimension < dimensions; dimension += 1) {
      vectors[row * dimensions + dimension] = Math.fround(
        ((row * 17 + dimension * 13) % 257) / 128 - 1,
      );
    }
  }
  return { rowCount, dimensions, vectors, rowIdsLow, rowIdsHigh, valid };
}
