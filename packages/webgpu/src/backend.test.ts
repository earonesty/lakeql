import {
  and,
  batchFromVectors,
  between,
  type Expr,
  eq,
  gt,
  isIn,
  isNull,
  type LakeqlError,
  or,
  type PhysicalFragment,
  physicalInputFromBatch,
  predicateSelection,
} from "lakeql-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { create, globals } from "webgpu";
import { WebGpuPhysicalBackend } from "./backend.js";
import type { WebGpuRuntime } from "./runtime.js";

describe("WebGpuPhysicalBackend with Dawn", () => {
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
    backend = new WebGpuPhysicalBackend(() => runtime);
  });

  afterAll(() => backend.close());

  it("executes nullable multi-shape predicates with CPU-equivalent three-valued logic", async () => {
    const batch = batchFromVectors({
      count: {
        type: "i32",
        values: Int32Array.of(1, 2, 3, 4, 5, 6),
        valid: Uint8Array.of(1, 1, 0, 1, 1, 1),
      },
      group: { type: "u32", values: Uint32Array.of(1, 2, 2, 3, 1, 2) },
      active: { type: "bool", values: Uint8Array.of(1, 1, 1, 0, 1, 0) },
      score: { type: "f32", values: Float32Array.of(0.5, 1, 2, 3, 4, 5) },
    });
    const predicate = or(
      and(gt("count", 1), isIn("group", [1, 2]), eq("active", true)),
      and(isNull("count"), between("score", 1, 2)),
    );
    const result = await execute(backend, batch, predicate);

    expect(result.output).toEqual({
      kind: "selection",
      selection: predicateSelection(batch, predicate),
    });
    expect(result.metrics).toMatchObject({
      backendId: "webgpu",
      inputRows: 6,
      selectedRows: 3,
      outputRows: 3,
      uploadBytes: 192,
      readbackBytes: 24,
      dispatches: 1,
      replayed: false,
    });
  });

  it("dispatches more than one workgroup and reuses the compiled pipeline", async () => {
    const rowCount = 1025;
    const values = new Uint32Array(rowCount);
    for (let index = 0; index < rowCount; index += 1) values[index] = index;
    const batch = batchFromVectors({ value: { type: "u32", values } });
    const predicate = between("value", 100, 999);

    const first = await execute(backend, batch, predicate);
    const second = await execute(backend, batch, predicate);
    expect(first.output).toEqual(second.output);
    expect(first.output.kind === "selection" && first.output.selection.reduce(sum)).toBe(900);
  });

  it("handles empty batches without dispatching", async () => {
    const batch = batchFromVectors({ value: { type: "u32", values: new Uint32Array() } });
    const result = await execute(backend, batch, eq("value", 1));
    expect(result.output).toEqual({ kind: "selection", selection: new Uint8Array() });
    expect(result.metrics.dispatches).toBe(0);
  });

  it("rejects physical transfer sizes against planning and execution budgets", async () => {
    const batch = batchFromVectors({
      value: { type: "u32", values: Uint32Array.of(1, 2, 3, 4) },
    });
    const target = fragment(batch, eq("value", 2));
    const assessment = backend.assess(target, {
      budget: { maxAcceleratorUploadBytes: 31 },
    });
    expect(assessment.supported).toBe(false);
    expect(assessment.reasons).toContainEqual(
      expect.objectContaining({
        code: "budget",
        details: expect.objectContaining({
          resource: "accelerator upload bytes",
          actual: 32,
          limit: 31,
        }),
      }),
    );

    const compiled = await backend.compile(target);
    await expect(
      backend.execute(
        compiled,
        { kind: "batch", batch },
        { budget: { maxAcceleratorUploadBytes: 31 } },
      ),
    ).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
  });

  it("honors cancellation before acquiring or submitting device work", async () => {
    const batch = batchFromVectors({
      value: { type: "u32", values: Uint32Array.of(1, 2) },
    });
    const target = fragment(batch, eq("value", 1));
    const compiled = await backend.compile(target);
    const controller = new AbortController();
    controller.abort("stop");
    await expect(
      backend.execute(
        compiled,
        { kind: "batch", batch },
        { budget: { signal: controller.signal } },
      ),
    ).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_ABORTED",
    });
  });

  it("executes u8 inputs and enforces output row budgets after readback", async () => {
    const batch = batchFromVectors({
      value: { type: "u8", values: Uint8Array.of(1, 2, 2, 3) },
    });
    const target = fragment(batch, eq("value", 2));
    const compiled = await backend.compile(target);
    await expect(
      backend.execute(compiled, { kind: "batch", batch }, { budget: { maxOutputRows: 1 } }),
    ).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: expect.objectContaining({ resource: "output rows", actual: 2, limit: 1 }),
    });
  });
});

describe("WebGpuPhysicalBackend validation", () => {
  const provider = () => {
    throw new Error("not acquired during validation");
  };

  it.each([
    [{ maxInputRows: 0 }, "row and storage limits"],
    [{ maxStorageBufferBytes: -1 }, "row and storage limits"],
    [{ maxResidentBytes: -1 }, "resident byte capacity"],
    [{ workgroupSize: 64 }, "workgroup size"],
    [{ cost: { fixedMs: 0 } }, "cost parameter fixedMs"],
  ])("rejects invalid options %j", (options, message) => {
    expect(() => new WebGpuPhysicalBackend(provider, options)).toThrow(message);
  });

  it("reports unsupported fragments and configured row limits", async () => {
    const backend = new WebGpuPhysicalBackend(provider, { maxInputRows: 1 });
    const batch = batchFromVectors({
      name: { type: "utf8", values: ["a", "b"] },
    });
    const target = fragment(batch, eq("name", "a"));
    const assessment = backend.assess(target, {});
    expect(assessment.supported).toBe(false);
    expect(assessment.reasons.map((reason) => reason.code)).toContain("operator");
    await expect(backend.compile(target)).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
    });
    backend.close();
  });

  it("requires immutable residency identities and rejects caching after close", async () => {
    const backend = new WebGpuPhysicalBackend(provider, { maxResidentBytes: 64 });
    const block = {
      rowCount: 1,
      dimensions: 1,
      vectors: Float32Array.of(1),
      rowIdsLow: Uint32Array.of(1),
      rowIdsHigh: Uint32Array.of(0),
    };
    await expect(
      backend.cacheVectorCandidates("vectors", block, { sourceIdentity: "" }),
    ).rejects.toMatchObject<LakeqlError>({ code: "LAKEQL_VALIDATION_ERROR" });
    backend.close();
    await expect(
      backend.cacheVectorCandidates("vectors", block, { sourceIdentity: "snapshot:a" }),
    ).rejects.toMatchObject<LakeqlError>({
      code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
    });
  });
});

function execute(
  backend: WebGpuPhysicalBackend,
  batch: Parameters<typeof physicalInputFromBatch>[0],
  predicate: Expr,
) {
  const target = fragment(batch, predicate);
  return backend.compile(target).then((compiled) =>
    backend.execute(compiled, {
      kind: "batch",
      batch,
    }),
  );
}

function fragment(
  batch: Parameters<typeof physicalInputFromBatch>[0],
  predicate: Expr,
): PhysicalFragment {
  return {
    id: `webgpu-test-${batch.rowCount}-${JSON.stringify(predicate)}`,
    input: physicalInputFromBatch(batch),
    operators: [{ kind: "select", predicate }],
    output: { kind: "selection" },
    estimates: {
      rowCount: batch.rowCount,
      inputBytes: Object.values(batch.columns).reduce(
        (bytes, vector) => bytes + ("values" in vector ? vector.values.byteLength : 0),
        0,
      ),
      outputBytes: batch.rowCount,
      dispatchCount: batch.rowCount === 0 ? 0 : 1,
    },
  };
}

function sum(total: number, value: number): number {
  return total + value;
}
