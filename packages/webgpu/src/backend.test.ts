import {
  and,
  batchFromVectors,
  between,
  CpuPhysicalBackend,
  type Expr,
  eq,
  gt,
  gte,
  isIn,
  isNull,
  Lake,
  type LakeqlError,
  MemoryBudget,
  MemoryScope,
  memoryStore,
  or,
  type PhysicalFragment,
  physicalInputFromBatch,
  planPhysicalFragment,
  predicateSelection,
  type ScanAdapter,
} from "lakeql-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { create, globals } from "webgpu";
import { dawnAdapterAvailable } from "../test/dawn.js";
import { WebGpuPhysicalBackend } from "./backend.js";
import type { WebGpuRuntime } from "./runtime.js";

describe.runIf(await dawnAdapterAvailable())("WebGpuPhysicalBackend with Dawn", () => {
  let backend: WebGpuPhysicalBackend;
  let gpu: GPU;
  let runtime: WebGpuRuntime;

  beforeAll(() => {
    gpu = create([]);
    const constants = globals as unknown as {
      GPUBufferUsage: typeof GPUBufferUsage;
      GPUMapMode: typeof GPUMapMode;
    };
    runtime = {
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

  it.each([
    "auto",
    "required",
  ] as const)("enforces scoped selection capability (%s)", async (policy) => {
    const store = memoryStore();
    await store.put("scores.parquet", Uint8Array.of(1));
    const batch = batchFromVectors({
      id: { type: "u32", values: Uint32Array.of(1, 2, 3) },
      score: { type: "f32", values: Float32Array.of(0.25, 0.75, 1) },
    });
    const scanner: ScanAdapter = {
      async *scan() {
        yield [];
      },
      async *scanVectorBatches() {
        yield { rowOffset: 0, batch };
      },
    };
    const lake = new Lake({
      store,
      scanner,
      physicalExecution: {
        backends: [backend],
        acceleratorPolicy: policy,
      },
    });

    const result = lake.path("scores.parquet").select(["id"]).where(gt("score", 0.5)).run();
    if (policy === "required") {
      await expect(result.toArray()).rejects.toMatchObject({
        code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
        details: {
          candidates: expect.arrayContaining([
            expect.objectContaining({
              backendId: "webgpu",
              reasons: expect.arrayContaining([
                expect.objectContaining({
                  message: "Backend does not support allocation scopes for this fragment",
                }),
              ]),
            }),
          ]),
        },
      });
    } else {
      await expect(result.toArray()).resolves.toEqual([{ id: 2 }, { id: 3 }]);
      expect((await result.explain()).json.physicalFragments).toEqual([
        expect.objectContaining({ backendId: "cpu", operators: ["select"], output: "selection" }),
      ]);
    }
  });

  it("admits selection backing and transfers only its mask", async () => {
    const batch = batchFromVectors({ value: { type: "u32", values: Uint32Array.of(1, 2, 3, 4) } });
    const target = fragment(batch, gt("value", 2));
    const compiled = await backend.compile(target);
    const run = async (capacity: number, preadmit = false) => {
      const pool = new MemoryBudget(capacity);
      const scope = new MemoryScope(pool);
      if (preadmit) scope.admit(capacity, "selection stage");
      try {
        const result = await backend.execute(
          compiled,
          { kind: "batch", batch },
          { allocationScope: scope },
        );
        expect(result.output).toEqual({ kind: "selection", selection: Uint8Array.of(0, 0, 1, 1) });
        expect(pool.usedBytes).toBe(4);
        return pool.peakBytes;
      } finally {
        scope.dispose();
        expect(pool.usedBytes).toBe(0);
      }
    };
    const peak = await run(65536);
    expect(peak).toBe(100);
    expect(await run(peak)).toBe(peak);
    expect(await run(peak, true)).toBe(peak);
    await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("evicts and recompiles real pipelines without changing selection", async () => {
    const local = new WebGpuPhysicalBackend(() => runtime, { maxPipelineCacheEntries: 2 });
    try {
      const batch = batchFromVectors({
        value: { type: "u32", values: Uint32Array.of(1, 2, 3, 4) },
      });
      const targets = [
        fragment(batch, gt("value", 1)),
        fragment(batch, gt("value", 2)),
        fragment(batch, gt("value", 3)),
      ] as const;
      for (const target of targets) {
        const result = await local.execute(await local.compile(target), { kind: "batch", batch });
        expect(result.output.kind).toBe("selection");
      }
      expect(local.assess(targets[0], {}).compiledResident).toBe(false);
      expect(local.assess(targets[1], {}).compiledResident).toBe(true);
      const result = await local.execute(await local.compile(targets[0]), {
        kind: "batch",
        batch,
      });
      expect(result.output).toEqual({ kind: "selection", selection: Uint8Array.of(0, 1, 1, 1) });
      expect(local.assess(targets[1], {}).compiledResident).toBe(false);
      expect(local.assess(targets[0], {}).compiledResident).toBe(true);
    } finally {
      local.close();
    }
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

  it("calibrates auto placement against the recorded fused-reduction benchmark shape", () => {
    const backend = new WebGpuPhysicalBackend(provider);
    const rowCount = 1_000_000;
    const target: PhysicalFragment = {
      id: "recorded-relational-benchmark",
      input: {
        kind: "batch",
        rowCount,
        columns: {
          id: { shape: "u32", nullable: false },
          score: { shape: "f32", nullable: true },
        },
      },
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
        rowCount,
        inputBytes: 9_000_000,
        outputBytes: 128,
        dispatchCount: 1,
      },
    };

    const plan = planPhysicalFragment(target, [new CpuPhysicalBackend(), backend]);
    expect(plan.backendId).toBe("webgpu");
    const candidate = plan.candidates.find(({ backendId }) => backendId === "webgpu");
    if (candidate === undefined) throw new Error("missing WebGPU candidate");
    const cost = candidate.assessment.cost;
    expect(cost.totalMs).toBeCloseTo(
      cost.inputConversionMs +
        cost.uploadMs +
        cost.compileMs +
        cost.computeMs +
        cost.synchronizationMs +
        cost.readbackMs +
        cost.outputConversionMs,
      10,
    );
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

  it("attaches completed transfer stages to replayable backend failures", async () => {
    const backend = new WebGpuPhysicalBackend(failingPipelineRuntime);
    const batch = batchFromVectors({
      value: { type: "u32", values: Uint32Array.of(1, 2, 3, 4) },
    });
    const target = fragment(batch, gt("value", 1));

    await expect(
      backend.execute(await backend.compile(target), { kind: "batch", batch }, {}),
    ).rejects.toMatchObject({
      code: "LAKEQL_PHYSICAL_BACKEND_FAILURE",
      attemptedMetrics: {
        backendId: "webgpu",
        uploadBytes: 32,
        readbackBytes: 0,
        dispatches: 0,
      },
    });
    backend.close();
  });
});

it.each([
  { createAt: 1, expected: [] },
  { createAt: 2, expected: [1] },
  { createAt: 3, expected: [1, 2] },
  { createAt: 4, expected: [1, 2, 3] },
  { uploadAt: 1, expected: [1] },
  { uploadAt: 2, expected: [1, 2] },
])("destroys every selection buffer after partial allocation (%j)", async (failure) => {
  const destroyed: number[] = [];
  const backend = new WebGpuPhysicalBackend(() =>
    failingPipelineRuntime({ ...failure, destroyed }),
  );
  try {
    const batch = batchFromVectors({ value: { type: "u32", values: Uint32Array.of(1, 2) } });
    await expect(execute(backend, batch, gt("value", 1))).rejects.toThrow();
    expect(destroyed.sort((a, b) => a - b)).toEqual(failure.expected);
  } finally {
    backend.close();
  }
});

it.each([
  "reduce",
  "vector",
  "resident",
] as const)("cleans partial %s buffer construction", async (kind) => {
  const count = kind === "reduce" ? 5 : kind === "vector" ? 8 : 4;
  const uploads = kind === "reduce" ? 3 : kind === "vector" ? 6 : 4;
  for (const operation of ["createAt", "uploadAt"] as const) {
    for (let index = 1; index <= (operation === "createAt" ? count : uploads); index++) {
      const destroyed: number[] = [];
      const backend = new WebGpuPhysicalBackend(
        () => failingPipelineRuntime({ [operation]: index, destroyed }),
        { maxResidentBytes: 1024 },
      );
      try {
        const block = {
          rowCount: 2,
          dimensions: 1,
          vectors: Float32Array.of(1, 2),
          rowIdsLow: Uint32Array.of(1, 2),
          rowIdsHigh: new Uint32Array(2),
        };
        if (kind === "resident") {
          await expect(
            backend.cacheVectorCandidates("test", block, { sourceIdentity: "test" }),
          ).rejects.toThrow();
        } else if (kind === "reduce") {
          const batch = batchFromVectors({ value: { type: "u32", values: Uint32Array.of(1, 2) } });
          const target = {
            ...fragment(batch, gt("value", 0)),
            operators: [{ kind: "reduce", aggregates: { first: { op: "min", column: "value" } } }],
            output: { kind: "aggregate-snapshot" },
          } satisfies PhysicalFragment;
          await expect(
            backend.execute(await backend.compile(target), { kind: "batch", batch }),
          ).rejects.toThrow();
        } else {
          const target: PhysicalFragment = {
            id: "vector-cleanup",
            input: {
              kind: "vector-candidates",
              rowCount: 2,
              dimensions: 1,
              encoding: "f32",
              sourceIdentity: "test",
            },
            operators: [
              { kind: "vector-distance", query: [1], metric: "dot" },
              { kind: "bounded-top-k", limit: 1 },
            ],
            output: { kind: "vector-candidates" },
            estimates: { rowCount: 2, inputBytes: 24, outputBytes: 16, dispatchCount: 1 },
          };
          await expect(
            backend.execute(await backend.compile(target), {
              kind: "vector-candidates",
              block,
              sourceIdentity: "test",
            }),
          ).rejects.toThrow();
        }
        expect(destroyed.sort((a, b) => a - b)).toEqual(
          Array.from({ length: operation === "createAt" ? index - 1 : index }, (_, i) => i + 1),
        );
      } finally {
        backend.close();
      }
    }
  }
});

it("pins selection backing until pending device work settles after scope disposal", async () => {
  let started!: () => void;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  let settle!: () => void;
  const pending = new Promise<void>((resolve) => {
    settle = resolve;
  });
  const destroyed: number[] = [];
  const backend = new WebGpuPhysicalBackend(() =>
    failingPipelineRuntime({
      destroyed,
      pipeline: async () => {
        started();
        await pending;
      },
    }),
  );
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  try {
    const batch = batchFromVectors({ value: { type: "u32", values: Uint32Array.of(1, 2, 3, 4) } });
    const compiled = await backend.compile(fragment(batch, gt("value", 1)));
    const work = backend.execute(compiled, { kind: "batch", batch }, { allocationScope: scope });
    const rejected = expect(work).rejects.toThrow();
    await ready;
    expect(pool.usedBytes).toBe(100);
    scope.dispose();
    expect(pool.usedBytes).toBe(100);
    expect(destroyed).toEqual([]);
    settle();
    await rejected;
    expect(destroyed).toEqual([1, 2, 3, 4]);
    expect(pool.usedBytes).toBe(0);
  } finally {
    settle();
    scope.dispose();
    backend.close();
  }
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

function failingPipelineRuntime(failure?: {
  createAt?: number;
  uploadAt?: number;
  destroyed: number[];
  pipeline?: () => Promise<void>;
}): WebGpuRuntime {
  let created = 0;
  let uploaded = 0;
  const device = {
    limits: {
      maxBufferSize: 1024 * 1024,
      maxStorageBufferBindingSize: 1024 * 1024,
    },
    lost: new Promise<GPUDeviceLostInfo>(() => {}),
    queue: {
      writeBuffer() {
        if (++uploaded === failure?.uploadAt) throw new Error("injected upload failure");
      },
    },
    destroy() {},
    pushErrorScope() {},
    async popErrorScope() {
      return null;
    },
    createBuffer(descriptor: GPUBufferDescriptor) {
      const id = ++created;
      if (id === failure?.createAt) throw new Error("injected allocation failure");
      const contents = new ArrayBuffer(Number(descriptor.size));
      return {
        destroy() {
          failure?.destroyed.push(id);
        },
        getMappedRange() {
          return contents;
        },
        unmap() {},
      } as unknown as GPUBuffer;
    },
    createShaderModule() {
      return {} as GPUShaderModule;
    },
    async createComputePipelineAsync() {
      await failure?.pipeline?.();
      throw new Error("device lost while compiling pipeline");
    },
  } as unknown as GPUDevice;
  const adapter = {
    async requestDevice() {
      return device;
    },
  } as unknown as GPUAdapter;
  return {
    gpu: {
      async requestAdapter() {
        return adapter;
      },
    } as unknown as GPU,
    constants: {
      bufferUsage: {
        MAP_READ: 1,
        COPY_SRC: 2,
        COPY_DST: 4,
        STORAGE: 8,
        UNIFORM: 16,
      },
      mapMode: { READ: 1 },
    },
  };
}
