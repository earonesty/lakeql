import { describe, expect, it } from "vitest";
import { batchFromColumns, batchFromVectors, materializeBatchRows } from "./batch.js";
import { col, eq, gt, mul } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import {
  type BackendAssessment,
  type BackendExecutionContext,
  type BackendPlanningContext,
  type CompiledPhysicalFragment,
  CpuPhysicalBackend,
  estimateBatchBytes,
  executePlannedPhysicalFragment,
  PhysicalBackendExecutionError,
  type PhysicalCapabilities,
  type PhysicalExecutionBackend,
  type PhysicalFragment,
  type PhysicalFragmentInput,
  type PhysicalFragmentResult,
  type PhysicalOutputValue,
  physicalInputFromBatch,
  planPhysicalFragment,
} from "./physical.js";
import { finalizeVectorAggregateStates, restoreVectorAggregateStates } from "./vector-aggregate.js";
import { finalizeVectorGroupByRows, restoreVectorGroupByState } from "./vector-group-by.js";

describe("physical execution", () => {
  it("describes exact vector shapes, nullability, dictionaries, and bytes", () => {
    const dictionary = batchFromColumns({ label: ["east", "west"] }).columns.label;
    if (dictionary === undefined) throw new Error("missing dictionary");
    const batch = batchFromVectors({
      score: {
        type: "f32",
        values: Float32Array.of(1, 2, 3),
        valid: Uint8Array.of(1, 0, 1),
      },
      region: {
        type: "dict",
        indices: Uint32Array.of(0, 1, 0),
        dictionary,
      },
    });

    expect(physicalInputFromBatch(batch, { sourceIdentity: "etag:abc" })).toEqual({
      kind: "batch",
      rowCount: 3,
      sourceIdentity: "etag:abc",
      columns: {
        score: { shape: "f32", nullable: true },
        region: {
          shape: "dict",
          nullable: false,
          dictionaryValueShape: { shape: "utf8", nullable: false },
        },
      },
    });
    expect(estimateBatchBytes(batch)).toBe(
      Float32Array.BYTES_PER_ELEMENT * 3 +
        Uint8Array.BYTES_PER_ELEMENT * 3 +
        Uint32Array.BYTES_PER_ELEMENT * 3 +
        ("east".length + "west".length) * 2,
    );
  });

  it("executes fused selection and projection with compact batch output", async () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      amount: [5, 10, 15, 20],
      region: ["west", "east", "east", "west"],
    });
    const fragment = testFragment(batch, {
      operators: [
        { kind: "select", predicate: gt("amount", 5) },
        { kind: "select", predicate: eq("region", "east") },
        {
          kind: "project",
          select: ["id"],
          projections: { doubled: mul(col("amount"), 2) },
        },
      ],
      output: { kind: "batch" },
    });
    const backend = new CpuPhysicalBackend();
    const compiled = await backend.compile(fragment);
    const result = await backend.execute(compiled, { kind: "batch", batch }, { now: clock() });

    expect(result.output.kind).toBe("batch");
    if (result.output.kind !== "batch") throw new Error("expected batch output");
    expect(materializeBatchRows(result.output.batch)).toEqual([
      { id: 2, doubled: 20 },
      { id: 3, doubled: 30 },
    ]);
    expect(result.metrics).toMatchObject({
      backendId: "cpu",
      inputRows: 4,
      outputRows: 2,
      uploadBytes: 0,
      readbackBytes: 0,
      dispatches: 0,
      replayed: false,
    });
  });

  it("returns reusable aggregate and grouped-aggregate snapshots", async () => {
    const batch = batchFromColumns({
      region: ["east", "west", "east", null],
      amount: [10, 20, 30, 40],
    });
    const backend = new CpuPhysicalBackend();
    const aggregateSpec = {
      rows: { op: "count" as const },
      total: { op: "sum" as const, column: "amount" },
    };
    const reduce = testFragment(batch, {
      operators: [
        { kind: "select", predicate: gt("amount", 10) },
        { kind: "reduce", aggregates: aggregateSpec },
      ],
      output: { kind: "aggregate-snapshot" },
    });
    const reduceResult = await backend.execute(
      await backend.compile(reduce),
      { kind: "batch", batch },
      {},
    );
    if (reduceResult.output.kind !== "aggregate-snapshot") {
      throw new Error("expected aggregate snapshot");
    }
    expect(
      finalizeVectorAggregateStates(restoreVectorAggregateStates(reduceResult.output.snapshot)),
    ).toEqual({ rows: 3, total: 90 });

    const grouped = testFragment(batch, {
      operators: [
        {
          kind: "grouped-reduce",
          keys: ["region"],
          aggregates: aggregateSpec,
          maxGroups: 4,
        },
      ],
      output: { kind: "grouped-aggregate-snapshot" },
    });
    const groupedResult = await backend.execute(
      await backend.compile(grouped),
      { kind: "batch", batch },
      {},
    );
    if (groupedResult.output.kind !== "grouped-aggregate-snapshot") {
      throw new Error("expected grouped aggregate snapshot");
    }
    expect(
      finalizeVectorGroupByRows(
        restoreVectorGroupByState(["region"], aggregateSpec, groupedResult.output.snapshot),
      ),
    ).toEqual([
      { region: "east", rows: 2, total: 40 },
      { region: "west", rows: 1, total: 20 },
      { region: null, rows: 1, total: 40 },
    ]);
  });

  it("executes exact vector distance and bounded top-k through the generic backend", async () => {
    const block = {
      rowCount: 4,
      dimensions: 2,
      vectors: Float32Array.of(1, 0, 0, 1, 1, 1, -1, 0),
      rowIdsLow: Uint32Array.of(10, 11, 12, 13),
      rowIdsHigh: Uint32Array.of(2, 2, 2, 2),
    };
    const fragment: PhysicalFragment = {
      id: "vector-candidates",
      input: {
        kind: "vector-candidates",
        rowCount: 4,
        dimensions: 2,
        encoding: "f32",
        sourceIdentity: "vectors:etag",
      },
      operators: [
        { kind: "vector-distance", query: [1, 0], metric: "dot" },
        { kind: "bounded-top-k", limit: 2 },
      ],
      output: { kind: "vector-candidates" },
      estimates: {
        rowCount: 4,
        inputBytes: 64,
        outputBytes: 32,
        dispatchCount: 1,
      },
    };
    const backend = new CpuPhysicalBackend();
    const result = await backend.execute(
      await backend.compile(fragment),
      { kind: "vector-candidates", block, sourceIdentity: "vectors:etag" },
      {},
    );
    expect(result.output).toEqual({
      kind: "vector-candidates",
      candidates: {
        rowIdsLow: Uint32Array.of(10, 12),
        rowIdsHigh: Uint32Array.of(2, 2),
        scores: Float32Array.of(1, 1),
        sourceIndices: Uint32Array.of(0, 2),
      },
    });
    expect(result.metrics).toMatchObject({
      inputRows: 4,
      selectedRows: 4,
      outputRows: 2,
      dispatches: 0,
    });

    for (const invalid of [
      {
        ...fragment,
        operators: [
          { kind: "vector-distance" as const, query: [0.1, 0], metric: "dot" as const },
          { kind: "bounded-top-k" as const, limit: 2 },
        ],
      },
      {
        ...fragment,
        operators: [
          { kind: "vector-distance" as const, query: [1, 0], metric: "dot" as const },
          { kind: "bounded-top-k" as const, limit: -1 },
        ],
      },
      {
        ...fragment,
        operators: [{ kind: "bounded-top-k" as const, limit: 2 }],
      },
    ]) {
      expect(backend.assess(invalid, {}).supported).toBe(false);
    }
    const compiled = await backend.compile(fragment);
    await expect(
      backend.execute(
        compiled,
        {
          kind: "vector-candidates",
          block: { ...block, dimensions: 1 },
          sourceIdentity: "vectors:etag",
        },
        {},
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      backend.execute(compiled, { kind: "vector-candidates", block, sourceIdentity: "wrong" }, {}),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      backend.execute(
        compiled,
        { kind: "vector-candidates", block, sourceIdentity: "vectors:etag" },
        { budget: { maxOutputRows: 1 } },
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      backend.execute(
        compiled,
        { kind: "vector-candidates", block, sourceIdentity: "vectors:etag" },
        { budget: { maxElapsedMs: 0 }, now: clock() },
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("executes stable order, top-k, selection, and index outputs", async () => {
    const batch = batchFromColumns({
      id: [1, 2, 3, 4],
      score: [20, 40, 10, 30],
    });
    const backend = new CpuPhysicalBackend();
    const ordered = testFragment(batch, {
      operators: [
        { kind: "select", predicate: gt("score", 10) },
        { kind: "order", orderBy: [{ column: "score", direction: "desc" }] },
        { kind: "top-k", orderBy: [{ column: "id", direction: "desc" }], limit: 2 },
      ],
      output: { kind: "batch" },
    });
    const orderedResult = await backend.execute(
      await backend.compile(ordered),
      { kind: "batch", batch },
      {},
    );
    if (orderedResult.output.kind !== "batch") throw new Error("expected batch");
    expect(materializeBatchRows(orderedResult.output.batch)).toEqual([
      { id: 4, score: 30 },
      { id: 2, score: 40 },
    ]);

    const selected = testFragment(batch, {
      operators: [{ kind: "select", predicate: gt("score", 20) }],
      output: { kind: "selection" },
    });
    const selectedResult = await backend.execute(
      await backend.compile(selected),
      { kind: "batch", batch },
      {},
    );
    expect(selectedResult.output).toEqual({
      kind: "selection",
      selection: Uint8Array.of(0, 1, 0, 1),
    });

    const indices = { ...selected, id: "indices", output: { kind: "indices" as const } };
    const indexResult = await backend.execute(
      await backend.compile(indices),
      { kind: "batch", batch },
      {},
    );
    expect(indexResult.output).toEqual({ kind: "indices", indices: Uint32Array.of(1, 3) });
  });

  it("validates fragment sequences, outputs, runtime shapes, identities, and cancellation", async () => {
    const batch = batchFromColumns({ id: [1, 2], amount: [10, 20] });
    const backend = new CpuPhysicalBackend();
    const invalid = testFragment(batch, {
      operators: [
        { kind: "reduce", aggregates: { rows: { op: "count" } } },
        { kind: "project", select: ["id"] },
      ],
      output: { kind: "batch" },
    });
    expect(backend.assess(invalid, {}).supported).toBe(false);
    await expect(backend.compile(invalid)).rejects.toMatchObject({
      code: "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
    });

    const fragment = testFragment(batch, {
      operators: [{ kind: "project", select: ["id"] }],
      output: { kind: "batch" },
      sourceIdentity: "snapshot-a",
    });
    const compiled = await backend.compile(fragment);
    await expect(
      backend.execute(compiled, { kind: "batch", batch, sourceIdentity: "snapshot-b" }, {}),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    await expect(
      backend.execute(compiled, { kind: "batch", batch: batchFromColumns({ id: [1] }) }, {}),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      backend.execute(
        compiled,
        {
          kind: "batch",
          batch: batchFromVectors({ id: { type: "i32", values: Int32Array.of(1, 2) } }),
        },
        {},
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });

    const controller = new AbortController();
    controller.abort("stop");
    await expect(
      backend.execute(
        compiled,
        { kind: "batch", batch, sourceIdentity: "snapshot-a" },
        {
          budget: { signal: controller.signal },
        },
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });

    const invalidTopK = testFragment(batch, {
      operators: [{ kind: "top-k", orderBy: [{ column: "id" }], limit: -1 }],
      output: { kind: "batch" },
    });
    await expect(backend.compile(invalidTopK)).rejects.toMatchObject({
      code: "LAKEQL_PHYSICAL_BACKEND_UNSUPPORTED",
    });

    const residentInput: PhysicalFragment = {
      ...fragment,
      input: {
        ...fragment.input,
        kind: "resident",
        backendId: "gpu",
        deviceGeneration: 1,
        representation: "columns-v1",
        cacheKey: "resident:test",
      },
    };
    expect(backend.assess(residentInput, {})).toMatchObject({
      supported: false,
      reasons: expect.arrayContaining([expect.objectContaining({ code: "input-shape" })]),
    });
  });

  it("plans by capability, cost, policy, and accelerator budgets", () => {
    const batch = batchFromColumns({ id: [1, 2, 3] });
    const fragment = testFragment(batch, {
      operators: [{ kind: "project", select: ["id"] }],
      output: { kind: "batch" },
    });
    const cpu = new CpuPhysicalBackend();
    const accelerator = new TestBackend("gpu", 0.000_001);

    const automatic = planPhysicalFragment(fragment, [cpu, accelerator]);
    expect(automatic.backendId).toBe("gpu");
    expect(automatic.candidates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ backendId: "gpu", selected: true }),
        expect.objectContaining({ backendId: "cpu", selected: false }),
      ]),
    );
    expect(
      planPhysicalFragment(fragment, [cpu, accelerator], { policy: "disabled" }).backendId,
    ).toBe("cpu");
    expect(
      planPhysicalFragment(fragment, [cpu, accelerator], { policy: "required" }).backendId,
    ).toBe("gpu");

    expect(() =>
      planPhysicalFragment(fragment, [cpu, accelerator], {
        policy: "required",
        budget: { maxAcceleratorUploadBytes: 0 },
      }),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE" }));
    expect(() => planPhysicalFragment(fragment, [])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE" }),
    );
    expect(() => planPhysicalFragment(fragment, [cpu], { policy: "required" })).toThrowError(
      expect.objectContaining({ code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE" }),
    );
    expect(
      planPhysicalFragment(fragment, [cpu, accelerator], {
        policy: "required",
        budget: {
          maxAcceleratorUploadBytes: 0,
          maxAcceleratorMemoryBytes: fragment.estimates.outputBytes,
        },
        inputResidencyKeys: new Set([fragment.input.sourceIdentity ?? fragment.id]),
      }).backendId,
    ).toBe("gpu");
    expect(planPhysicalFragment(fragment, [cpu, new TestBackend("bad-cost", -1)]).backendId).toBe(
      "cpu",
    );
    expect(() => planPhysicalFragment(fragment, [cpu, new CpuPhysicalBackend()])).toThrowError(
      expect.objectContaining({ code: "LAKEQL_VALIDATION_ERROR" }),
    );
  });

  it("replays only explicitly replayable accelerator failures on CPU", async () => {
    const batch = batchFromColumns({ id: [1, 2, 3] });
    const fragment = testFragment(batch, {
      operators: [{ kind: "select", predicate: gt("id", 1) }],
      output: { kind: "batch" },
    });
    const cpu = new CpuPhysicalBackend();
    const accelerator = new TestBackend("gpu", 0.000_001, "replayable-failure");
    const plan = planPhysicalFragment(fragment, [cpu, accelerator]);

    await expect(
      executePlannedPhysicalFragment(plan, [cpu, accelerator], { kind: "batch", batch }),
    ).rejects.toBeInstanceOf(PhysicalBackendExecutionError);

    const replayed = await executePlannedPhysicalFragment(
      plan,
      [cpu, accelerator],
      { kind: "batch", batch },
      {},
      { replayOnCpu: true },
    );
    expect(replayed.metrics).toMatchObject({
      backendId: "cpu",
      replayed: true,
      replaySourceBackendId: "gpu",
      uploadBytes: 64,
      readbackBytes: 16,
      dispatches: 1,
    });
    if (replayed.output.kind !== "batch") throw new Error("expected replayed batch");
    expect(materializeBatchRows(replayed.output.batch)).toEqual([{ id: 2 }, { id: 3 }]);
  });

  it("enforces CPU output, buffering, and elapsed budgets", async () => {
    const batch = batchFromColumns({ id: [1, 2, 3], score: [30, 20, 10] });
    const backend = new CpuPhysicalBackend();
    const passthrough = testFragment(batch, { operators: [], output: { kind: "batch" } });
    await expect(
      backend.execute(
        await backend.compile(passthrough),
        { kind: "batch", batch },
        {
          budget: { maxOutputRows: 2 },
        },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { resource: "output rows", limit: 2, actual: 3 },
    });

    const ordered = testFragment(batch, {
      operators: [{ kind: "order", orderBy: [{ column: "score" }] }],
      output: { kind: "batch" },
    });
    await expect(
      backend.execute(
        await backend.compile(ordered),
        { kind: "batch", batch },
        {
          budget: { maxBufferedRows: 2 },
        },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { resource: "buffered rows", limit: 2, actual: 3 },
    });
    await expect(
      backend.execute(
        await backend.compile(passthrough),
        { kind: "batch", batch },
        {
          budget: { maxElapsedMs: 0 },
          now: clock(),
        },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { resource: "elapsed milliseconds", limit: 0, actual: 1 },
    });
  });
});

class TestBackend implements PhysicalExecutionBackend {
  constructor(
    readonly id: string,
    private readonly costMs: number,
    private readonly behavior: "success" | "replayable-failure" = "success",
  ) {}

  capabilities(): PhysicalCapabilities {
    return {
      backendKind: "accelerator",
      operators: [
        {
          kind: "project",
          inputShapes: ["f64"],
          outputShapes: ["f64"],
          supportsNulls: true,
          supportsDictionary: false,
        },
        {
          kind: "select",
          inputShapes: ["f64"],
          outputShapes: ["bool"],
          supportsNulls: true,
          supportsDictionary: false,
        },
      ],
      fusedSequences: [["select", "project"]],
      vectorShapes: ["f64"],
      semantics: {
        nulls: "lakeql",
        numeric: "lakeql",
        ordering: "stable",
        aggregateState: "lakeql-snapshot",
      },
      limits: {},
      supportsResidentInput: true,
      supportsResidentOutput: true,
    };
  }

  assess(fragment: PhysicalFragment, context: BackendPlanningContext): BackendAssessment {
    return {
      supported: true,
      reasons: [],
      cost: {
        totalMs: this.costMs,
        inputConversionMs: 0,
        uploadMs: 0,
        compileMs: 0,
        computeMs: this.costMs,
        synchronizationMs: 0,
        readbackMs: 0,
        outputConversionMs: 0,
      },
      inputResident:
        context.inputResidencyKeys?.has(fragment.input.sourceIdentity ?? fragment.id) ?? false,
      compiledResident: false,
    };
  }

  async compile(fragment: PhysicalFragment): Promise<CompiledPhysicalFragment> {
    return { backendId: this.id, fragment };
  }

  async execute(
    compiled: CompiledPhysicalFragment,
    input: PhysicalFragmentInput,
    _context: BackendExecutionContext,
  ): Promise<PhysicalFragmentResult> {
    if (this.behavior === "replayable-failure") {
      throw new PhysicalBackendExecutionError(this.id, "device lost", {
        replayable: true,
        attemptedMetrics: {
          backendId: this.id,
          elapsedMs: 2,
          uploadBytes: 64,
          readbackBytes: 16,
          dispatches: 1,
        },
      });
    }
    if (input.kind !== "batch") {
      throw new PhysicalBackendExecutionError(this.id, "test backend expected a batch", {
        replayable: false,
      });
    }
    return {
      output: { kind: "batch", batch: input.batch },
      metrics: {
        backendId: this.id,
        elapsedMs: 0,
        inputRows: input.batch.rowCount,
        outputRows: input.batch.rowCount,
        inputBytes: compiled.fragment.estimates.inputBytes,
        uploadBytes: compiled.fragment.estimates.inputBytes,
        readbackBytes: compiled.fragment.estimates.outputBytes,
        dispatches: 1,
        replayed: false,
      },
    };
  }
}

function testFragment(
  batch: ReturnType<typeof batchFromColumns> | ReturnType<typeof batchFromVectors>,
  options: {
    operators: PhysicalFragment["operators"];
    output: PhysicalFragment["output"];
    sourceIdentity?: string;
  },
): PhysicalFragment {
  return {
    id: "test-fragment",
    input: physicalInputFromBatch(batch, {
      ...(options.sourceIdentity === undefined ? {} : { sourceIdentity: options.sourceIdentity }),
    }),
    operators: options.operators,
    output: options.output,
    estimates: {
      rowCount: batch.rowCount,
      inputBytes: estimateBatchBytes(batch),
      outputBytes: estimateBatchBytes(batch),
    },
  };
}

function clock(): () => number {
  let time = 0;
  return () => {
    time += 1;
    return time;
  };
}

it("plans scoped snapshots and continuations with separate capabilities", () => {
  const batch = batchFromColumns({ key: ["a"], value: [1] });
  const fragment = testFragment(batch, {
    operators: [
      { kind: "grouped-reduce", keys: ["key"], aggregates: { n: { op: "sum", column: "value" } } },
    ],
    output: { kind: "grouped-aggregate-snapshot" },
  });
  const backend = new CpuPhysicalBackend();
  expect(() =>
    planPhysicalFragment(fragment, [backend], { requireAllocationScope: true }),
  ).not.toThrow();
  expect(() =>
    planPhysicalFragment(fragment, [backend], {
      requireAllocationScope: true,
      requireReductionContinuation: true,
    }),
  ).not.toThrow();
  expect(() =>
    planPhysicalFragment(fragment, [backend], {
      policy: "required",
      requireReductionContinuation: true,
    }),
  ).toThrow();
  const ordinaryOnly: PhysicalExecutionBackend = {
    id: "ordinary-only",
    capabilities: () => backend.capabilities(),
    assess: (f, c) => backend.assess(f, c),
    compile: (f) => backend.compile(f),
    execute: (f, i, c) => backend.execute(f, i, c),
  };
  expect(() =>
    planPhysicalFragment(fragment, [ordinaryOnly], { requireReductionContinuation: true }),
  ).toThrow();
});

it("rejects unadmitted expression and column shapes during continuation placement", () => {
  const batch = batchFromColumns({ value: ["a"] });
  const backend = new CpuPhysicalBackend();
  const fragment = testFragment(batch, {
    operators: [
      {
        kind: "reduce",
        aggregates: {
          n: {
            op: "count_distinct",
            expr: {
              kind: "call",
              fn: "regexp_replace",
              args: [
                { kind: "column", name: "value" },
                { kind: "literal", value: "a" },
                { kind: "literal", value: "b" },
              ],
            },
          },
        },
      },
    ],
    output: { kind: "aggregate-snapshot" },
  });
  expect(() =>
    planPhysicalFragment(fragment, [backend], { requireReductionContinuation: true }),
  ).toThrow();
  const direct = testFragment(batch, {
    operators: [{ kind: "reduce", aggregates: { n: { op: "count", column: "value" } } }],
    output: { kind: "aggregate-snapshot" },
  });
  if (direct.input.kind !== "batch") throw Error("Missing input");
  direct.input.columns.value = {
    shape: "dict",
    nullable: false,
    dictionaryValueShape: { shape: "binary", nullable: false },
  };
  expect(() =>
    planPhysicalFragment(direct, [backend], { requireReductionContinuation: true }),
  ).toThrow();
});

it("plans nested union children explicitly and rejects changed runtime child shapes", async () => {
  const batch = batchFromVectors({
    value: {
      type: "union",
      tags: Uint8Array.of(0, 1),
      offsets: Uint32Array.of(0, 0),
      children: [
        { type: "f64", values: Float64Array.of(1) },
        { type: "dict", indices: Uint32Array.of(0), dictionary: { type: "utf8", values: ["a"] } },
      ],
    },
  });
  const fragment = testFragment(batch, {
    operators: [{ kind: "reduce", aggregates: { n: { op: "count_distinct", column: "value" } } }],
    output: { kind: "aggregate-snapshot" },
  });
  expect(fragment.input.kind === "batch" && fragment.input.columns.value).toEqual({
    shape: "union",
    nullable: false,
    unionChildShapes: [
      { shape: "f64", nullable: false },
      { shape: "dict", nullable: false, dictionaryValueShape: { shape: "utf8", nullable: false } },
    ],
  });
  const backend = new CpuPhysicalBackend();
  expect(() =>
    planPhysicalFragment(fragment, [backend], { requireReductionContinuation: true }),
  ).not.toThrow();
  const compiled = await backend.compile(fragment);
  const changed = batchFromVectors({
    value: {
      type: "union",
      tags: Uint8Array.of(0, 1),
      offsets: Uint32Array.of(0, 0),
      children: [
        { type: "f64", values: Float64Array.of(1) },
        {
          type: "dict",
          indices: Uint32Array.of(0),
          dictionary: { type: "f64", values: Float64Array.of(2) },
        },
      ],
    },
  });
  await expect(backend.execute(compiled, { kind: "batch", batch: changed })).rejects.toThrow(
    /shape changed/,
  );
  if (fragment.input.kind !== "batch") throw Error("Missing batch");
  const shape = fragment.input.columns.value;
  if (!shape) throw Error("Missing shape");
  delete shape.unionChildShapes;
  expect(() =>
    planPhysicalFragment(fragment, [backend], { requireReductionContinuation: true }),
  ).toThrow();
  shape.unionChildShapes = [{ shape: "binary", nullable: false }];
  expect(() =>
    planPhysicalFragment(fragment, [backend], { requireReductionContinuation: true }),
  ).toThrow();
});

it.each([
  false,
  true,
])("owns one-shot reduction checkpoints through restore (grouped=%s)", async (grouped) => {
  const aggregates = {
    n: { op: "count" as const },
    total: { op: "sum" as const, column: "value" },
  };
  const run = async (capacity: number) => {
    const budget = new MemoryBudget(capacity);
    const backend = new CpuPhysicalBackend();
    let prior: OwnedMemory<PhysicalOutputValue> | undefined;
    try {
      for (const values of [
        [0, 1, 2],
        [3, 4],
      ]) {
        const scope = new MemoryScope(budget);
        try {
          const batch = batchFromColumns({ key: values.map(() => "a"), value: values });
          const fragment = testFragment(batch, {
            operators: [
              { kind: "select", predicate: gt("value", 0) },
              grouped
                ? { kind: "grouped-reduce", keys: ["key"], aggregates }
                : { kind: "reduce", aggregates },
            ],
            output: { kind: grouped ? "grouped-aggregate-snapshot" : "aggregate-snapshot" },
          });
          const previous = prior?.value;
          if (
            previous &&
            previous.kind !== "aggregate-snapshot" &&
            previous.kind !== "grouped-aggregate-snapshot"
          )
            throw Error("Invalid checkpoint");
          const plan = planPhysicalFragment(fragment, [backend], { requireAllocationScope: true });
          const result = await executePlannedPhysicalFragment(
            plan,
            [backend],
            { kind: "batch", batch },
            {
              allocationScope: scope,
              ...(previous ? { priorOutput: previous } : {}),
            },
          );
          const next = scope.retainAllocations(result.output);
          prior?.release();
          prior = next;
        } finally {
          scope.dispose();
        }
      }
      expect(budget.usedBytes).toBeGreaterThan(0);
      const result = prior?.value;
      if (result?.kind === "grouped-aggregate-snapshot")
        expect(
          finalizeVectorGroupByRows(
            restoreVectorGroupByState(["key"], aggregates, result.snapshot),
          ),
        ).toEqual([{ key: "a", n: 4, total: 10 }]);
      else if (result?.kind === "aggregate-snapshot")
        expect(
          finalizeVectorAggregateStates(restoreVectorAggregateStates(result.snapshot)),
        ).toEqual({ n: 4, total: 10 });
      else throw Error("Missing checkpoint");
      return budget.peakBytes;
    } finally {
      prior?.release();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  // Aggregate arenas shrink to available capacity rather than requiring the generous-pool peak.
  expect(await run(peak - 1)).toBeLessThanOrEqual(peak - 1);
  await expect(run(16)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it.each([
  false,
  true,
])("plans scoped reduction through chained projections (grouped=%s)", async (grouped) => {
  const batch = batchFromColumns({ value: [1, 2] });
  const fragment = testFragment(batch, {
    operators: [
      { kind: "project", projections: { value: mul(col("value"), 2) } },
      { kind: "project", select: ["value"], projections: { alias: col("value") } },
      grouped
        ? { kind: "grouped-reduce", keys: ["alias"], aggregates: { n: { op: "count" } } }
        : { kind: "reduce", aggregates: { total: { op: "sum", column: "alias" } } },
    ],
    output: { kind: grouped ? "grouped-aggregate-snapshot" : "aggregate-snapshot" },
  });
  const backend = new CpuPhysicalBackend();
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  try {
    const plan = planPhysicalFragment(fragment, [backend], { requireAllocationScope: true });
    const result = await executePlannedPhysicalFragment(
      plan,
      [backend],
      { kind: "batch", batch },
      { allocationScope: scope },
    );
    if (result.output.kind === "aggregate-snapshot")
      expect(
        finalizeVectorAggregateStates(restoreVectorAggregateStates(result.output.snapshot)),
      ).toEqual({ total: 6 });
    else if (result.output.kind === "grouped-aggregate-snapshot")
      expect(
        finalizeVectorGroupByRows(
          restoreVectorGroupByState(["alias"], { n: { op: "count" } }, result.output.snapshot),
        ),
      ).toEqual([
        { alias: 2, n: 1 },
        { alias: 4, n: 1 },
      ]);
    else throw Error("Missing snapshot");
  } finally {
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "dropped",
  "binary",
  "binary-literal",
])("rejects unsupported projected reduction input: %s", (shape) => {
  const batch = batchFromColumns({ value: [1], binary: [Uint8Array.of(1)] });
  const fragment = testFragment(batch, {
    operators: [
      {
        kind: "project",
        select: ["binary"],
        ...(shape === "dropped"
          ? {}
          : {
              projections: {
                value:
                  shape === "binary"
                    ? col("binary")
                    : { kind: "literal" as const, value: Uint8Array.of(1) },
              },
            }),
      },
      { kind: "reduce", aggregates: { n: { op: "count", column: "value" } } },
    ],
    output: { kind: "aggregate-snapshot" },
  });
  expect(() =>
    planPhysicalFragment(fragment, [new CpuPhysicalBackend()], { requireAllocationScope: true }),
  ).toThrowError(expect.objectContaining({ code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE" }));
});
