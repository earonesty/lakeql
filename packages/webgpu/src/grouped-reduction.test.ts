import {
  type AggregateSpec,
  batchFromVectors,
  CpuPhysicalBackend,
  type Expr,
  finalizeVectorGroupByRows,
  gt,
  type PhysicalFragment,
  type PhysicalOutputValue,
  physicalInputFromBatch,
  restoreVectorGroupByState,
} from "lakeql-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { create, globals } from "webgpu";
import { WebGpuPhysicalBackend } from "./backend.js";
import { compileWebGpuGroupedReduction } from "./grouped-reduction.js";
import type { WebGpuRuntime } from "./runtime.js";

describe("WebGPU grouped reduction with Dawn", () => {
  let backend: WebGpuPhysicalBackend;

  beforeAll(() => {
    const constants = globals as unknown as {
      GPUBufferUsage: typeof GPUBufferUsage;
      GPUMapMode: typeof GPUMapMode;
    };
    const runtime: WebGpuRuntime = {
      gpu: create([]),
      constants: {
        bufferUsage: constants.GPUBufferUsage,
        mapMode: constants.GPUMapMode,
      },
    };
    backend = new WebGpuPhysicalBackend(() => runtime);
  });

  afterAll(() => backend.close());

  it("matches CPU grouping, nulls, selection, and count/min/max across tiles", async () => {
    const rowCount = 2051;
    const keys = new Int32Array(rowCount);
    const keyValid = new Uint8Array(rowCount);
    const values = new Float32Array(rowCount);
    const valueValid = new Uint8Array(rowCount);
    for (let row = 0; row < rowCount; row += 1) {
      keys[row] = (row % 7) - 3;
      keyValid[row] = row % 181 === 0 ? 0 : 1;
      values[row] = Math.fround((row % 97) - 48.5);
      valueValid[row] = row % 53 === 0 ? 0 : 1;
    }
    const batch = batchFromVectors({
      group: { type: "i32", values: keys, valid: keyValid },
      value: { type: "f32", values, valid: valueValid },
    });
    const aggregates = {
      rows: { op: "count" },
      present: { op: "count", column: "value" },
      minimum: { op: "min", column: "value" },
      maximum: { op: "max", column: "value" },
    } satisfies AggregateSpec;
    const target = fragment(batch, ["group"], aggregates, 8, gt("value", -40.5));
    const [actual, expected] = await Promise.all([
      execute(backend, target, batch),
      execute(new CpuPhysicalBackend(), target, batch),
    ]);
    expect(groupedRows(target, actual.output)).toEqual(groupedRows(target, expected.output));
    expect(actual.metrics).toMatchObject({
      inputRows: rowCount,
      outputRows: 8,
      dispatches: 1,
    });
  });

  it("uses SQL-equivalent f32 grouping for signed zero and NaN", async () => {
    const batch = batchFromVectors({
      group: {
        type: "f32",
        values: Float32Array.of(0, -0, Number.NaN, Number.NaN, 1),
      },
    });
    const target = fragment(batch, ["group"], { rows: { op: "count" } }, 3);
    const [actual, expected] = await Promise.all([
      execute(backend, target, batch),
      execute(new CpuPhysicalBackend(), target, batch),
    ]);
    expect(groupedRows(target, actual.output)).toEqual(groupedRows(target, expected.output));
  });

  it("merges a prior grouped snapshot and preserves group budgets", async () => {
    const first = batchFromVectors({
      group: { type: "u32", values: Uint32Array.of(1, 2) },
      value: { type: "u32", values: Uint32Array.of(10, 20) },
    });
    const second = batchFromVectors({
      group: { type: "u32", values: Uint32Array.of(2, 3) },
      value: { type: "u32", values: Uint32Array.of(5, 30) },
    });
    const aggregates = {
      rows: { op: "count" },
      minimum: { op: "min", column: "value" },
    } satisfies AggregateSpec;
    const firstResult = await execute(backend, fragment(first, ["group"], aggregates, 3), first);
    const secondTarget = fragment(second, ["group"], aggregates, 3);
    const merged = await execute(backend, secondTarget, second, firstResult.output);
    expect(groupedRows(secondTarget, merged.output)).toEqual([
      { group: 1, rows: 1, minimum: 10 },
      { group: 2, rows: 2, minimum: 5 },
      { group: 3, rows: 1, minimum: 30 },
    ]);

    const overflow = batchFromVectors({
      group: { type: "u32", values: Uint32Array.of(1, 2, 3, 4) },
    });
    await expect(
      execute(backend, fragment(overflow, ["group"], { rows: { op: "count" } }, 3), overflow),
    ).rejects.toMatchObject({ code: "LAKEQL_GROUP_LIMIT_EXCEEDED" });
  });

  it("rejects the wrong prior snapshot kind", async () => {
    const batch = batchFromVectors({
      group: { type: "u8", values: Uint8Array.of(1) },
    });
    await expect(
      execute(backend, fragment(batch, ["group"], { rows: { op: "count" } }, 2), batch, {
        kind: "aggregate-snapshot",
        snapshot: { states: {} },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("executes empty and boolean-key inputs and enforces output budgets", async () => {
    const empty = batchFromVectors({
      group: { type: "bool", values: new Uint8Array() },
      value: { type: "i32", values: new Int32Array() },
    });
    const aggregates = {
      rows: { op: "count" },
      minimum: { op: "min", column: "value" },
    } satisfies AggregateSpec;
    const emptyResult = await execute(backend, fragment(empty, ["group"], aggregates, 2), empty);
    expect(emptyResult.output).toEqual({
      kind: "grouped-aggregate-snapshot",
      snapshot: { groups: [] },
    });
    expect(emptyResult.metrics.dispatches).toBe(0);

    const batch = batchFromVectors({
      group: { type: "bool", values: Uint8Array.of(1, 0, 1) },
      value: { type: "i32", values: Int32Array.of(-5, 4, -2) },
    });
    const target = fragment(batch, ["group"], aggregates, 2);
    const result = await execute(backend, target, batch);
    expect(groupedRows(target, result.output)).toEqual([
      { group: true, rows: 2, minimum: -5 },
      { group: false, rows: 1, minimum: 4 },
    ]);
    await expect(
      backend.execute(
        await backend.compile(target),
        { kind: "batch", batch },
        {
          budget: { maxOutputRows: 1 },
        },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { resource: "output rows", actual: 2, limit: 1 },
    });
  });
});

describe("compileWebGpuGroupedReduction", () => {
  const batch = batchFromVectors({
    first: { type: "u32", values: Uint32Array.of(1) },
    second: { type: "u32", values: Uint32Array.of(2) },
  });

  it("requires a bounded one-key shape", () => {
    expect(
      compileWebGpuGroupedReduction(fragment(batch, ["first"], { rows: { op: "count" } })),
    ).toEqual({
      supported: false,
      reason: "WebGPU grouped reduction requires maxGroups between 1 and 32",
    });
    expect(
      compileWebGpuGroupedReduction(
        fragment(batch, ["first", "second"], { rows: { op: "count" } }, 4),
      ),
    ).toEqual({
      supported: false,
      reason: "WebGPU grouped reduction requires exactly one physical key column",
    });
  });

  it("rejects invalid limits, aggregate shapes, and fragment contracts", () => {
    for (const maxGroups of [0, 1.5, 33]) {
      expect(
        compileWebGpuGroupedReduction(
          fragment(batch, ["first"], { rows: { op: "count" } }, maxGroups),
        ),
      ).toEqual({
        supported: false,
        reason: "WebGPU grouped reduction requires maxGroups between 1 and 32",
      });
    }
    expect(
      compileWebGpuGroupedReduction(
        fragment(
          batch,
          ["first"],
          Object.fromEntries(
            Array.from({ length: 17 }, (_, index) => [`count${index}`, { op: "count" }]),
          ),
          4,
        ),
      ),
    ).toEqual({
      supported: false,
      reason: "WebGPU grouped reduction supports at most 16 aggregates",
    });
    expect(
      compileWebGpuGroupedReduction(
        fragment(batch, ["first"], { total: { op: "sum", column: "second" } }, 4),
      ),
    ).toEqual({
      supported: false,
      reason: "Aggregate sum is not supported by WebGPU reduction",
    });
    expect(
      compileWebGpuGroupedReduction({
        ...fragment(batch, ["first"], { rows: { op: "count" } }, 4),
        output: { kind: "selection" },
      }),
    ).toEqual({
      supported: false,
      reason: "WebGPU grouped reduction requires an optional select followed by grouped-reduce",
    });
  });
});

async function execute(
  backend: WebGpuPhysicalBackend | CpuPhysicalBackend,
  target: PhysicalFragment,
  batch: Parameters<typeof physicalInputFromBatch>[0],
  priorOutput?: Extract<
    PhysicalOutputValue,
    { kind: "aggregate-snapshot" | "grouped-aggregate-snapshot" }
  >,
) {
  return backend.execute(
    await backend.compile(target),
    { kind: "batch", batch },
    priorOutput === undefined ? {} : { priorOutput },
  );
}

function groupedRows(fragment: PhysicalFragment, output: PhysicalOutputValue) {
  if (output.kind !== "grouped-aggregate-snapshot") {
    throw new Error(`Expected grouped output, received ${output.kind}`);
  }
  const grouped = fragment.operators.at(-1);
  if (grouped?.kind !== "grouped-reduce") throw new Error("Missing grouped operator");
  return finalizeVectorGroupByRows(
    restoreVectorGroupByState(grouped.keys, grouped.aggregates, output.snapshot),
  );
}

function fragment(
  batch: Parameters<typeof physicalInputFromBatch>[0],
  keys: readonly string[],
  aggregates: AggregateSpec,
  maxGroups?: number,
  predicate?: Expr,
): PhysicalFragment {
  return {
    id: `grouped-${batch.rowCount}-${keys.join(",")}-${maxGroups ?? "unbounded"}`,
    input: physicalInputFromBatch(batch),
    operators: [
      ...(predicate === undefined ? [] : [{ kind: "select" as const, predicate }]),
      {
        kind: "grouped-reduce",
        keys,
        aggregates,
        ...(maxGroups === undefined ? {} : { maxGroups }),
      },
    ],
    output: { kind: "grouped-aggregate-snapshot" },
    estimates: {
      rowCount: batch.rowCount,
      inputBytes: batch.rowCount * Object.keys(batch.columns).length * 4,
      outputBytes: (maxGroups ?? 0) * (3 + Object.keys(aggregates).length * 2) * 4,
      dispatchCount: batch.rowCount === 0 ? 0 : 1,
    },
  };
}
