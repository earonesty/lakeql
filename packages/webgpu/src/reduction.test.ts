import {
  type AggregateSpec,
  type Batch,
  batchFromVectors,
  CpuPhysicalBackend,
  type Expr,
  gt,
  type PhysicalFragment,
  type PhysicalOutputValue,
  physicalInputFromBatch,
} from "lakeql-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { create, globals } from "webgpu";
import { WebGpuPhysicalBackend } from "./backend.js";
import { compileWebGpuReduction } from "./reduction.js";
import type { WebGpuRuntime } from "./runtime.js";

describe("WebGPU reductions with Dawn", () => {
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

  it("fuses selection with exact count and order-preserving min/max partials", async () => {
    const rowCount = 2051;
    const signed = new Int32Array(rowCount);
    const unsigned = new Uint32Array(rowCount);
    const score = new Float32Array(rowCount);
    const flag = new Uint8Array(rowCount);
    const valid = new Uint8Array(rowCount);
    valid.fill(1);
    for (let index = 0; index < rowCount; index += 1) {
      signed[index] = index - 1025;
      unsigned[index] = index;
      score[index] = index % 2 === 0 ? -0 : index / 2;
      flag[index] = index % 3 === 0 ? 1 : 0;
    }
    valid[1500] = 0;
    score[1600] = Number.NaN;
    const batch = batchFromVectors({
      signed: { type: "i32", values: signed, valid },
      unsigned: { type: "u32", values: unsigned },
      score: { type: "f32", values: score },
      flag: { type: "bool", values: flag },
    });
    const spec = {
      rows: { op: "count" },
      values: { op: "count", column: "signed" },
      low: { op: "min", column: "signed" },
      high: { op: "max", column: "unsigned" },
      scoreLow: { op: "min", column: "score" },
      anyFlag: { op: "max", column: "flag" },
    } satisfies AggregateSpec;
    const target = fragment(batch, spec, gt("unsigned", 100));

    const [actual, expected] = await Promise.all([
      execute(backend, target, batch),
      execute(new CpuPhysicalBackend(), target, batch),
    ]);
    expect(actual.output).toEqual(expected.output);
    expect(actual.metrics).toMatchObject({
      inputRows: rowCount,
      selectedRows: 1950,
      outputRows: 1,
      dispatches: 1,
    });
    expect(actual.metrics.readbackBytes).toBe(3 * (1 + Object.keys(spec).length * 2) * 4);
  });

  it("merges prior snapshots in batch order and preserves signed zero and NaN behavior", async () => {
    const spec = {
      count: { op: "count" },
      minimum: { op: "min", column: "score" },
      maximum: { op: "max", column: "score" },
    } satisfies AggregateSpec;
    const firstBatch = batchFromVectors({
      score: { type: "f32", values: Float32Array.of(-0, 3, Number.NaN) },
    });
    const secondBatch = batchFromVectors({
      score: { type: "f32", values: Float32Array.of(0, -2, 9) },
    });
    const first = await execute(backend, fragment(firstBatch, spec), firstBatch);
    expect(first.output.kind).toBe("aggregate-snapshot");
    const prior = first.output as Extract<PhysicalOutputValue, { kind: "aggregate-snapshot" }>;
    const second = await execute(backend, fragment(secondBatch, spec), secondBatch, prior);
    expect(second.output).toEqual({
      kind: "aggregate-snapshot",
      snapshot: {
        count: { op: "count", count: 6 },
        minimum: { op: "min", value: -2 },
        maximum: { op: "max", value: 9 },
      },
    });
  });

  it("returns initial snapshots for empty input without a dispatch", async () => {
    const batch = batchFromVectors({
      value: { type: "u32", values: new Uint32Array() },
    });
    const result = await execute(
      backend,
      fragment(batch, {
        count: { op: "count" },
        minimum: { op: "min", column: "value" },
      }),
      batch,
    );
    expect(result.output).toEqual({
      kind: "aggregate-snapshot",
      snapshot: {
        count: { op: "count", count: 0 },
        minimum: { op: "min", value: null },
      },
    });
    expect(result.metrics.dispatches).toBe(0);
  });

  it("rejects a grouped prior snapshot at the reduction boundary", async () => {
    const batch = batchFromVectors({
      value: { type: "u32", values: Uint32Array.of(1) },
    });
    const target = fragment(batch, { count: { op: "count" } });
    const compiled = await backend.compile(target);
    await expect(
      backend.execute(
        compiled,
        { kind: "batch", batch },
        {
          priorOutput: {
            kind: "grouped-aggregate-snapshot",
            snapshot: { groups: [] },
          },
        },
      ),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
  });
});

describe("compileWebGpuReduction", () => {
  const batch = batchFromVectors({
    value: { type: "u32", values: Uint32Array.of(1) },
  });

  it.each([
    [{ total: { op: "sum", column: "value" } }, "Aggregate sum is not supported"],
    [{ minimum: { op: "min" } }, "Aggregate min requires a physical column"],
    [
      {
        count: {
          op: "count",
          expr: {
            kind: "arithmetic",
            op: "add",
            left: { kind: "column", name: "value" },
            right: { kind: "literal", value: 1 },
          },
        },
      },
      "Aggregate count expression must be a direct column",
    ],
  ] satisfies Array<
    [AggregateSpec, string]
  >)("rejects unsupported aggregate contracts", (spec, reason) => {
    expect(compileWebGpuReduction(fragment(batch, spec))).toEqual({
      supported: false,
      reason: expect.stringContaining(reason),
    });
  });

  it("rejects empty aggregates and non-reduction output contracts", () => {
    expect(compileWebGpuReduction(fragment(batch, {}))).toEqual({
      supported: false,
      reason: "WebGPU reduction requires at least one aggregate",
    });
    expect(
      compileWebGpuReduction({
        ...fragment(batch, { count: { op: "count" } }),
        output: { kind: "selection" },
      }),
    ).toEqual({
      supported: false,
      reason: "WebGPU reduction requires an optional select followed by reduce",
    });
  });
});

async function execute(
  backend: WebGpuPhysicalBackend | CpuPhysicalBackend,
  target: PhysicalFragment,
  batch: Batch,
  priorOutput?: Extract<PhysicalOutputValue, { kind: "aggregate-snapshot" }>,
) {
  const compiled = await backend.compile(target);
  return backend.execute(
    compiled,
    { kind: "batch", batch },
    priorOutput === undefined ? {} : { priorOutput },
  );
}

function fragment(batch: Batch, aggregates: AggregateSpec, predicate?: Expr): PhysicalFragment {
  return {
    id: `reduction-${batch.rowCount}-${JSON.stringify(aggregates)}-${JSON.stringify(predicate)}`,
    input: physicalInputFromBatch(batch),
    operators: [
      ...(predicate === undefined ? [] : [{ kind: "select" as const, predicate }]),
      { kind: "reduce", aggregates },
    ],
    output: { kind: "aggregate-snapshot" },
    estimates: {
      rowCount: batch.rowCount,
      inputBytes: batch.rowCount * Object.keys(batch.columns).length * 4,
      outputBytes: Object.keys(aggregates).length * 16,
      dispatchCount: batch.rowCount === 0 ? 0 : 1,
    },
  };
}
