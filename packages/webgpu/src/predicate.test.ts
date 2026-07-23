import {
  add,
  and,
  batchFromVectors,
  col,
  type Expr,
  eq,
  gt,
  gte,
  isIn,
  isNull,
  like,
  lit,
  lte,
  ne,
  not,
  notIn,
  or,
  type PhysicalFragment,
  physicalInputFromBatch,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import { compileWebGpuPredicate } from "./predicate.js";

describe("compileWebGpuPredicate", () => {
  it("packs multi-column nullable predicates into three bindings", () => {
    const batch = batchFromVectors({
      active: { type: "bool", values: Uint8Array.of(1, 0) },
      count: { type: "i32", values: Int32Array.of(2, 3), valid: Uint8Array.of(1, 0) },
    });
    const result = compileWebGpuPredicate(fragment(batch, and(eq("active", true), gt("count", 1))));

    expect(result.supported).toBe(true);
    if (!result.supported) return;
    expect(result.compiled.columns).toEqual([
      { name: "active", shape: "bool", ordinal: 0 },
      { name: "count", shape: "i32", ordinal: 1 },
    ]);
    expect(result.compiled.outputBinding).toBe(2);
    expect(result.compiled.wgsl).toContain("@group(0) @binding(0)");
    expect(result.compiled.wgsl).toContain("bitcast<i32>");
  });

  it("supports null checks without weakening selection semantics", () => {
    const batch = batchFromVectors({
      count: { type: "u32", values: Uint32Array.of(2), valid: Uint8Array.of(0) },
    });
    const result = compileWebGpuPredicate(fragment(batch, isNull("count")));
    expect(result.supported).toBe(true);
  });

  it("rejects f32 literals that would change CPU comparison boundaries", () => {
    const batch = batchFromVectors({
      score: { type: "f32", values: Float32Array.of(0.1) },
    });
    const result = compileWebGpuPredicate(fragment(batch, gt("score", 0.1)));
    expect(result).toEqual({
      supported: false,
      reason: "f32 predicate literals must be exactly representable to preserve CPU semantics",
    });
  });

  it("rejects unsupported expression and output shapes", () => {
    const batch = batchFromVectors({
      count: { type: "i32", values: Int32Array.of(2) },
    });
    const unsupported = compileWebGpuPredicate(fragment(batch, col("count")));
    expect(unsupported).toEqual({
      supported: false,
      reason: "Column count is not boolean",
    });

    const wrongOutput = {
      ...fragment(batch, eq("count", 2)),
      output: { kind: "indices" },
    } as const;
    expect(compileWebGpuPredicate(wrongOutput)).toEqual({
      supported: false,
      reason: "WebGPU selection requires one select operator and selection output",
    });

    const literal = compileWebGpuPredicate(fragment(batch, lit(null)));
    expect(literal.supported).toBe(true);
  });

  it("compiles comparison, negation, membership, and literal variants", () => {
    const batch = batchFromVectors({
      signed: { type: "i32", values: Int32Array.of(-1) },
      unsigned: { type: "u32", values: Uint32Array.of(1) },
      byte: { type: "u8", values: Uint8Array.of(1) },
      score: { type: "f32", values: Float32Array.of(-0) },
      active: { type: "bool", values: Uint8Array.of(1) },
    });
    const predicates: Expr[] = [
      ne("signed", -2),
      lte("signed", 0),
      gte("unsigned", 1),
      not(eq("active", false)),
      isIn("byte", []),
      notIn("byte", [1, 2]),
      or(eq("score", -0), eq("score", 0.5)),
      and(lit(true), col("active")),
    ];
    for (const predicate of predicates) {
      expect(compileWebGpuPredicate(fragment(batch, predicate)).supported).toBe(true);
    }
  });

  it.each([
    [eq("signed", 0x8000_0000), "i32 predicate literal is out of range"],
    [eq("unsigned", -1), "u32 predicate literal is out of range"],
    [eq("score", Number.NaN), "Numeric WebGPU literals must be finite numbers"],
    [eq("active", 1), "Boolean columns require boolean literals"],
    [eq("name", "lake"), "Physical shape utf8 is not supported by WebGPU"],
    [eq("missing", 1), "Unknown column missing"],
    [eq(lit(1), 1), "Numeric WebGPU literals require a typed column operand"],
    [add("signed", 1), "Predicate arithmetic is not supported by WebGPU"],
    [like("name", "l%"), "Predicate like is not supported by WebGPU"],
  ] satisfies Array<[Expr, string]>)("rejects %j: %s", (predicate, reason) => {
    const batch = batchFromVectors({
      signed: { type: "i32", values: Int32Array.of(1) },
      unsigned: { type: "u32", values: Uint32Array.of(1) },
      score: { type: "f32", values: Float32Array.of(1) },
      active: { type: "bool", values: Uint8Array.of(1) },
      name: { type: "utf8", values: ["lake"] },
    });
    expect(compileWebGpuPredicate(fragment(batch, predicate))).toEqual({
      supported: false,
      reason,
    });
  });
});

function fragment(
  batch: Parameters<typeof physicalInputFromBatch>[0],
  predicate: Expr,
): PhysicalFragment {
  return {
    id: "predicate-test",
    input: physicalInputFromBatch(batch),
    operators: [{ kind: "select", predicate }],
    output: { kind: "selection" },
    estimates: {
      rowCount: batch.rowCount,
      inputBytes: 128,
      outputBytes: batch.rowCount,
      dispatchCount: 1,
    },
  };
}
