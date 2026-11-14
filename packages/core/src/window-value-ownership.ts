import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import { setRecordValue } from "./record-value.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import type { Row } from "./types.js";

/** Synchronous kernels transfer plain results at return, unless a caller keeps
 * an explicit result scope through streaming or asynchronous checkpoint writes. */
export function withWindowResultScope<T extends readonly unknown[]>(
  options: { memoryBudget?: MemoryBudget; resultScope?: MemoryScope },
  run: (scope: MemoryScope) => T,
): T {
  const parent = options.resultScope;
  const scope =
    parent?.childScope() ??
    new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    if (options.memoryBudget !== undefined && options.memoryBudget !== scope.budget)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Window result scope must use the execution budget",
      );
    const result = run(scope);
    if (parent !== undefined) {
      const owner = scope.retainAllocations(undefined);
      try {
        parent.adopt(owner);
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return result;
  } finally {
    scope.dispose();
  }
}

/** Consume probe values before closing temporary expression storage. */
export function withWindowValue<T>(
  expr: Expr,
  row: Row,
  scope: MemoryScope | undefined,
  consume: (value: unknown, temporary?: MemoryScope) => T,
): T {
  if (scope === undefined) return consume(evaluate(expr, row));
  const temporary = scope.childScope();
  try {
    return consume(evaluate(expr, row, temporary), temporary);
  } finally {
    temporary.dispose();
  }
}

export function retainWindowValue(
  value: unknown,
  scope?: MemoryScope,
  temporary?: MemoryScope,
): unknown {
  if (
    scope === undefined ||
    value === null ||
    value === undefined ||
    typeof value === "number" ||
    typeof value === "boolean"
  )
    return value;
  if (typeof value === "string" || typeof value === "bigint") {
    const existing = scope.tryRetainAllocation(value);
    if (existing !== undefined) {
      const retained = existing.value;
      existing.release();
      return retained;
    }
    const created = temporary?.tryRetainAllocation(value);
    if (created !== undefined) {
      try {
        return scope.adopt(created).value;
      } catch (error) {
        created.release();
        throw error;
      }
    }
    return scope.allocate(
      typeof value === "string"
        ? c.string + value.length * c.stringCodeUnit
        : c.bigint + decimalStringAllocationBytes(value),
      "window retained scalar",
      () => value,
    ).value;
  }
  const wrapper = scope.reserve(c.object + c.property, "window value copy wrapper");
  try {
    const owner = detachOwnedRow({ value }, scope.budget, temporary);
    try {
      return scope.adopt(owner).value.value;
    } catch (error) {
      owner.release();
      throw error;
    }
  } finally {
    wrapper.release();
  }
}

export function evaluateWindowValue(expr: Expr, row: Row, scope?: MemoryScope): unknown {
  return withWindowValue(expr, row, scope, (value, temporary) =>
    retainWindowValue(value, scope, temporary),
  );
}

/** Copy row containers while payloads remain pinned by the input lifetime. */
export function copyWindowRows(rows: readonly Row[], scope: MemoryScope): Row[] {
  return scope.allocate(c.array + rows.length * c.arraySlot, "window result rows", () =>
    rows.map((source) => {
      const row = scope.allocate(c.object, "window result record", () => ({}) as Row).value;
      for (const key in source) {
        if (Object.hasOwn(source, key)) setWindowResultValue(row, key, source[key], scope);
      }
      return row;
    }),
  ).value;
}

/** Replacing a field reuses its admitted slot; new aliases reserve before mutation. */
export function setWindowResultValue(
  row: Row,
  key: string,
  value: unknown,
  scope: MemoryScope,
): void {
  if (Object.hasOwn(row, key)) setRecordValue(row, key, value);
  else scope.allocate(c.property, "window result property", () => setRecordValue(row, key, value));
}
