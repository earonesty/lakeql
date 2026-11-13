import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch, Vector } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";

const admittedCalls = new Set([
  "lower",
  "upper",
  "trim",
  "substr",
  "replace",
  "floor",
  "ceil",
  "abs",
  "round",
  "coalesce",
  "nullif",
]);

/** Explicit allocation capabilities, not public query-language restrictions.
 * Regex, spatial construction and object-to-number casts await
 * their allocation contracts. Planner integration must honor this assessment.
 */
export function ownedAggregateExpressionBytes(batch: Batch, expr: Expr): number {
  return ownedAggregateExpressionBytesForColumns(expr, (name) => {
    requireOwnedAggregateVector(batch, name);
  });
}

export function ownedAggregateExpressionBytesForColumns(
  expr: Expr,
  requireColumn: (name: string) => void,
): number {
  return expressionPlanBytes(expr, requireColumn, false);
}

function expressionPlanBytes(
  expr: Expr,
  requireColumn: (name: string) => void,
  predicate: boolean,
): number {
  let bytes = c.object + 4 * c.property;
  const scalar = (value: Expr) => expressionPlanBytes(value, requireColumn, false);
  const condition = (value: Expr) => expressionPlanBytes(value, requireColumn, true);
  if (predicate) {
    switch (expr.kind) {
      case "compare":
        return bytes + scalar(expr.left) + scalar(expr.right);
      case "in":
        bytes += c.array + c.arraySlot * expr.values.length + scalar(expr.target);
        for (const value of expr.values) bytes += scalar(value);
        return checkedPlanBytes(bytes);
      case "between":
        return bytes + scalar(expr.target) + scalar(expr.low) + scalar(expr.high);
      case "null-check":
        return bytes + scalar(expr.target);
      case "logical":
        for (const operand of expr.operands) bytes += condition(operand);
        return checkedPlanBytes(bytes);
      case "not":
        return bytes + condition(expr.operand);
    }
  }
  switch (expr.kind) {
    case "case":
      bytes += c.array + c.arraySlot * expr.whens.length;
      for (const branch of expr.whens) bytes += condition(branch.when) + scalar(branch.value);
      if (expr.else !== undefined) bytes += scalar(expr.else);
      return checkedPlanBytes(bytes);
    case "literal":
      return bytes;
    case "column":
      requireColumn(expr.name);
      return bytes;
    case "arithmetic":
      return (
        bytes +
        ownedAggregateExpressionBytesForColumns(expr.left, requireColumn) +
        ownedAggregateExpressionBytesForColumns(expr.right, requireColumn)
      );
    case "call": {
      const fn = expr.fn.toLowerCase();
      const supported =
        admittedCalls.has(fn) ||
        (fn === "cast" && expr.args[1]?.kind === "literal" && expr.args[1].value === "string");
      if (!supported) throw unsupported("call", fn);
      bytes += c.array + c.arraySlot * expr.args.length;
      for (const arg of expr.args)
        bytes += ownedAggregateExpressionBytesForColumns(arg, requireColumn);
      return checkedPlanBytes(bytes);
    }
    default:
      throw unsupported("expression", expr.kind);
  }
}

function checkedPlanBytes(bytes: number): number {
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Aggregate expression extent overflow");
  return bytes;
}

export function requireOwnedAggregateVector(batch: Batch, name: string): Vector {
  const vector = batch.columns[name];
  if (!vector)
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${name}`, { column: name });
  requireScalarShape(vector);
  return vector;
}

function requireScalarShape(vector: Vector): void {
  switch (vector.type) {
    case "union":
      for (const child of vector.children) requireScalarShape(child);
      return;
    case "dict":
      requireScalarShape(vector.dictionary);
      return;
    case "binary":
    case "binary-view":
    case "list":
    case "struct":
    case "map":
      throw unsupported("vector", vector.type);
    default:
      return;
  }
}

function unsupported(kind: string, shape: string): LakeqlError {
  return new LakeqlError(
    "LAKEQL_UNSUPPORTED_PUSHDOWN",
    "Owned aggregate input shape lacks an allocation contract",
    { kind, shape },
  );
}
