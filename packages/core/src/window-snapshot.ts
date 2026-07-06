import { isIntervalValue } from "./interval.js";
import type { WindowExpr } from "./window.js";

export function isWindowExprSnapshot(value: unknown): value is WindowExpr {
  if (!isRecord(value)) return false;
  const fn = value.fn;
  const validFn =
    typeof fn === "string" ||
    (isRecord(fn) && typeof fn.aggregate === "string" && Object.keys(fn).length === 1);
  return (
    validFn &&
    Array.isArray(value.args) &&
    value.args.every(isExprSnapshot) &&
    isRecord(value.over) &&
    Array.isArray(value.over.partitionBy) &&
    value.over.partitionBy.every(isExprSnapshot) &&
    Array.isArray(value.over.orderBy) &&
    value.over.orderBy.every(isWindowOrderTermSnapshot) &&
    (value.over.frame === undefined || isWindowFrameSnapshot(value.over.frame)) &&
    (value.filter === undefined || isExprSnapshot(value.filter)) &&
    (value.ignoreNulls === undefined || typeof value.ignoreNulls === "boolean") &&
    (value.distinct === undefined || typeof value.distinct === "boolean")
  );
}

function isWindowOrderTermSnapshot(value: unknown): boolean {
  return (
    isRecord(value) &&
    isExprSnapshot(value.expr) &&
    (value.direction === undefined || value.direction === "asc" || value.direction === "desc") &&
    (value.nulls === undefined || value.nulls === "first" || value.nulls === "last")
  );
}

function isWindowFrameSnapshot(value: unknown): boolean {
  return (
    isRecord(value) &&
    (value.mode === "rows" || value.mode === "range" || value.mode === "groups") &&
    isWindowFrameBoundSnapshot(value.start) &&
    isWindowFrameBoundSnapshot(value.end) &&
    (value.exclude === "no-others" ||
      value.exclude === "current-row" ||
      value.exclude === "group" ||
      value.exclude === "ties")
  );
}

function isWindowFrameBoundSnapshot(value: unknown): boolean {
  return (
    isRecord(value) &&
    (value.kind === "unbounded-preceding" ||
      value.kind === "preceding" ||
      value.kind === "current-row" ||
      value.kind === "following" ||
      value.kind === "unbounded-following") &&
    (value.offset === undefined || isExprSnapshot(value.offset))
  );
}

function isExprSnapshot(value: unknown): boolean {
  if (!isRecord(value) || typeof value.kind !== "string") return false;
  switch (value.kind) {
    case "literal":
      return isWindowSnapshotValue(value.value) || isIntervalValue(value.value);
    case "column":
      return typeof value.name === "string";
    case "compare":
      return (
        (value.op === "eq" ||
          value.op === "ne" ||
          value.op === "lt" ||
          value.op === "lte" ||
          value.op === "gt" ||
          value.op === "gte") &&
        isExprSnapshot(value.left) &&
        isExprSnapshot(value.right)
      );
    case "in":
      return (
        typeof value.negated === "boolean" &&
        isExprSnapshot(value.target) &&
        Array.isArray(value.values) &&
        value.values.every(isExprSnapshot)
      );
    case "between":
      return (
        isExprSnapshot(value.target) && isExprSnapshot(value.low) && isExprSnapshot(value.high)
      );
    case "null-check":
      return typeof value.negated === "boolean" && isExprSnapshot(value.target);
    case "logical":
      return (
        (value.op === "and" || value.op === "or") &&
        Array.isArray(value.operands) &&
        value.operands.every(isExprSnapshot)
      );
    case "not":
      return isExprSnapshot(value.operand);
    case "like":
      return (
        typeof value.caseInsensitive === "boolean" &&
        isExprSnapshot(value.target) &&
        typeof value.pattern === "string"
      );
    case "call":
      return (
        typeof value.fn === "string" &&
        Array.isArray(value.args) &&
        value.args.every(isExprSnapshot)
      );
    case "arithmetic":
      return (
        (value.op === "add" ||
          value.op === "sub" ||
          value.op === "mul" ||
          value.op === "div" ||
          value.op === "mod") &&
        isExprSnapshot(value.left) &&
        isExprSnapshot(value.right)
      );
    case "case":
      return (
        Array.isArray(value.whens) &&
        value.whens.every(
          (branch) =>
            isRecord(branch) && isExprSnapshot(branch.when) && isExprSnapshot(branch.value),
        ) &&
        (value.else === undefined || isExprSnapshot(value.else))
      );
    default:
      return false;
  }
}

function isWindowSnapshotValue(value: unknown): boolean {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
