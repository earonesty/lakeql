import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import { compareTimestampValues, isTimestampValue, type TimestampValue } from "./timestamp.js";

export interface ScalarOrderTerm {
  column?: string;
  direction?: "asc" | "desc";
  nulls?: "first" | "last";
}

export function compareOrderedScalars(
  left: unknown,
  right: unknown,
  term: ScalarOrderTerm = {},
  context = "orderBy",
): number {
  const direction = term.direction ?? "asc";
  const nulls = term.nulls ?? (direction === "asc" ? "last" : "first");
  const leftNull = left === null || left === undefined;
  const rightNull = right === null || right === undefined;
  if (leftNull || rightNull) {
    if (leftNull && rightNull) return 0;
    const nullOrder = nulls === "first" ? -1 : 1;
    return leftNull ? nullOrder : -nullOrder;
  }
  const comparison = compareNonNullScalars(left, right, context, term.column);
  return direction === "desc" ? -comparison : comparison;
}

export function compareAggregateScalars(left: Scalar, right: Scalar): number {
  if (left === null || right === null) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare null aggregate values");
  }
  return compareNonNullScalars(left, right, "aggregate");
}

export function numericAggregateValue(op: string, value: Scalar): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${op} window aggregate requires numeric input`, {
      aggregate: op,
    });
  }
  return value;
}

export function isSortableScalar(
  value: unknown,
): value is string | number | bigint | boolean | TimestampValue {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    isTimestampValue(value)
  );
}

function compareNonNullScalars(
  left: unknown,
  right: unknown,
  context: string,
  column?: string,
): number {
  if (!isSortableScalar(left) || !isSortableScalar(right)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${context} values must be scalar`, {
      ...(column === undefined ? {} : { column }),
    });
  }
  if (
    (typeof left === "number" && !Number.isFinite(left)) ||
    (typeof right === "number" && !Number.isFinite(right))
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${context} numeric values must be finite`, {
      ...(column === undefined ? {} : { column }),
    });
  }
  if (isTimestampValue(left) || isTimestampValue(right)) {
    if (!isTimestampValue(left) || !isTimestampValue(right)) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        `${context} timestamp values must have matching types`,
        {
          ...(column === undefined ? {} : { column }),
        },
      );
    }
    return compareTimestampValues(left, right);
  }
  if (typeof left !== typeof right) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${context} values must have matching types`, {
      ...(column === undefined ? {} : { column }),
    });
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}
