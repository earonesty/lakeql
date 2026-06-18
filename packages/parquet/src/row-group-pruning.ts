import type { RowGroup } from "hyparquet";
import type { CompareOp, Expr } from "lakeql-core";

export type RowGroupStatsValue = string | number | bigint | boolean;

/** @internal Exposed for pruning tests; not part of the stable public API. */
export function rowGroupMayMatch(rowGroup: RowGroup, expr: Expr | undefined): boolean {
  if (!expr) return true;
  switch (expr.kind) {
    case "literal":
    case "column":
    case "null-check":
    case "like":
    case "arithmetic":
    case "case":
      return true;
    case "call":
      return callMayMatch(rowGroup, expr);
    case "not":
      return true;
    case "logical":
      if (expr.op === "and")
        return expr.operands.every((operand) => rowGroupMayMatch(rowGroup, operand));
      return expr.operands.some((operand) => rowGroupMayMatch(rowGroup, operand));
    case "compare":
      return compareMayMatch(rowGroup, expr);
    case "in":
      return inMayMatch(rowGroup, expr);
    case "between":
      return betweenMayMatch(rowGroup, expr);
  }
}

/** @internal Exposed for scan/aggregate tests; not part of the stable public API. */
export function rowGroupMustMatch(rowGroup: RowGroup, expr: Expr | undefined): boolean {
  if (!expr) return true;
  switch (expr.kind) {
    case "literal":
      return expr.value === true;
    case "column":
    case "null-check":
    case "like":
    case "arithmetic":
    case "case":
    case "call":
    case "not":
    case "in":
      return false;
    case "logical":
      if (expr.op === "and")
        return expr.operands.every((operand) => rowGroupMustMatch(rowGroup, operand));
      return expr.operands.some((operand) => rowGroupMustMatch(rowGroup, operand));
    case "compare":
      return compareMustMatch(rowGroup, expr);
    case "between":
      return betweenMustMatch(rowGroup, expr);
  }
}

function compareMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "compare" }>): boolean {
  const pair = columnLiteralCompare(expr.left, expr.op, expr.right);
  if (!pair) return true;
  const stats = columnStats(rowGroup, pair.column);
  if (!stats) return true;
  const { min, max } = stats;
  const value = pair.value;
  if (!sameComparableType(min, value) || !sameComparableType(max, value)) return true;
  switch (pair.op) {
    case "eq":
      return compareValues(value, min) >= 0 && compareValues(value, max) <= 0;
    case "ne":
      return !(compareValues(min, value) === 0 && compareValues(max, value) === 0);
    case "lt":
      return compareValues(min, value) < 0;
    case "lte":
      return compareValues(min, value) <= 0;
    case "gt":
      return compareValues(max, value) > 0;
    case "gte":
      return compareValues(max, value) >= 0;
  }
}

function compareMustMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "compare" }>): boolean {
  const pair = columnLiteralCompare(expr.left, expr.op, expr.right);
  if (!pair) return false;
  const stats = columnStats(rowGroup, pair.column);
  if (!stats?.hasNoNulls) return false;
  const { min, max } = stats;
  const value = pair.value;
  if (!sameComparableType(min, value) || !sameComparableType(max, value)) return false;
  switch (pair.op) {
    case "eq":
      return compareValues(min, value) === 0 && compareValues(max, value) === 0;
    case "ne":
      return compareValues(value, min) < 0 || compareValues(value, max) > 0;
    case "lt":
      return compareValues(max, value) < 0;
    case "lte":
      return compareValues(max, value) <= 0;
    case "gt":
      return compareValues(min, value) > 0;
    case "gte":
      return compareValues(min, value) >= 0;
  }
}

function inMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "in" }>): boolean {
  if (expr.negated) return true;
  if (expr.target.kind !== "column") return true;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats) return true;
  return expr.values.some((valueExpr) => {
    if (valueExpr.kind !== "literal" || valueExpr.value === null) return true;
    const value = valueExpr.value;
    if (
      !isStatsValue(value) ||
      !sameComparableType(stats.min, value) ||
      !sameComparableType(stats.max, value)
    ) {
      return true;
    }
    return compareValues(value, stats.min) >= 0 && compareValues(value, stats.max) <= 0;
  });
}

function betweenMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "between" }>): boolean {
  if (
    expr.target.kind !== "column" ||
    expr.low.kind !== "literal" ||
    expr.high.kind !== "literal"
  ) {
    return true;
  }
  if (!isStatsValue(expr.low.value) || !isStatsValue(expr.high.value)) return true;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats) return true;
  if (
    !sameComparableType(stats.min, expr.low.value) ||
    !sameComparableType(stats.max, expr.high.value)
  ) {
    return true;
  }
  return (
    compareValues(stats.max, expr.low.value) >= 0 && compareValues(stats.min, expr.high.value) <= 0
  );
}

function betweenMustMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "between" }>): boolean {
  if (
    expr.target.kind !== "column" ||
    expr.low.kind !== "literal" ||
    expr.high.kind !== "literal"
  ) {
    return false;
  }
  if (!isStatsValue(expr.low.value) || !isStatsValue(expr.high.value)) return false;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats?.hasNoNulls) return false;
  if (
    !sameComparableType(stats.min, expr.low.value) ||
    !sameComparableType(stats.max, expr.high.value)
  ) {
    return false;
  }
  return (
    compareValues(stats.min, expr.low.value) >= 0 && compareValues(stats.max, expr.high.value) <= 0
  );
}

function callMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  if (expr.fn === "st_intersects") return stIntersectsMayMatch(rowGroup, expr);
  if (expr.fn === "h3_in") return h3InMayMatch(rowGroup, expr);
  if (expr.fn === "h3_within") return h3WithinMayMatch(rowGroup, expr);
  return true;
}

function stIntersectsMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, query] = expr.args;
  if (target?.kind !== "column") return true;
  const queryBBox = bboxLiteral(query);
  if (!queryBBox) return true;
  const groupBBox = rowGroupBBox(rowGroup, target.name);
  if (!groupBBox) return true;
  return (
    groupBBox.maxx >= queryBBox.minx &&
    groupBBox.minx <= queryBBox.maxx &&
    groupBBox.maxy >= queryBBox.miny &&
    groupBBox.miny <= queryBBox.maxy
  );
}

function h3InMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, values] = expr.args;
  if (target?.kind !== "column") return true;
  const cells = h3CellList(values);
  if (!cells) return true;
  const stats = columnStats(rowGroup, target.name);
  if (!stats || typeof stats.min !== "string" || typeof stats.max !== "string") return true;
  return cells.some((cell) => cell >= stats.min && cell <= stats.max);
}

function h3WithinMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, origin, radius] = expr.args;
  if (
    target?.kind !== "column" ||
    origin?.kind !== "literal" ||
    typeof origin.value !== "string" ||
    radius?.kind !== "literal" ||
    radius.value !== 0
  ) {
    return true;
  }
  return h3InMayMatch(rowGroup, {
    kind: "call",
    fn: "h3_in",
    args: [target, { kind: "literal", value: JSON.stringify([origin.value]) }],
  });
}

function bboxLiteral(
  expr: Expr | undefined,
): { minx: number; miny: number; maxx: number; maxy: number } | undefined {
  if (expr?.kind === "call" && expr.fn === "st_bbox" && expr.args.length === 4) {
    const values = expr.args.map((arg) => (arg.kind === "literal" ? arg.value : undefined));
    const bbox = numberTuple4(values);
    if (bbox) {
      const [minx, miny, maxx, maxy] = bbox;
      if (minx <= maxx && miny <= maxy) return { minx, miny, maxx, maxy };
    }
  }
  if (expr?.kind === "literal" && typeof expr.value === "string") {
    try {
      const parsed = JSON.parse(expr.value) as unknown;
      const bbox = numberTuple4(parsed);
      if (bbox) {
        const [minx, miny, maxx, maxy] = bbox;
        if (minx <= maxx && miny <= maxy) return { minx, miny, maxx, maxy };
      }
    } catch {
      return undefined;
    }
  }
  return undefined;
}

function numberTuple4(value: unknown): [number, number, number, number] | undefined {
  if (!Array.isArray(value) || value.length !== 4) {
    return undefined;
  }
  const [first, second, third, fourth] = value;
  if (
    typeof first !== "number" ||
    typeof second !== "number" ||
    typeof third !== "number" ||
    typeof fourth !== "number"
  ) {
    return undefined;
  }
  return [first, second, third, fourth];
}

function h3CellList(expr: Expr | undefined): string[] | undefined {
  if (expr?.kind !== "literal" || typeof expr.value !== "string") return undefined;
  try {
    const parsed = JSON.parse(expr.value) as unknown;
    if (Array.isArray(parsed) && parsed.every((value) => typeof value === "string")) {
      return parsed;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function rowGroupBBox(
  rowGroup: RowGroup,
  geometryColumn: string,
): { minx: number; miny: number; maxx: number; maxy: number } | undefined {
  const candidates = [
    {
      minx: `${geometryColumn}_minx`,
      miny: `${geometryColumn}_miny`,
      maxx: `${geometryColumn}_maxx`,
      maxy: `${geometryColumn}_maxy`,
    },
    { minx: "minx", miny: "miny", maxx: "maxx", maxy: "maxy" },
  ];
  for (const columns of candidates) {
    const minx = numericColumnStats(rowGroup, columns.minx)?.min;
    const miny = numericColumnStats(rowGroup, columns.miny)?.min;
    const maxx = numericColumnStats(rowGroup, columns.maxx)?.max;
    const maxy = numericColumnStats(rowGroup, columns.maxy)?.max;
    if (minx !== undefined && miny !== undefined && maxx !== undefined && maxy !== undefined) {
      return { minx, miny, maxx, maxy };
    }
  }
  return undefined;
}

function numericColumnStats(
  rowGroup: RowGroup,
  column: string,
): { min: number; max: number } | undefined {
  const stats = columnStats(rowGroup, column);
  if (!stats || !isNumberLike(stats.min) || !isNumberLike(stats.max)) return undefined;
  return { min: Number(stats.min), max: Number(stats.max) };
}

function columnLiteralCompare(
  left: Expr,
  op: CompareOp,
  right: Expr,
): { column: string; op: CompareOp; value: RowGroupStatsValue } | undefined {
  if (left.kind === "column" && right.kind === "literal" && isStatsValue(right.value)) {
    return { column: left.name, op, value: right.value };
  }
  if (right.kind === "column" && left.kind === "literal" && isStatsValue(left.value)) {
    return { column: right.name, op: invertCompareOp(op), value: left.value };
  }
  return undefined;
}

function invertCompareOp(op: CompareOp): CompareOp {
  switch (op) {
    case "eq":
      return "eq";
    case "ne":
      return "ne";
    case "lt":
      return "gt";
    case "lte":
      return "gte";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
  }
}

export function columnStats(
  rowGroup: RowGroup,
  column: string,
): { min: RowGroupStatsValue; max: RowGroupStatsValue; hasNoNulls: boolean } | undefined {
  for (const chunk of rowGroup.columns) {
    const metadata = chunk.meta_data;
    if (!metadata || metadata.path_in_schema.join(".") !== column) continue;
    const min = metadata.statistics?.min_value ?? metadata.statistics?.min;
    const max = metadata.statistics?.max_value ?? metadata.statistics?.max;
    const nullCount = metadata.statistics?.null_count;
    if (isStatsValue(min) && isStatsValue(max)) {
      return {
        min,
        max,
        hasNoNulls: nullCount === 0n,
      };
    }
  }
  return undefined;
}

function isStatsValue(value: unknown): value is RowGroupStatsValue {
  return (
    typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value)) ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  );
}

function sameComparableType(left: RowGroupStatsValue, right: RowGroupStatsValue): boolean {
  if (typeof left === "number" && !Number.isFinite(left)) return false;
  if (typeof right === "number" && !Number.isFinite(right)) return false;
  if (typeof left === typeof right) return true;
  return isLosslessNumberBigIntPair(left, right);
}

function isNumberLike(value: RowGroupStatsValue): value is number | bigint {
  return typeof value === "number" || typeof value === "bigint";
}

function isLosslessNumberBigIntPair(left: RowGroupStatsValue, right: RowGroupStatsValue): boolean {
  if (typeof left === "number" && typeof right === "bigint") return Number.isSafeInteger(left);
  if (typeof left === "bigint" && typeof right === "number") return Number.isSafeInteger(right);
  return false;
}

function compareValues(left: RowGroupStatsValue, right: RowGroupStatsValue): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}
