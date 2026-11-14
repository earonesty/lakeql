import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";
import type { MemoryScope } from "./memory-scope.js";
import { parseOwnedJson } from "./owned-json-parse.js";

export type IndexValue = string | number | bigint | boolean;

export interface MinMaxColumnIndex {
  min: IndexValue;
  max: IndexValue;
  nullCount?: number;
}

export interface BBoxIndex {
  minx: number;
  miny: number;
  maxx: number;
  maxy: number;
}

export interface SidecarFileIndex {
  path: string;
  columns?: Record<string, MinMaxColumnIndex>;
  bbox?: Record<string, BBoxIndex>;
  h3?: Record<string, string[]>;
}

export interface IndexPruneResult<T extends SidecarFileIndex = SidecarFileIndex> {
  planned: T[];
  skipped: T[];
}

export interface IndexPruneOptions<T extends SidecarFileIndex> {
  /** Own result containers; the caller retains the referenced file metadata. */
  scope?: MemoryScope;
  /** Resolve index metadata separately without copying it onto each file. */
  indexForFile?: (file: T) => SidecarFileIndex | undefined;
}

export function pruneFilesWithIndex<T extends SidecarFileIndex>(
  files: T[],
  where: Expr | undefined,
  options: IndexPruneOptions<T> = {},
): IndexPruneResult<T> {
  const output = options.scope?.childScope();
  const scratch = output?.childScope();
  const allocate = <V>(
    scope: MemoryScope | undefined,
    bytes: number,
    purpose: string,
    create: () => V,
  ): V => (scope === undefined ? create() : scope.allocate(bytes, purpose, create).value);
  try {
    // Lazy preparation preserves short-circuit and missing-index behavior.
    let literals: Map<string, unknown> | undefined;
    const parseLiteral = (text: string): unknown => {
      if (literals?.has(text)) return literals.get(text);
      literals ??= allocate(scratch, c.object, "sidecar literal directory", () => new Map());
      scratch?.allocate(c.property + 2 * c.arraySlot, "sidecar literal entry", () => undefined);
      let parsed: unknown;
      if (scratch === undefined) parsed = JSON.parse(text);
      else {
        const owner = parseOwnedJson(text, scratch);
        try {
          parsed = owner.value;
        } finally {
          owner.release();
        }
      }
      literals.set(text, parsed);
      return parsed;
    };
    let ordered = files;
    for (let index = 1; index < files.length; index++) {
      const previous = files[index - 1],
        current = files[index];
      if (
        previous !== undefined &&
        current !== undefined &&
        previous.path.localeCompare(current.path) > 0
      ) {
        ordered = allocate(
          scratch,
          c.array + files.length * c.arraySlot,
          "sidecar ordering input",
          () => [...files],
        );
        const ordering = scratch?.reserve(
          c.array + files.length * 2 * c.arraySlot,
          "sidecar sorting workspace",
        );
        try {
          ordered.sort((a, b) => a.path.localeCompare(b.path));
        } finally {
          ordering?.release();
        }
        break;
      }
    }
    const planned = allocate(output, c.array, "sidecar planned list", () => [] as T[]);
    const skipped = allocate(output, c.array, "sidecar skipped list", () => [] as T[]);
    for (const file of ordered) {
      const index = options.indexForFile === undefined ? file : options.indexForFile(file);
      const target =
        index === undefined || fileMayMatch(index, where, parseLiteral) ? planned : skipped;
      output?.allocate(c.arraySlot, "sidecar result slot", () => undefined);
      target.push(file);
    }
    const result = allocate(output, c.object + 2 * c.property, "sidecar result", () => ({
      planned,
      skipped,
    }));
    if (output !== undefined && options.scope !== undefined) {
      const owner = output.retainAllocations(result);
      try {
        return options.scope.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return result;
  } finally {
    scratch?.dispose();
    output?.dispose();
  }
}

export function bboxMayIntersect(index: BBoxIndex, query: BBoxIndex): boolean {
  validateBBox(index);
  validateBBox(query);
  return (
    index.maxx >= query.minx &&
    index.minx <= query.maxx &&
    index.maxy >= query.miny &&
    index.miny <= query.maxy
  );
}

export function buildMinMaxIndex<T extends Record<string, unknown>>(
  rows: T[],
  columns: string[],
): Record<string, MinMaxColumnIndex> {
  const out: Record<string, MinMaxColumnIndex> = {};
  for (const column of columns) {
    let min: IndexValue | undefined;
    let max: IndexValue | undefined;
    let nullCount = 0;
    for (const row of rows) {
      const value = row[column];
      if (value === null || value === undefined) {
        nullCount += 1;
        continue;
      }
      if (!isIndexValue(value)) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          `Index column ${column} must contain scalar values`,
          {
            column,
          },
        );
      }
      if (min === undefined || compareIndexValues(value, min) < 0) min = value;
      if (max === undefined || compareIndexValues(value, max) > 0) max = value;
    }
    if (min !== undefined && max !== undefined) {
      const entry: MinMaxColumnIndex = { min, max };
      if (nullCount > 0) entry.nullCount = nullCount;
      out[column] = entry;
    }
  }
  return out;
}

export function buildBBoxIndex<T extends Record<string, unknown>>(
  rows: T[],
  columns: { minx: string; miny: string; maxx: string; maxy: string },
): BBoxIndex {
  let minx = Number.POSITIVE_INFINITY;
  let miny = Number.POSITIVE_INFINITY;
  let maxx = Number.NEGATIVE_INFINITY;
  let maxy = Number.NEGATIVE_INFINITY;
  for (const row of rows) {
    const box = rowBBox(row, columns);
    minx = Math.min(minx, box.minx);
    miny = Math.min(miny, box.miny);
    maxx = Math.max(maxx, box.maxx);
    maxy = Math.max(maxy, box.maxy);
  }
  if (!Number.isFinite(minx))
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot index empty bbox rows");
  return { minx, miny, maxx, maxy };
}

function fileMayMatch(
  file: SidecarFileIndex,
  expr: Expr | undefined,
  parseLiteral: (text: string) => unknown,
): boolean {
  if (!expr) return true;
  switch (expr.kind) {
    case "literal":
    case "column":
    case "null-check":
    case "like":
    case "arithmetic":
    case "case":
    case "call":
      return callMayMatch(file, expr, parseLiteral);
    case "not":
      return true;
    case "logical":
      return expr.op === "and"
        ? expr.operands.every((operand) => fileMayMatch(file, operand, parseLiteral))
        : expr.operands.some((operand) => fileMayMatch(file, operand, parseLiteral));
    case "compare":
      return compareMayMatch(file, expr);
    case "between":
      return betweenMayMatch(file, expr);
    case "in":
      return inMayMatch(file, expr);
  }
}

function compareMayMatch(
  file: SidecarFileIndex,
  expr: Extract<Expr, { kind: "compare" }>,
): boolean {
  const pair = columnLiteralPair(expr.left, expr.right);
  if (!pair) return true;
  const stats = file.columns?.[pair.column];
  if (!stats) return true;
  if (!sameComparableType(stats.min, pair.value) || !sameComparableType(stats.max, pair.value)) {
    return true;
  }
  switch (expr.op) {
    case "eq":
      return (
        compareIndexValues(pair.value, stats.min) >= 0 &&
        compareIndexValues(pair.value, stats.max) <= 0
      );
    case "ne":
      return !(
        compareIndexValues(stats.min, pair.value) === 0 &&
        compareIndexValues(stats.max, pair.value) === 0
      );
    case "lt":
      return compareIndexValues(stats.min, pair.value) < 0;
    case "lte":
      return compareIndexValues(stats.min, pair.value) <= 0;
    case "gt":
      return compareIndexValues(stats.max, pair.value) > 0;
    case "gte":
      return compareIndexValues(stats.max, pair.value) >= 0;
  }
}

function betweenMayMatch(
  file: SidecarFileIndex,
  expr: Extract<Expr, { kind: "between" }>,
): boolean {
  if (
    expr.target.kind !== "column" ||
    expr.low.kind !== "literal" ||
    expr.high.kind !== "literal" ||
    !isIndexValue(expr.low.value) ||
    !isIndexValue(expr.high.value)
  ) {
    return true;
  }
  const stats = file.columns?.[expr.target.name];
  if (!stats) return true;
  if (
    !sameComparableType(stats.min, expr.low.value) ||
    !sameComparableType(stats.max, expr.high.value)
  ) {
    return true;
  }
  return (
    compareIndexValues(stats.max, expr.low.value) >= 0 &&
    compareIndexValues(stats.min, expr.high.value) <= 0
  );
}

function inMayMatch(file: SidecarFileIndex, expr: Extract<Expr, { kind: "in" }>): boolean {
  if (expr.negated || expr.target.kind !== "column") return true;
  const stats = file.columns?.[expr.target.name];
  if (!stats) return true;
  return expr.values.some((valueExpr) => {
    if (valueExpr.kind !== "literal" || !isIndexValue(valueExpr.value)) return true;
    if (
      !sameComparableType(stats.min, valueExpr.value) ||
      !sameComparableType(stats.max, valueExpr.value)
    ) {
      return true;
    }
    return (
      compareIndexValues(valueExpr.value, stats.min) >= 0 &&
      compareIndexValues(valueExpr.value, stats.max) <= 0
    );
  });
}

function callMayMatch(
  file: SidecarFileIndex,
  expr: Expr,
  parseLiteral: (text: string) => unknown,
): boolean {
  if (expr.kind !== "call") return true;
  if (expr.fn === "st_intersects") return stIntersectsMayMatch(file, expr, parseLiteral);
  if (expr.fn === "h3_in") return h3InMayMatch(file, expr, parseLiteral);
  if (expr.fn === "h3_within") return h3WithinMayMatch(file, expr);
  return true;
}

function stIntersectsMayMatch(
  file: SidecarFileIndex,
  expr: Extract<Expr, { kind: "call" }>,
  parseLiteral: (text: string) => unknown,
): boolean {
  const [target, query] = expr.args;
  if (target?.kind !== "column" || query?.kind !== "literal" || typeof query.value !== "string")
    return true;
  const index = file.bbox?.[target.name];
  if (!index) return true;
  const parsed = parseBBoxLiteral(parseLiteral(query.value));
  return parsed ? bboxMayIntersect(index, parsed) : true;
}

function h3InMayMatch(
  file: SidecarFileIndex,
  expr: Extract<Expr, { kind: "call" }>,
  parseLiteral: (text: string) => unknown,
): boolean {
  const [target, cells] = expr.args;
  if (target?.kind !== "column" || cells?.kind !== "literal" || typeof cells.value !== "string")
    return true;
  const indexed = file.h3?.[target.name];
  if (!indexed) return true;
  const wanted = parseStringArray(parseLiteral(cells.value));
  return wanted ? wanted.some((cell) => indexed.includes(cell)) : true;
}

function h3WithinMayMatch(file: SidecarFileIndex, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, origin, k] = expr.args;
  if (
    target?.kind !== "column" ||
    origin?.kind !== "literal" ||
    typeof origin.value !== "string" ||
    k?.kind !== "literal" ||
    typeof k.value !== "number" ||
    !Number.isInteger(k.value) ||
    k.value < 0
  ) {
    return true;
  }
  const indexed = file.h3?.[target.name];
  if (!indexed) return true;
  if (k.value > 0) return true;
  return indexed.includes(origin.value);
}

function parseBBoxLiteral(parsed: unknown): BBoxIndex | undefined {
  if (
    typeof parsed === "object" &&
    parsed !== null &&
    "minx" in parsed &&
    "miny" in parsed &&
    "maxx" in parsed &&
    "maxy" in parsed
  ) {
    const box = parsed as Record<string, unknown>;
    if (
      typeof box.minx === "number" &&
      typeof box.miny === "number" &&
      typeof box.maxx === "number" &&
      typeof box.maxy === "number"
    ) {
      return box as unknown as BBoxIndex;
    }
  }
  return undefined;
}

function parseStringArray(parsed: unknown): string[] | undefined {
  return Array.isArray(parsed) && parsed.every((item) => typeof item === "string")
    ? parsed
    : undefined;
}

function columnLiteralPair(
  left: Expr,
  right: Expr,
): { column: string; value: IndexValue } | undefined {
  if (left.kind === "column" && right.kind === "literal" && isIndexValue(right.value)) {
    return { column: left.name, value: right.value };
  }
  if (right.kind === "column" && left.kind === "literal" && isIndexValue(left.value)) {
    return { column: right.name, value: left.value };
  }
  return undefined;
}

function rowBBox(
  row: Record<string, unknown>,
  columns: { minx: string; miny: string; maxx: string; maxy: string },
): BBoxIndex {
  const minx = numberColumn(row, columns.minx);
  const miny = numberColumn(row, columns.miny);
  const maxx = numberColumn(row, columns.maxx);
  const maxy = numberColumn(row, columns.maxy);
  return { minx, miny, maxx, maxy };
}

function numberColumn(row: Record<string, unknown>, column: string): number {
  const value = row[column];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `BBox column ${column} must be a finite number`, {
      column,
    });
  }
  return value;
}

function validateBBox(box: BBoxIndex): void {
  if (
    !Number.isFinite(box.minx) ||
    !Number.isFinite(box.miny) ||
    !Number.isFinite(box.maxx) ||
    !Number.isFinite(box.maxy) ||
    box.minx > box.maxx ||
    box.miny > box.maxy
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "BBox index bounds must be finite and ordered", {
      box,
    });
  }
}

function isIndexValue(value: unknown): value is IndexValue {
  return (
    typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value)) ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  );
}

function sameComparableType(left: IndexValue, right: IndexValue): boolean {
  if (typeof left === "number" && !Number.isFinite(left)) return false;
  if (typeof right === "number" && !Number.isFinite(right)) return false;
  if (typeof left === typeof right) return true;
  return isLosslessNumberBigIntPair(left, right);
}

function isLosslessNumberBigIntPair(left: IndexValue, right: IndexValue): boolean {
  if (typeof left === "number" && typeof right === "bigint") return Number.isSafeInteger(left);
  if (typeof left === "bigint" && typeof right === "number") return Number.isSafeInteger(right);
  return false;
}

function compareIndexValues(left: IndexValue, right: IndexValue): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}
