import { type BatchExprValues, type PackedByteVector, vectorValue } from "./batch.js";
import { expandDictionaryMask } from "./dictionary-mask.js";
import { LakeqlError } from "./errors.js";
import {
  type BBox,
  bboxDistance,
  bboxIntersects,
  envelopeFromGeometry,
  envelopeOf,
  type GeoJsonGeometry,
  geometryDistance,
  parseGeometry,
  requireGeoBackend,
  toGeometry,
} from "./evaluator.js";
import type { Expr, Scalar } from "./expr.js";
import type { MemoryScope } from "./memory-scope.js";
import { allocatePredicateMask } from "./predicate-mask.js";
import { regexpMatchesValue, regexpReplaceValue } from "./regex-functions.js";
import { scalarStringWithAdmission } from "./scalar-cast-allocation.js";
import { replaceAllWithAdmission } from "./string-replacement-allocation.js";
import { transformStringWithAdmission } from "./string-transform-allocation.js";
import { admitVectorBuffer } from "./vector-buffer.js";

export function batchCallExprValues(
  rowCount: number,
  name: string,
  args: BatchExprValues[],
  compareEq: (left: Scalar, right: Scalar, scope?: MemoryScope) => boolean | null,
): BatchExprValues {
  const fn = name.toLowerCase();
  switch (fn) {
    case "lower":
      return unaryStringCallValues(fn, args, (value) => value.toLowerCase());
    case "upper":
      return unaryStringCallValues(fn, args, (value) => value.toUpperCase());
    case "trim":
      return unaryStringCallValues(fn, args, (value) => value.trim());
    case "substr":
      return substrCallValues(args);
    case "replace":
      return replaceCallValues(args);
    case "regexp_matches":
      return regexpMatchesCallValues(args);
    case "regexp_replace":
      return regexpReplaceCallValues(args);
    case "cast":
      return castCallValues(args);
    case "round":
      return roundCallValues(args);
    case "floor":
      return unaryNumberCallValues(fn, args, Math.floor);
    case "ceil":
      return unaryNumberCallValues(fn, args, Math.ceil);
    case "abs":
      return unaryNumberCallValues(fn, args, Math.abs);
    case "coalesce":
      return coalesceCallValues(rowCount, args);
    case "nullif":
      return nullifCallValues(rowCount, args, compareEq);
    case "st_bbox":
      return stBBoxCallValues(args);
    case "st_point":
      return stPointCallValues(args);
    default:
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PUSHDOWN",
        "Vector predicate evaluation does not support call expressions",
        { kind: "call" },
      );
  }
}

export function batchCallPredicateMask(
  name: string,
  args: BatchExprValues[],
  scope?: MemoryScope,
): Uint8Array | undefined {
  switch (name.toLowerCase()) {
    case "h3_in":
      return h3InMask(args, scope);
    case "h3_within":
      return h3WithinMask(args, scope);
    case "st_intersects":
      return stIntersectsMask(args, scope);
    case "st_contains":
      return stContainsMask(args, scope);
    case "st_within":
      return stWithinMask(args, scope);
    case "st_dwithin":
      return stDWithinMask(args, scope);
    default:
      return undefined;
  }
}

export function vectorCallExprSupported(expr: Extract<Expr, { kind: "call" }>): boolean {
  switch (expr.fn.toLowerCase()) {
    case "lower":
    case "upper":
    case "trim":
    case "substr":
    case "replace":
    case "regexp_matches":
    case "regexp_replace":
    case "cast":
    case "round":
    case "floor":
    case "ceil":
    case "abs":
    case "coalesce":
    case "nullif":
      return true;
    case "st_bbox":
      return (
        expr.args.length === 4 &&
        expr.args.every((arg) => arg.kind === "literal" && typeof arg.value === "number")
      );
    case "st_point":
      return (
        expr.args.length === 2 &&
        expr.args.every((arg) => arg.kind === "literal" && typeof arg.value === "number")
      );
    case "h3_in":
      return (
        expr.args.length === 2 && expr.args[0]?.kind === "column" && literalStringExpr(expr.args[1])
      );
    case "h3_within":
      return (
        expr.args.length === 3 &&
        expr.args[0]?.kind === "column" &&
        literalStringExpr(expr.args[1]) &&
        literalNonNegativeIntegerExpr(expr.args[2])
      );
    case "st_intersects":
      return stIntersectsVectorShape(expr);
    case "st_contains":
    case "st_within":
      return stBinarySpatialVectorShape(expr);
    case "st_dwithin":
      return stDWithinVectorShape(expr);
    default:
      return false;
  }
}

function stBBoxCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("st_bbox", args, 4);
  const minx = args[0]?.literal;
  const miny = args[1]?.literal;
  const maxx = args[2]?.literal;
  const maxy = args[3]?.literal;
  if (
    typeof minx !== "number" ||
    typeof miny !== "number" ||
    typeof maxx !== "number" ||
    typeof maxy !== "number"
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_bbox() vector kernel requires literal bounds");
  }
  if (
    !Number.isFinite(minx) ||
    !Number.isFinite(miny) ||
    !Number.isFinite(maxx) ||
    !Number.isFinite(maxy)
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_bbox() expects finite number bounds");
  }
  if (minx > maxx || miny > maxy) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_bbox() bounds must be ordered min <= max");
  }
  const value = JSON.stringify({ type: "BBox", minx, miny, maxx, maxy });
  return { rowCount: args[0]?.rowCount ?? 0, literal: value, valueAt: () => value };
}

function stPointCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("st_point", args, 2);
  const lon = args[0]?.literal;
  const lat = args[1]?.literal;
  if (typeof lon !== "number" || typeof lat !== "number") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_point() vector kernel requires literal lon/lat");
  }
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_point() expects finite lon/lat numbers");
  }
  const value = JSON.stringify({ type: "Point", coordinates: [lon, lat] });
  return { rowCount: args[0]?.rowCount ?? 0, literal: value, valueAt: () => value };
}

function h3InMask(args: BatchExprValues[], scope?: MemoryScope): Uint8Array | undefined {
  const cell = args[0];
  const list = args[1]?.literal;
  if (cell?.vector !== undefined && list === null) return nullMask(cell.rowCount, scope);
  if (cell?.vector === undefined || typeof list !== "string") return undefined;
  const parsed: unknown = JSON.parse(list);
  if (!Array.isArray(parsed) || !parsed.every((value) => typeof value === "string")) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_in() cell list must be a JSON string array");
  }
  const cells = new Set(parsed);
  return stringVectorMask(cell.vector, (value) => cells.has(value), scope);
}

function h3WithinMask(args: BatchExprValues[], scope?: MemoryScope): Uint8Array | undefined {
  const cell = args[0];
  const origin = args[1]?.literal;
  const radius = args[2]?.literal;
  if (cell?.vector !== undefined && (origin === null || radius === null))
    return nullMask(cell.rowCount, scope);
  if (cell?.vector === undefined || typeof origin !== "string" || typeof radius !== "number") {
    return undefined;
  }
  if (!Number.isInteger(radius) || radius < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_within() expects a non-negative integer radius");
  }
  const geo = requireGeoBackend();
  if (!geo.isValidCell(origin)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3 cell origin is invalid", { cell: origin });
  }
  const cells = new Set(geo.gridDisk(origin, radius));
  return stringVectorMask(
    cell.vector,
    (value) => {
      if (!geo.isValidCell(value)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3 cell cell is invalid", { cell: value });
      }
      return cells.has(value);
    },
    scope,
  );
}

function stIntersectsMask(args: BatchExprValues[], scope?: MemoryScope): Uint8Array | undefined {
  const left = args[0];
  const right = args[1];
  if (left?.vector !== undefined && right !== undefined) {
    if (right.literal === null) return nullMask(left.rowCount, scope);
    const constant = spatialConstant(right);
    if (constant !== undefined)
      return geometryVectorIntersectsMask(left.vector, constant, false, scope);
  }
  if (right?.vector !== undefined && left !== undefined) {
    if (left.literal === null) return nullMask(right.rowCount, scope);
    const constant = spatialConstant(left);
    if (constant !== undefined)
      return geometryVectorIntersectsMask(right.vector, constant, true, scope);
  }
  return undefined;
}

function stDWithinMask(args: BatchExprValues[], scope?: MemoryScope): Uint8Array | undefined {
  const left = args[0];
  const right = args[1];
  const distance = args[2]?.literal;
  if (distance === null && (left?.vector !== undefined || right?.vector !== undefined)) {
    return nullMask(left?.rowCount ?? right?.rowCount ?? 0, scope);
  }
  if (typeof distance !== "number") return undefined;
  if (!Number.isFinite(distance) || distance < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_dwithin() expects a non-negative distance");
  }
  if (left?.vector !== undefined && right !== undefined) {
    if (right.literal === null) return nullMask(left.rowCount, scope);
    const constant = spatialConstant(right, "st_dwithin");
    if (constant !== undefined) {
      return geometryVectorDWithinMask(left.vector, constant, distance, scope);
    }
  }
  if (right?.vector !== undefined && left !== undefined) {
    if (left.literal === null) return nullMask(right.rowCount, scope);
    const constant = spatialConstant(left, "st_dwithin");
    if (constant !== undefined) {
      return geometryVectorDWithinMask(right.vector, constant, distance, scope);
    }
  }
  return undefined;
}

function stContainsMask(args: BatchExprValues[], scope?: MemoryScope): Uint8Array | undefined {
  const left = args[0];
  const right = args[1];
  if (left?.vector !== undefined && right !== undefined) {
    if (right.literal === null) return nullMask(left.rowCount, scope);
    const constant = spatialConstant(right, "st_contains");
    if (constant !== undefined) {
      return geometryVectorContainsMask(left.vector, constant, false, "st_contains", scope);
    }
  }
  if (right?.vector !== undefined && left !== undefined) {
    if (left.literal === null) return nullMask(right.rowCount, scope);
    const constant = spatialConstant(left, "st_contains");
    if (constant !== undefined) {
      return geometryVectorContainsMask(right.vector, constant, true, "st_contains", scope);
    }
  }
  return undefined;
}

function stWithinMask(args: BatchExprValues[], scope?: MemoryScope): Uint8Array | undefined {
  const left = args[0];
  const right = args[1];
  if (left?.vector !== undefined && right !== undefined) {
    if (right.literal === null) return nullMask(left.rowCount, scope);
    const constant = spatialConstant(right, "st_within");
    if (constant !== undefined) {
      return geometryVectorContainsMask(left.vector, constant, true, "st_within", scope);
    }
  }
  if (right?.vector !== undefined && left !== undefined) {
    if (left.literal === null) return nullMask(right.rowCount, scope);
    const constant = spatialConstant(left, "st_within");
    if (constant !== undefined) {
      return geometryVectorContainsMask(right.vector, constant, false, "st_within", scope);
    }
  }
  return undefined;
}

interface SpatialConstant {
  geometry: GeoJsonGeometry;
  envelope: BBox;
}

function spatialConstant(
  values: BatchExprValues,
  name = "st_intersects",
): SpatialConstant | undefined {
  if (typeof values.literal !== "string" && !(values.literal instanceof Uint8Array)) {
    return undefined;
  }
  const geometry = toGeometry(parseGeometry(values.literal, name), name);
  return { geometry, envelope: envelopeOf(geometry) };
}

function geometryVectorIntersectsMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  constant: SpatialConstant,
  constantFirst: boolean,
  scope?: MemoryScope,
): Uint8Array {
  const geo = requireGeoBackend();
  return geometryVectorMask(
    vector,
    (value) => {
      const parsed = parseGeometry(value, "st_intersects");
      const envelope = envelopeFromGeometry(parsed, "st_intersects");
      if (!bboxIntersects(envelope, constant.envelope)) return false;
      const geometry = toGeometry(parsed, "st_intersects");
      return constantFirst
        ? geo.booleanIntersects(constant.geometry, geometry)
        : geo.booleanIntersects(geometry, constant.geometry);
    },
    scope,
  );
}

function geometryVectorContainsMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  constant: SpatialConstant,
  constantContainsValue: boolean,
  name: string,
  scope?: MemoryScope,
): Uint8Array {
  const geo = requireGeoBackend();
  return geometryVectorMask(
    vector,
    (value) => {
      const parsed = parseGeometry(value, name);
      const envelope = envelopeFromGeometry(parsed, name);
      if (constantContainsValue) {
        if (!bboxContainsLocal(constant.envelope, envelope)) return false;
        return geo.booleanContains(constant.geometry, toGeometry(parsed, name));
      }
      if (!bboxContainsLocal(envelope, constant.envelope)) return false;
      return geo.booleanContains(toGeometry(parsed, name), constant.geometry);
    },
    scope,
  );
}

function geometryVectorDWithinMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  constant: SpatialConstant,
  distance: number,
  scope?: MemoryScope,
): Uint8Array {
  return geometryVectorMask(
    vector,
    (value) => {
      const parsed = parseGeometry(value, "st_dwithin");
      const envelope = envelopeFromGeometry(parsed, "st_dwithin");
      if (bboxDistance(envelope, constant.envelope) > distance) return false;
      const geometry = toGeometry(parsed, "st_dwithin");
      return geometryDistance(geometry, constant.geometry) <= distance;
    },
    scope,
  );
}

function bboxContainsLocal(left: BBox, right: BBox): boolean {
  return (
    left.minx <= right.minx &&
    left.miny <= right.miny &&
    left.maxx >= right.maxx &&
    left.maxy >= right.maxy
  );
}

function geometryVectorMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  predicate: (value: string | Uint8Array) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  switch (vector.type) {
    case "null":
      return nullMask(vector.length, scope);
    case "utf8-view":
      return packedPredicateMask(vector, (value) => predicate(value as string), scope);
    case "utf8":
      return utf8VectorMask(vector, predicate, scope);
    case "binary-view":
      return packedPredicateMask(vector, predicate, scope);
    case "binary":
      return binaryVectorMask(vector, predicate, scope);
    case "dict":
      return dictGeometryVectorMask(vector, predicate, scope);
    default:
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Vector spatial predicate requires geometry values",
        {
          vectorType: vector.type,
        },
      );
  }
}

function stringVectorMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  predicate: (value: string) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  switch (vector.type) {
    case "null":
      return nullMask(vector.length, scope);
    case "utf8-view":
      return packedPredicateMask(vector, (value) => predicate(value as string), scope);
    case "utf8":
      return utf8VectorMask(vector, predicate, scope);
    case "dict":
      return dictStringVectorMask(vector, predicate, scope);
    default:
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector spatial/H3 predicate requires strings", {
        vectorType: vector.type,
      });
  }
}

function binaryVectorMask(
  vector: Extract<NonNullable<BatchExprValues["vector"]>, { type: "binary" }>,
  predicate: (value: Uint8Array) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  const mask = allocatePredicateMask(vector.values.length, scope);
  const valid = vector.valid;
  for (let index = 0; index < vector.values.length; index += 1) {
    if (valid !== undefined && valid[index] === 0) {
      mask[index] = 2;
      continue;
    }
    mask[index] = predicate(
      vector.values[index] ?? admitVectorBuffer(scope, 0, () => new Uint8Array()),
    )
      ? 1
      : 0;
  }
  return mask;
}

function dictGeometryVectorMask(
  vector: Extract<NonNullable<BatchExprValues["vector"]>, { type: "dict" }>,
  predicate: (value: string | Uint8Array) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  if (!["utf8", "binary", "utf8-view", "binary-view"].includes(vector.dictionary.type)) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Dictionary spatial predicate requires geometry values",
      {
        vectorType: vector.dictionary.type,
      },
    );
  }
  const dictionaryMask = geometryVectorMask(vector.dictionary, predicate, scope);
  return expandDictionaryMask(vector, dictionaryMask, scope);
}

function utf8VectorMask(
  vector: Extract<NonNullable<BatchExprValues["vector"]>, { type: "utf8" }>,
  predicate: (value: string) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  const mask = allocatePredicateMask(vector.values.length, scope);
  const valid = vector.valid;
  for (let index = 0; index < vector.values.length; index += 1) {
    if (valid !== undefined && valid[index] === 0) {
      mask[index] = 2;
      continue;
    }
    mask[index] = predicate(vector.values[index] ?? "") ? 1 : 0;
  }
  return mask;
}

function dictStringVectorMask(
  vector: Extract<NonNullable<BatchExprValues["vector"]>, { type: "dict" }>,
  predicate: (value: string) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  if (vector.dictionary.type !== "utf8" && vector.dictionary.type !== "utf8-view") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Dictionary spatial/H3 predicate requires strings", {
      vectorType: vector.dictionary.type,
    });
  }
  const dictionaryMask = stringVectorMask(vector.dictionary, predicate, scope);
  return expandDictionaryMask(vector, dictionaryMask, scope);
}

function nullMask(rowCount: number, scope?: MemoryScope): Uint8Array {
  const mask = allocatePredicateMask(rowCount, scope);
  mask.fill(2);
  return mask;
}

function stIntersectsVectorShape(expr: Extract<Expr, { kind: "call" }>): boolean {
  if (expr.args.length !== 2) return false;
  const left = expr.args[0];
  const right = expr.args[1];
  return (
    (left?.kind === "column" && spatialConstantExpr(right)) ||
    (right?.kind === "column" && spatialConstantExpr(left))
  );
}

function stDWithinVectorShape(expr: Extract<Expr, { kind: "call" }>): boolean {
  if (expr.args.length !== 3 || !literalNonNegativeNumberExpr(expr.args[2])) return false;
  const left = expr.args[0];
  const right = expr.args[1];
  return (
    (left?.kind === "column" && spatialConstantExpr(right)) ||
    (right?.kind === "column" && spatialConstantExpr(left))
  );
}

function stBinarySpatialVectorShape(expr: Extract<Expr, { kind: "call" }>): boolean {
  if (expr.args.length !== 2) return false;
  const left = expr.args[0];
  const right = expr.args[1];
  return (
    (left?.kind === "column" && spatialConstantExpr(right)) ||
    (right?.kind === "column" && spatialConstantExpr(left))
  );
}

function spatialConstantExpr(expr: Expr | undefined): boolean {
  if (literalGeometryExpr(expr)) return true;
  return (
    expr?.kind === "call" &&
    ((expr.fn.toLowerCase() === "st_bbox" &&
      expr.args.length === 4 &&
      expr.args.every(literalNumberExpr)) ||
      (expr.fn.toLowerCase() === "st_point" &&
        expr.args.length === 2 &&
        expr.args.every(literalNumberExpr)))
  );
}

function literalGeometryExpr(expr: Expr | undefined): boolean {
  return (
    expr?.kind === "literal" && (typeof expr.value === "string" || expr.value instanceof Uint8Array)
  );
}

function literalStringExpr(expr: Expr | undefined): boolean {
  return expr?.kind === "literal" && typeof expr.value === "string";
}

function literalNumberExpr(expr: Expr | undefined): boolean {
  return expr?.kind === "literal" && typeof expr.value === "number";
}

function literalNonNegativeIntegerExpr(expr: Expr | undefined): boolean {
  return (
    expr?.kind === "literal" &&
    typeof expr.value === "number" &&
    Number.isInteger(expr.value) &&
    expr.value >= 0
  );
}

function literalNonNegativeNumberExpr(expr: Expr | undefined): boolean {
  return (
    expr?.kind === "literal" &&
    typeof expr.value === "number" &&
    Number.isFinite(expr.value) &&
    expr.value >= 0
  );
}

function coalesceCallValues(rowCount: number, args: BatchExprValues[]): BatchExprValues {
  return {
    rowCount,
    valueAt(index, valueScope) {
      let result: Scalar = null;
      for (const arg of args) {
        const scratch = arg.allocationFree ? undefined : valueScope?.childScope();
        let retained = false;
        try {
          const value = arg.valueAt(index, scratch);
          if (result === null && value !== null) {
            result = value;
            retained = true;
          }
        } finally {
          // Preserve eager evaluation, but only the selected argument can escape.
          if (!retained) scratch?.dispose();
        }
      }
      return result;
    },
  };
}

function nullifCallValues(
  rowCount: number,
  args: BatchExprValues[],
  compareEq: (left: Scalar, right: Scalar, scope?: MemoryScope) => boolean | null,
): BatchExprValues {
  requireVectorArgCount("nullif", args, 2);
  return {
    rowCount,
    valueAt(index, valueScope) {
      const leftScope = args[0]?.allocationFree ? undefined : valueScope?.childScope();
      const rightScope = args[1]?.allocationFree ? undefined : valueScope?.childScope();
      let retained = false;
      try {
        const left = args[0]?.valueAt(index, leftScope) ?? null;
        const right = args[1]?.valueAt(index, rightScope) ?? null;
        if (compareEq(left, right, valueScope) === true) return null;
        retained = left !== null;
        return left;
      } finally {
        rightScope?.dispose();
        if (!retained) leftScope?.dispose();
      }
    },
  };
}

function unaryStringCallValues(
  name: "lower" | "upper" | "trim",
  args: BatchExprValues[],
  cb: (value: string) => string,
): BatchExprValues {
  requireVectorArgCount(name, args, 1);
  const source = args[0];
  return {
    rowCount: source?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = source?.valueAt(index, valueScope) ?? null;
      if (value === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects string`, {
          expected: "string",
          actual: typeof value,
        });
      }
      return transformStringWithAdmission(value, name, cb, valueScope);
    },
  };
}

function unaryNumberCallValues(
  name: string,
  args: BatchExprValues[],
  cb: (value: number) => number,
): BatchExprValues {
  requireVectorArgCount(name, args, 1);
  const source = args[0];
  return {
    rowCount: source?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = source?.valueAt(index, valueScope) ?? null;
      if (value === null) return null;
      if (typeof value !== "number") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects number`, {
          expected: "number",
          actual: typeof value,
        });
      }
      return cb(value);
    },
  };
}

function substrCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("substr", args, 3);
  const valueArg = args[0];
  const startArg = args[1];
  const lengthArg = args[2];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = valueArg?.valueAt(index, valueScope) ?? null;
      const start = startArg?.valueAt(index, valueScope) ?? null;
      const length = lengthArg?.valueAt(index, valueScope) ?? null;
      if (value === null || start === null || length === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "substr() expects string", {
          expected: "string",
          actual: typeof value,
        });
      }
      if (typeof start !== "number" || typeof length !== "number") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "substr() start and length must be numbers");
      }
      return transformStringWithAdmission(
        value,
        "substr",
        (text) => text.slice(start, start + length),
        valueScope,
      );
    },
  };
}

function replaceCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("replace", args, 3);
  const valueArg = args[0];
  const searchArg = args[1];
  const replacementArg = args[2];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = valueArg?.valueAt(index, valueScope) ?? null;
      const search = searchArg?.valueAt(index, valueScope) ?? null;
      const replacement = replacementArg?.valueAt(index, valueScope) ?? null;
      if (value === null || search === null || replacement === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "replace() expects string", {
          expected: "string",
          actual: typeof value,
        });
      }
      if (typeof search !== "string" || typeof replacement !== "string") {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "replace() search and replacement must be strings",
        );
      }
      return replaceAllWithAdmission(value, search, replacement, valueScope);
    },
  };
}

function regexpMatchesCallValues(args: BatchExprValues[]): BatchExprValues {
  if (args.length < 2 || args.length > 3) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "regexp_matches() expects 2 or 3 arguments", {
      received: args.length,
    });
  }
  const valueArg = args[0];
  const patternArg = args[1];
  const optionsArg = args[2];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = valueArg?.valueAt(index, valueScope) ?? null;
      const pattern = patternArg?.valueAt(index, valueScope) ?? null;
      const options = optionsArg?.valueAt(index, valueScope) ?? "";
      if (value === null || pattern === null || options === null) return null;
      if (typeof value !== "string" || typeof pattern !== "string" || typeof options !== "string") {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "regexp_matches() value, pattern, and options must be strings",
        );
      }
      return regexpMatchesValue(value, pattern, options);
    },
  };
}

function regexpReplaceCallValues(args: BatchExprValues[]): BatchExprValues {
  if (args.length < 3 || args.length > 4) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "regexp_replace() expects 3 or 4 arguments", {
      received: args.length,
    });
  }
  const valueArg = args[0];
  const patternArg = args[1];
  const replacementArg = args[2];
  const optionsArg = args[3];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = valueArg?.valueAt(index, valueScope) ?? null;
      const pattern = patternArg?.valueAt(index, valueScope) ?? null;
      const replacement = replacementArg?.valueAt(index, valueScope) ?? null;
      const options = optionsArg?.valueAt(index, valueScope) ?? "";
      if (value === null || pattern === null || replacement === null || options === null) {
        return null;
      }
      if (
        typeof value !== "string" ||
        typeof pattern !== "string" ||
        typeof replacement !== "string" ||
        typeof options !== "string"
      ) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "regexp_replace() value, pattern, replacement, and options must be strings",
        );
      }
      return regexpReplaceValue(value, pattern, replacement, options);
    },
  };
}

function castCallValues(args: BatchExprValues[]): BatchExprValues {
  requireVectorArgCount("cast", args, 2);
  const valueArg = args[0];
  const targetArg = args[1];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = valueArg?.valueAt(index, valueScope) ?? null;
      if (value === null) return null;
      const target = targetArg?.valueAt(index, valueScope) ?? null;
      if (typeof target !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "cast() expects string type name", {
          expected: "string type name",
          actual: typeof target,
        });
      }
      switch (target) {
        case "string":
          return scalarStringWithAdmission(value, valueScope);
        case "float64":
        case "number": {
          const number = Number(value);
          return Number.isNaN(number) ? null : number;
        }
        case "boolean":
          return Boolean(value);
        default:
          throw new LakeqlError("LAKEQL_TYPE_ERROR", `Unsupported cast target ${target}`, {
            target,
          });
      }
    },
  };
}

function roundCallValues(args: BatchExprValues[]): BatchExprValues {
  if (args.length < 1 || args.length > 2) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "round() expects 1 or 2 arguments", {
      received: args.length,
    });
  }
  const valueArg = args[0];
  const placesArg = args[1];
  return {
    rowCount: valueArg?.rowCount ?? 0,
    valueAt(index, valueScope) {
      const value = valueArg?.valueAt(index, valueScope) ?? null;
      const places = placesArg?.valueAt(index, valueScope) ?? 0;
      if (value === null) return null;
      if (typeof value !== "number" || typeof places !== "number") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "round() arguments must be numbers");
      }
      const scale = 10 ** places;
      return Math.round(value * scale) / scale;
    },
  };
}

function requireVectorArgCount(name: string, args: unknown[], expected: number): void {
  if (args.length !== expected) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects ${expected} arguments`, {
      expected,
      received: args.length,
    });
  }
}

function packedPredicateMask(
  vector: PackedByteVector,
  predicate: (value: string | Uint8Array) => boolean,
  scope?: MemoryScope,
): Uint8Array {
  const mask = allocatePredicateMask(vector.length, scope);
  for (let row = 0; row < vector.length; row++) {
    const value = vectorValue(vector, row);
    mask[row] = value === null ? 2 : predicate(value as string | Uint8Array) ? 1 : 0;
  }
  return mask;
}
