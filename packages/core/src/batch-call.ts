import type { BatchExprValues } from "./batch.js";
import { LakeqlError } from "./errors.js";
import {
  type BBox,
  bboxIntersects,
  envelopeFromGeometry,
  envelopeOf,
  type GeoJsonGeometry,
  parseGeometry,
  requireGeoBackend,
  toGeometry,
} from "./evaluator.js";
import type { Expr, Scalar } from "./expr.js";
import { regexpMatchesValue, regexpReplaceValue } from "./regex-functions.js";

export function batchCallExprValues(
  rowCount: number,
  name: string,
  args: BatchExprValues[],
  compareEq: (left: Scalar, right: Scalar) => boolean | null,
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
): Uint8Array | undefined {
  switch (name.toLowerCase()) {
    case "h3_in":
      return h3InMask(args);
    case "h3_within":
      return h3WithinMask(args);
    case "st_intersects":
      return stIntersectsMask(args);
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

function h3InMask(args: BatchExprValues[]): Uint8Array | undefined {
  const cell = args[0];
  const list = args[1]?.literal;
  if (cell?.vector !== undefined && list === null) return nullMask(cell.rowCount);
  if (cell?.vector === undefined || typeof list !== "string") return undefined;
  const parsed: unknown = JSON.parse(list);
  if (!Array.isArray(parsed) || !parsed.every((value) => typeof value === "string")) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_in() cell list must be a JSON string array");
  }
  const cells = new Set(parsed);
  return stringVectorMask(cell.vector, (value) => cells.has(value));
}

function h3WithinMask(args: BatchExprValues[]): Uint8Array | undefined {
  const cell = args[0];
  const origin = args[1]?.literal;
  const radius = args[2]?.literal;
  if (cell?.vector !== undefined && (origin === null || radius === null))
    return nullMask(cell.rowCount);
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
  return stringVectorMask(cell.vector, (value) => {
    if (!geo.isValidCell(value)) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3 cell cell is invalid", { cell: value });
    }
    return cells.has(value);
  });
}

function stIntersectsMask(args: BatchExprValues[]): Uint8Array | undefined {
  const left = args[0];
  const right = args[1];
  if (left?.vector !== undefined && right !== undefined) {
    if (right.literal === null) return nullMask(left.rowCount);
    const constant = spatialConstant(right);
    if (constant !== undefined) return geometryVectorIntersectsMask(left.vector, constant, false);
  }
  if (right?.vector !== undefined && left !== undefined) {
    if (left.literal === null) return nullMask(right.rowCount);
    const constant = spatialConstant(left);
    if (constant !== undefined) return geometryVectorIntersectsMask(right.vector, constant, true);
  }
  return undefined;
}

interface SpatialConstant {
  geometry: GeoJsonGeometry;
  envelope: BBox;
}

function spatialConstant(values: BatchExprValues): SpatialConstant | undefined {
  if (typeof values.literal !== "string") return undefined;
  const geometry = toGeometry(parseGeometry(values.literal, "st_intersects"), "st_intersects");
  return { geometry, envelope: envelopeOf(geometry) };
}

function geometryVectorIntersectsMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  constant: SpatialConstant,
  constantFirst: boolean,
): Uint8Array {
  const geo = requireGeoBackend();
  return stringVectorMask(vector, (value) => {
    const parsed = parseGeometry(value, "st_intersects");
    const envelope = envelopeFromGeometry(parsed, "st_intersects");
    if (!bboxIntersects(envelope, constant.envelope)) return false;
    const geometry = toGeometry(parsed, "st_intersects");
    return constantFirst
      ? geo.booleanIntersects(constant.geometry, geometry)
      : geo.booleanIntersects(geometry, constant.geometry);
  });
}

function stringVectorMask(
  vector: NonNullable<BatchExprValues["vector"]>,
  predicate: (value: string) => boolean,
): Uint8Array {
  switch (vector.type) {
    case "null":
      return nullMask(vector.length);
    case "utf8":
      return utf8VectorMask(vector, predicate);
    case "dict":
      return dictStringVectorMask(vector, predicate);
    default:
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector spatial/H3 predicate requires strings", {
        vectorType: vector.type,
      });
  }
}

function utf8VectorMask(
  vector: Extract<NonNullable<BatchExprValues["vector"]>, { type: "utf8" }>,
  predicate: (value: string) => boolean,
): Uint8Array {
  const mask = new Uint8Array(vector.values.length);
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
): Uint8Array {
  if (vector.dictionary.type !== "utf8") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Dictionary spatial/H3 predicate requires strings", {
      vectorType: vector.dictionary.type,
    });
  }
  const dictionaryMask = utf8VectorMask(vector.dictionary, predicate);
  const mask = new Uint8Array(vector.indices.length);
  const valid = vector.valid;
  for (let index = 0; index < vector.indices.length; index += 1) {
    if (valid !== undefined && valid[index] === 0) {
      mask[index] = 2;
      continue;
    }
    mask[index] = dictionaryMask[vector.indices[index] ?? 0] ?? 2;
  }
  return mask;
}

function nullMask(rowCount: number): Uint8Array {
  const mask = new Uint8Array(rowCount);
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

function spatialConstantExpr(expr: Expr | undefined): boolean {
  if (literalStringExpr(expr)) return true;
  return (
    expr?.kind === "call" &&
    expr.fn.toLowerCase() === "st_bbox" &&
    expr.args.length === 4 &&
    expr.args.every(literalNumberExpr)
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

function coalesceCallValues(rowCount: number, args: BatchExprValues[]): BatchExprValues {
  return {
    rowCount,
    valueAt(index) {
      return args.map((arg) => arg.valueAt(index)).find((value) => value !== null) ?? null;
    },
  };
}

function nullifCallValues(
  rowCount: number,
  args: BatchExprValues[],
  compareEq: (left: Scalar, right: Scalar) => boolean | null,
): BatchExprValues {
  requireVectorArgCount("nullif", args, 2);
  return {
    rowCount,
    valueAt(index) {
      const left = args[0]?.valueAt(index) ?? null;
      const right = args[1]?.valueAt(index) ?? null;
      return compareEq(left, right) === true ? null : left;
    },
  };
}

function unaryStringCallValues(
  name: string,
  args: BatchExprValues[],
  cb: (value: string) => string,
): BatchExprValues {
  requireVectorArgCount(name, args, 1);
  const source = args[0];
  return {
    rowCount: source?.rowCount ?? 0,
    valueAt(index) {
      const value = source?.valueAt(index) ?? null;
      if (value === null) return null;
      if (typeof value !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects string`, {
          expected: "string",
          actual: typeof value,
        });
      }
      return cb(value);
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
    valueAt(index) {
      const value = source?.valueAt(index) ?? null;
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
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const start = startArg?.valueAt(index) ?? null;
      const length = lengthArg?.valueAt(index) ?? null;
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
      return value.slice(start, start + length);
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
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const search = searchArg?.valueAt(index) ?? null;
      const replacement = replacementArg?.valueAt(index) ?? null;
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
      return value.replaceAll(search, replacement);
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
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const pattern = patternArg?.valueAt(index) ?? null;
      const options = optionsArg?.valueAt(index) ?? "";
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
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const pattern = patternArg?.valueAt(index) ?? null;
      const replacement = replacementArg?.valueAt(index) ?? null;
      const options = optionsArg?.valueAt(index) ?? "";
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
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      if (value === null) return null;
      const target = targetArg?.valueAt(index) ?? null;
      if (typeof target !== "string") {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "cast() expects string type name", {
          expected: "string type name",
          actual: typeof target,
        });
      }
      switch (target) {
        case "string":
          return String(value);
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
    valueAt(index) {
      const value = valueArg?.valueAt(index) ?? null;
      const places = placesArg?.valueAt(index) ?? 0;
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
