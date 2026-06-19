import { LakeqlError } from "./errors.js";
import type { Expr, Scalar } from "./expr.js";
import { regexpMatchesValue, regexpReplaceValue } from "./regex-functions.js";
import type { Row } from "./types.js";

export type SqlBoolean = boolean | null;

// --- Lazy geospatial backend -------------------------------------------------
//
// The heavy geospatial libraries (@turf/* ~1MB, h3-js ~6.5MB) used to be
// statically imported here. Because the evaluator is on every query's hot path,
// that pulled turf + h3-js into the bundle of *every* consumer — even one that
// only runs `SELECT … FROM parquet`. They now live in `./geo-backend.ts`, which
// is dynamically imported (see `ensureGeoBackendForExprs`) the first time a
// query uses a spatial function that needs exact geometry or H3 indexing. Pure
// bbox/JSON spatial helpers (st_point, st_bbox, st_distance, st_area, h3_in, …)
// need no backend and keep working with zero extra deps.

/** External geometry/H3 primitives the evaluator can't compute from bbox math alone. */
export interface GeoBackend {
  booleanContains(a: GeoJsonGeometry, b: GeoJsonGeometry): boolean;
  booleanIntersects(a: GeoJsonGeometry, b: GeoJsonGeometry): boolean;
  cellToParent(cell: string, res: number): string;
  gridDisk(origin: string, k: number): string[];
  isValidCell(cell: string): boolean;
  latLngToCell(lat: number, lon: number, res: number): string;
}

// Spatial functions whose evaluation requires the external backend. The rest of
// the st_*/h3_* surface is pure and never triggers a load.
const GEO_BACKEND_FUNCTIONS: ReadonlySet<string> = new Set([
  "st_intersects",
  "st_disjoint",
  "st_contains",
  "st_within",
  "h3_within",
  "h3_cell",
  "h3_parent",
]);

let geoBackend: GeoBackend | null = null;

/** Install the backend. Called by `./geo-backend.ts` once its libraries load. */
export function setGeoBackend(backend: GeoBackend): void {
  geoBackend = backend;
}

export function requireGeoBackend(): GeoBackend {
  if (geoBackend === null) {
    throw new LakeqlError(
      "LAKEQL_GEO_BACKEND_MISSING",
      "Spatial function requires the geo backend. Query execution loads it " +
        "automatically; when calling evaluate() directly, await loadGeoBackend() first.",
    );
  }
  return geoBackend;
}

/** Force-load the geo backend (idempotent). Exposed for direct evaluate() callers. */
export async function loadGeoBackend(): Promise<void> {
  if (geoBackend !== null) return;
  const module = await import("./geo-backend.js");
  module.installGeoBackend();
}

/**
 * Load the geo backend iff any of `exprs` uses a backend-requiring spatial
 * function. Cheap no-op once the backend is installed or when no geo is present,
 * so query execution can call it unconditionally.
 */
export async function ensureGeoBackendForExprs(exprs: Iterable<Expr | undefined>): Promise<void> {
  if (geoBackend !== null) return;
  for (const expr of exprs) {
    if (exprNeedsGeoBackend(expr)) {
      await loadGeoBackend();
      return;
    }
  }
}

function exprNeedsGeoBackend(expr: Expr | undefined): boolean {
  if (!expr) return false;
  switch (expr.kind) {
    case "literal":
    case "column":
      return false;
    case "compare":
    case "arithmetic":
      return exprNeedsGeoBackend(expr.left) || exprNeedsGeoBackend(expr.right);
    case "in":
      return exprNeedsGeoBackend(expr.target) || expr.values.some(exprNeedsGeoBackend);
    case "between":
      return (
        exprNeedsGeoBackend(expr.target) ||
        exprNeedsGeoBackend(expr.low) ||
        exprNeedsGeoBackend(expr.high)
      );
    case "null-check":
    case "like":
      return exprNeedsGeoBackend(expr.target);
    case "logical":
      return expr.operands.some(exprNeedsGeoBackend);
    case "not":
      return exprNeedsGeoBackend(expr.operand);
    case "call":
      return (
        GEO_BACKEND_FUNCTIONS.has(expr.fn.toLowerCase()) || expr.args.some(exprNeedsGeoBackend)
      );
    case "case":
      return (
        expr.whens.some(
          (branch) => exprNeedsGeoBackend(branch.when) || exprNeedsGeoBackend(branch.value),
        ) || exprNeedsGeoBackend(expr.else)
      );
  }
}

type EvalValue = Scalar;

export interface BBox {
  minx: number;
  miny: number;
  maxx: number;
  maxy: number;
}

const textEncoder = new TextEncoder();

export function evaluate(expr: Expr, row: Row): EvalValue {
  switch (expr.kind) {
    case "literal":
      return expr.value;
    case "column":
      return rowValue(row, expr.name);
    case "compare":
      return compare(expr.op, evaluate(expr.left, row), evaluate(expr.right, row));
    case "in": {
      const target = evaluate(expr.target, row);
      const result = inList(
        target,
        expr.values.map((value) => evaluate(value, row)),
      );
      return expr.negated ? sqlNot(result) : result;
    }
    case "between": {
      const target = evaluate(expr.target, row);
      const low = evaluate(expr.low, row);
      const high = evaluate(expr.high, row);
      return sqlAnd(compare("gte", target, low), compare("lte", target, high));
    }
    case "null-check": {
      const result = evaluate(expr.target, row) === null;
      return expr.negated ? !result : result;
    }
    case "logical":
      return expr.op === "and"
        ? expr.operands.reduce<SqlBoolean>(
            (acc, operand) => sqlAnd(acc, toSqlBoolean(evaluate(operand, row))),
            true,
          )
        : expr.operands.reduce<SqlBoolean>(
            (acc, operand) => sqlOr(acc, toSqlBoolean(evaluate(operand, row))),
            false,
          );
    case "not":
      return sqlNot(toSqlBoolean(evaluate(expr.operand, row)));
    case "like":
      return likeMatch(evaluate(expr.target, row), expr.pattern, expr.caseInsensitive);
    case "call":
      return callFunction(
        expr.fn,
        expr.args.map((arg) => evaluate(arg, row)),
      );
    case "arithmetic":
      return arithmetic(expr.op, evaluate(expr.left, row), evaluate(expr.right, row));
    case "case":
      for (const branch of expr.whens) {
        if (toSqlBoolean(evaluate(branch.when, row)) === true) return evaluate(branch.value, row);
      }
      return expr.else === undefined ? null : evaluate(expr.else, row);
  }
}

export function matches(expr: Expr | undefined, row: Row): boolean {
  if (!expr) return true;
  return toSqlBoolean(evaluate(expr, row)) === true;
}

export function jsonSafeValue(value: unknown): unknown {
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    return Number.isSafeInteger(asNumber) ? asNumber : value.toString();
  }
  if (Array.isArray(value)) return value.map(jsonSafeValue);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, inner] of Object.entries(value)) out[key] = jsonSafeValue(inner);
    return out;
  }
  return value;
}

export function encodeJsonLine(row: Row): Uint8Array {
  return textEncoder.encode(`${JSON.stringify(jsonSafeValue(row))}\n`);
}

function rowValue(row: Row, name: string): EvalValue {
  if (!(name in row)) {
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${name}`, { column: name });
  }
  const value = row[name];
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `Column ${name} is not a scalar value`, {
    column: name,
    valueType: typeof value,
  });
}

function toSqlBoolean(value: EvalValue): SqlBoolean {
  if (value === null) return null;
  if (typeof value === "boolean") return value;
  throw new LakeqlError("LAKEQL_TYPE_ERROR", "Predicate expression must evaluate to boolean", {
    value,
  });
}

function compare(op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte", left: EvalValue, right: EvalValue) {
  if (left === null || right === null) return null;
  if (typeof left !== typeof right && !(isNumberLike(left) && isNumberLike(right))) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare values of different types", {
      leftType: typeof left,
      rightType: typeof right,
    });
  }
  const order = left < right ? -1 : left > right ? 1 : 0;
  switch (op) {
    case "eq":
      return order === 0;
    case "ne":
      return order !== 0;
    case "lt":
      return order < 0;
    case "lte":
      return order <= 0;
    case "gt":
      return order > 0;
    case "gte":
      return order >= 0;
  }
}

function arithmetic(
  op: "add" | "sub" | "mul" | "div" | "mod",
  left: EvalValue,
  right: EvalValue,
): EvalValue {
  if (left === null || right === null) return null;
  if (typeof left !== "number" || typeof right !== "number") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Arithmetic expressions require numeric values", {
      leftType: typeof left,
      rightType: typeof right,
    });
  }
  switch (op) {
    case "add":
      return left + right;
    case "sub":
      return left - right;
    case "mul":
      return left * right;
    case "div":
      return left / right;
    case "mod":
      return left % right;
  }
}

function isNumberLike(value: EvalValue): value is number | bigint {
  return typeof value === "number" || typeof value === "bigint";
}

function inList(target: EvalValue, values: EvalValue[]): SqlBoolean {
  if (target === null) return null;
  let sawNull = false;
  for (const value of values) {
    const result = compare("eq", target, value);
    if (result === true) return true;
    if (result === null) sawNull = true;
  }
  return sawNull ? null : false;
}

function sqlAnd(left: SqlBoolean, right: SqlBoolean): SqlBoolean {
  if (left === false || right === false) return false;
  if (left === null || right === null) return null;
  return true;
}

function sqlOr(left: SqlBoolean, right: SqlBoolean): SqlBoolean {
  if (left === true || right === true) return true;
  if (left === null || right === null) return null;
  return false;
}

function sqlNot(value: SqlBoolean): SqlBoolean {
  if (value === null) return null;
  return !value;
}

function likeMatch(value: EvalValue, pattern: string, caseInsensitive: boolean): SqlBoolean {
  if (value === null) return null;
  if (typeof value !== "string") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "LIKE expects a string value", {
      valueType: typeof value,
    });
  }
  const regex = new RegExp(`^${escapeLikePattern(pattern)}$`, caseInsensitive ? "iu" : "u");
  return regex.test(value);
}

function escapeLikePattern(pattern: string): string {
  let out = "";
  for (const char of pattern) {
    if (char === "%") out += ".*";
    else if (char === "_") out += ".";
    else out += char.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  }
  return out;
}

function callFunction(name: string, args: EvalValue[]): EvalValue {
  const fn = name.toLowerCase();
  switch (fn) {
    case "lower":
      return unaryString(fn, args, (value) => value.toLowerCase());
    case "upper":
      return unaryString(fn, args, (value) => value.toUpperCase());
    case "trim":
      return unaryString(fn, args, (value) => value.trim());
    case "substr":
      return substr(args);
    case "replace":
      return replace(args);
    case "regexp_matches":
      return regexpMatches(args);
    case "regexp_replace":
      return regexpReplace(args);
    case "coalesce":
      return args.find((value) => value !== null) ?? null;
    case "nullif":
      requireArgCount(fn, args, 2);
      return compare("eq", args[0] ?? null, args[1] ?? null) === true ? null : (args[0] ?? null);
    case "cast":
      return cast(args);
    case "year":
      return datePart(fn, args, (date) => date.getUTCFullYear());
    case "month":
      return datePart(fn, args, (date) => date.getUTCMonth() + 1);
    case "day":
      return datePart(fn, args, (date) => date.getUTCDate());
    case "hour":
      return datePart(fn, args, (date) => date.getUTCHours());
    case "date_trunc":
      return dateTrunc(args);
    case "round":
      return round(args);
    case "floor":
      return unaryNumber(fn, args, Math.floor);
    case "ceil":
      return unaryNumber(fn, args, Math.ceil);
    case "abs":
      return unaryNumber(fn, args, Math.abs);
    case "least":
      return leastGreatest(fn, args, "least");
    case "greatest":
      return leastGreatest(fn, args, "greatest");
    case "st_point":
      return stPoint(args);
    case "st_x":
      return pointCoordinate(fn, args, 0);
    case "st_y":
      return pointCoordinate(fn, args, 1);
    case "st_bbox":
      return stBBox(args);
    case "st_intersects":
      return spatialPredicate(fn, args, "intersects");
    case "st_contains":
      return spatialPredicate(fn, args, "contains");
    case "st_within":
      return spatialPredicate(fn, args, "within");
    case "st_disjoint":
      return spatialPredicate(fn, args, "disjoint");
    case "st_distance":
      return spatialMeasurement(fn, args, bboxDistance);
    case "st_area":
      return geometryMeasurement(fn, args, geometryArea);
    case "st_length":
      return geometryMeasurement(fn, args, geometryLength);
    case "st_centroid":
      return geometryTransform(fn, args, geometryCentroid);
    case "st_envelope":
      return geometryTransform(fn, args, (geometry) =>
        JSON.stringify(envelopeFromGeometry(geometry, fn)),
      );
    case "h3_in":
      return h3In(args);
    case "h3_within":
      return h3Within(args);
    case "h3_cell":
      return h3Cell(args);
    case "h3_parent":
      return h3Parent(args);
    default:
      throw new LakeqlError("LAKEQL_UNSUPPORTED_PUSHDOWN", `Unsupported scalar function ${name}`, {
        function: name,
      });
  }
}

function requireArgCount(name: string, args: EvalValue[], expected: number): void {
  if (args.length !== expected) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects ${expected} arguments`, {
      expected,
      received: args.length,
    });
  }
}

function unaryString(name: string, args: EvalValue[], cb: (value: string) => string): EvalValue {
  requireArgCount(name, args, 1);
  const value = args[0] ?? null;
  if (value === null) return null;
  if (typeof value !== "string") throwType(name, "string", value);
  return cb(value);
}

function unaryNumber(name: string, args: EvalValue[], cb: (value: number) => number): EvalValue {
  requireArgCount(name, args, 1);
  const value = args[0] ?? null;
  if (value === null) return null;
  if (typeof value !== "number") throwType(name, "number", value);
  return cb(value);
}

function substr(args: EvalValue[]): EvalValue {
  requireArgCount("substr", args, 3);
  const value = args[0] ?? null;
  const start = args[1] ?? null;
  const length = args[2] ?? null;
  if (value === null || start === null || length === null) return null;
  if (typeof value !== "string") throwType("substr", "string", value);
  if (typeof start !== "number" || typeof length !== "number") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "substr() start and length must be numbers");
  }
  return value.slice(start, start + length);
}

function replace(args: EvalValue[]): EvalValue {
  requireArgCount("replace", args, 3);
  const value = args[0] ?? null;
  const search = args[1] ?? null;
  const replacement = args[2] ?? null;
  if (value === null || search === null || replacement === null) return null;
  if (typeof value !== "string") throwType("replace", "string", value);
  if (typeof search !== "string" || typeof replacement !== "string") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "replace() search and replacement must be strings");
  }
  return value.replaceAll(search, replacement);
}

function regexpMatches(args: EvalValue[]): EvalValue {
  if (args.length < 2 || args.length > 3) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "regexp_matches() expects 2 or 3 arguments", {
      received: args.length,
    });
  }
  const value = args[0] ?? null;
  const pattern = args[1] ?? null;
  const options = args[2] ?? "";
  if (value === null || pattern === null || options === null) return null;
  if (typeof value !== "string" || typeof pattern !== "string" || typeof options !== "string") {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "regexp_matches() value, pattern, and options must be strings",
    );
  }
  return regexpMatchesValue(value, pattern, options);
}

function regexpReplace(args: EvalValue[]): EvalValue {
  if (args.length < 3 || args.length > 4) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "regexp_replace() expects 3 or 4 arguments", {
      received: args.length,
    });
  }
  const value = args[0] ?? null;
  const pattern = args[1] ?? null;
  const replacement = args[2] ?? null;
  const options = args[3] ?? "";
  if (value === null || pattern === null || replacement === null || options === null) return null;
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
}

function stPoint(args: EvalValue[]): EvalValue {
  requireArgCount("st_point", args, 2);
  const [lon, lat] = args;
  if (!finiteNumber(lon) || !finiteNumber(lat)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_point() expects finite lon/lat numbers");
  }
  return JSON.stringify({ type: "Point", coordinates: [lon, lat] });
}

function pointCoordinate(name: string, args: EvalValue[], index: 0 | 1): EvalValue {
  requireArgCount(name, args, 1);
  const value = args[0] ?? null;
  if (value === null) return null;
  return pointFromGeometry(parseGeometry(value, name), name)[index];
}

function stBBox(args: EvalValue[]): EvalValue {
  requireArgCount("st_bbox", args, 4);
  const [minx, miny, maxx, maxy] = args;
  if (!finiteNumber(minx) || !finiteNumber(miny) || !finiteNumber(maxx) || !finiteNumber(maxy)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_bbox() expects finite number bounds");
  }
  if (minx > maxx || miny > maxy) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_bbox() bounds must be ordered min <= max");
  }
  return JSON.stringify({ type: "BBox", minx, miny, maxx, maxy });
}

type SpatialOp = "intersects" | "contains" | "within" | "disjoint";

// Bounding boxes are the cheap prefilter: a few float comparisons that can
// decide the obvious cases without parsing full geometry. Turf only runs on the
// survivors — the candidates whose envelopes overlap but whose true geometries
// still need an exact answer.
function spatialPredicate(name: string, args: EvalValue[], op: SpatialOp): EvalValue {
  requireArgCount(name, args, 2);
  const left = args[0] ?? null;
  const right = args[1] ?? null;
  if (left === null || right === null) return null;
  const a = toGeometry(parseGeometry(left, name), name);
  const b = toGeometry(parseGeometry(right, name), name);
  const ea = envelopeOf(a);
  const eb = envelopeOf(b);
  const geo = requireGeoBackend();
  switch (op) {
    case "intersects":
      // Disjoint envelopes cannot intersect; otherwise check the real geometry.
      return bboxIntersects(ea, eb) && geo.booleanIntersects(a, b);
    case "disjoint":
      // Disjoint envelopes are definitely disjoint; otherwise check the real geometry.
      return !bboxIntersects(ea, eb) || !geo.booleanIntersects(a, b);
    case "contains":
      // `a` can only contain `b` if `a`'s envelope contains `b`'s.
      return bboxContains(ea, eb) && geo.booleanContains(a, b);
    case "within":
      // `a` within `b` is `b` contains `a`; requires `b`'s envelope to contain `a`'s.
      return bboxContains(eb, ea) && geo.booleanContains(b, a);
  }
}

// Envelope of an already-normalized geometry, for the bbox prefilter.
export function envelopeOf(geometry: GeoJsonGeometry): BBox {
  switch (geometry.type) {
    case "Point":
      return pointsEnvelope([geometry.coordinates]);
    case "LineString":
      return pointsEnvelope(geometry.coordinates);
    case "Polygon":
      return pointsEnvelope(geometry.coordinates.flat());
  }
}

export type GeoJsonGeometry =
  | { type: "Point"; coordinates: [number, number] }
  | { type: "LineString"; coordinates: [number, number][] }
  | { type: "Polygon"; coordinates: [number, number][][] };

// Builds a clean, closed GeoJSON geometry from already-parsed input for Turf.
// BBox geometries become their rectangle polygon so envelope-only inputs still
// get an exact answer.
export function toGeometry(parsed: Record<string, unknown>, name: string): GeoJsonGeometry {
  switch (parsed.type) {
    case "Point":
      return { type: "Point", coordinates: pointFromGeometry(parsed, name) };
    case "LineString":
      return { type: "LineString", coordinates: lineStringPoints(parsed, name) };
    case "Polygon":
      return { type: "Polygon", coordinates: polygonRings(parsed, name) };
    case "BBox":
      return { type: "Polygon", coordinates: [bboxRing(bboxFromRecord(parsed, name))] };
    default:
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        `${name}() supports Point, LineString, Polygon, or BBox geometry`,
      );
  }
}

function bboxRing(box: BBox): [number, number][] {
  return [
    [box.minx, box.miny],
    [box.maxx, box.miny],
    [box.maxx, box.maxy],
    [box.minx, box.maxy],
    [box.minx, box.miny],
  ];
}

function polygonRings(record: Record<string, unknown>, name: string): [number, number][][] {
  const rings = record.coordinates;
  if (!Array.isArray(rings) || rings.length === 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() Polygon coordinates are invalid`);
  }
  return rings.map((ring) => {
    if (!Array.isArray(ring) || ring.length === 0 || !ring.every(isPosition)) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() Polygon coordinates are invalid`);
    }
    return closeRing(ring);
  });
}

// GeoJSON requires linear rings to be closed (first position === last). Turf
// expects valid input, so close any ring the caller left open.
function closeRing(ring: [number, number][]): [number, number][] {
  const first = ring[0];
  const last = ring[ring.length - 1];
  if (first && last && (first[0] !== last[0] || first[1] !== last[1])) {
    return [...ring, first];
  }
  return ring;
}

function spatialMeasurement(
  name: string,
  args: EvalValue[],
  measure: (left: BBox, right: BBox) => number,
): EvalValue {
  requireArgCount(name, args, 2);
  const left = args[0] ?? null;
  const right = args[1] ?? null;
  if (left === null || right === null) return null;
  return measure(envelope(left, name), envelope(right, name));
}

function geometryMeasurement(
  name: string,
  args: EvalValue[],
  measure: (geometry: Record<string, unknown>, name: string) => number,
): EvalValue {
  requireArgCount(name, args, 1);
  const value = args[0] ?? null;
  if (value === null) return null;
  return measure(parseGeometry(value, name), name);
}

function geometryTransform(
  name: string,
  args: EvalValue[],
  transform: (geometry: Record<string, unknown>) => string,
): EvalValue {
  requireArgCount(name, args, 1);
  const value = args[0] ?? null;
  if (value === null) return null;
  return transform(parseGeometry(value, name));
}

function h3In(args: EvalValue[]): EvalValue {
  requireArgCount("h3_in", args, 2);
  const cell = args[0] ?? null;
  const cells = args[1] ?? null;
  if (cell === null || cells === null) return null;
  if (typeof cell !== "string" || typeof cells !== "string") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_in() expects a string cell and JSON cell list");
  }
  const parsed: unknown = JSON.parse(cells);
  if (!Array.isArray(parsed) || !parsed.every((value) => typeof value === "string")) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_in() cell list must be a JSON string array");
  }
  return parsed.includes(cell);
}

function h3Within(args: EvalValue[]): EvalValue {
  requireArgCount("h3_within", args, 3);
  const cell = args[0] ?? null;
  const origin = args[1] ?? null;
  const k = args[2] ?? null;
  if (cell === null || origin === null || k === null) return null;
  if (typeof cell !== "string" || typeof origin !== "string") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_within() expects string cells");
  }
  validateH3Cell(cell, "cell");
  validateH3Cell(origin, "origin");
  if (typeof k !== "number" || !Number.isInteger(k) || k < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_within() expects a non-negative integer radius");
  }
  return requireGeoBackend().gridDisk(origin, k).includes(cell);
}

function h3Cell(args: EvalValue[]): EvalValue {
  requireArgCount("h3_cell", args, 3);
  const lat = args[0] ?? null;
  const lon = args[1] ?? null;
  const res = args[2] ?? null;
  if (!finiteNumber(lat) || !finiteNumber(lon)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_cell() expects finite lat/lon numbers");
  }
  if (typeof res !== "number" || !Number.isInteger(res) || res < 0 || res > 15) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_cell() expects an integer resolution 0..15");
  }
  return requireGeoBackend().latLngToCell(lat, lon, res);
}

function h3Parent(args: EvalValue[]): EvalValue {
  requireArgCount("h3_parent", args, 2);
  const cell = args[0] ?? null;
  const res = args[1] ?? null;
  if (cell === null || res === null) return null;
  if (typeof cell !== "string") throwType("h3_parent", "string", cell);
  validateH3Cell(cell, "cell");
  if (typeof res !== "number" || !Number.isInteger(res) || res < 0 || res > 15) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "h3_parent() expects an integer resolution 0..15");
  }
  return requireGeoBackend().cellToParent(cell, res);
}

function validateH3Cell(cell: string, label: string): void {
  if (!requireGeoBackend().isValidCell(cell)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `h3 cell ${label} is invalid`, { cell });
  }
}

function envelope(value: EvalValue, name: string): BBox {
  return envelopeFromGeometry(parseGeometry(value, name), name);
}

export function parseGeometry(value: EvalValue, name: string): Record<string, unknown> {
  if (typeof value !== "string") throwType(name, "GeoJSON or BBox JSON string", value);
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch (cause) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() received invalid geometry JSON`, {
      cause,
    });
  }
  if (!isRecord(parsed) || typeof parsed.type !== "string") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects GeoJSON or BBox JSON`);
  }
  return parsed;
}

export function envelopeFromGeometry(parsed: Record<string, unknown>, name: string): BBox {
  if (parsed.type === "BBox") return bboxFromRecord(parsed, name);
  if (parsed.type === "Point") return pointEnvelope(parsed, name);
  if (parsed.type === "LineString") return lineStringEnvelope(parsed, name);
  if (parsed.type === "Polygon") return polygonEnvelope(parsed, name);
  throw new LakeqlError(
    "LAKEQL_TYPE_ERROR",
    `${name}() supports Point, LineString, Polygon, or BBox geometry`,
  );
}

function bboxFromRecord(record: Record<string, unknown>, name: string): BBox {
  const { minx, miny, maxx, maxy } = record;
  if (!finiteNumber(minx) || !finiteNumber(miny) || !finiteNumber(maxx) || !finiteNumber(maxy)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() BBox values must be finite numbers`);
  }
  return { minx, miny, maxx, maxy };
}

function pointEnvelope(record: Record<string, unknown>, name: string): BBox {
  const [x, y] = pointFromGeometry(record, name);
  return { minx: x, miny: y, maxx: x, maxy: y };
}

function lineStringEnvelope(record: Record<string, unknown>, name: string): BBox {
  return pointsEnvelope(lineStringPoints(record, name));
}

function polygonEnvelope(record: Record<string, unknown>, name: string): BBox {
  const points = polygonPoints(record, name);
  return pointsEnvelope(points);
}

function pointsEnvelope(points: [number, number][]): BBox {
  let minx = Number.POSITIVE_INFINITY;
  let miny = Number.POSITIVE_INFINITY;
  let maxx = Number.NEGATIVE_INFINITY;
  let maxy = Number.NEGATIVE_INFINITY;
  for (const [x, y] of points) {
    minx = Math.min(minx, x);
    miny = Math.min(miny, y);
    maxx = Math.max(maxx, x);
    maxy = Math.max(maxy, y);
  }
  return { minx, miny, maxx, maxy };
}

export function bboxIntersects(left: BBox, right: BBox): boolean {
  return (
    left.maxx >= right.minx &&
    left.minx <= right.maxx &&
    left.maxy >= right.miny &&
    left.miny <= right.maxy
  );
}

function bboxContains(left: BBox, right: BBox): boolean {
  return (
    left.minx <= right.minx &&
    left.miny <= right.miny &&
    left.maxx >= right.maxx &&
    left.maxy >= right.maxy
  );
}

function bboxDistance(left: BBox, right: BBox): number {
  const dx =
    left.maxx < right.minx
      ? right.minx - left.maxx
      : right.maxx < left.minx
        ? left.minx - right.maxx
        : 0;
  const dy =
    left.maxy < right.miny
      ? right.miny - left.maxy
      : right.maxy < left.miny
        ? left.miny - right.maxy
        : 0;
  return Math.hypot(dx, dy);
}

function geometryArea(geometry: Record<string, unknown>, name: string): number {
  if (geometry.type === "BBox") {
    const box = bboxFromRecord(geometry, name);
    return (box.maxx - box.minx) * (box.maxy - box.miny);
  }
  if (geometry.type === "Polygon") return Math.abs(ringArea(polygonPoints(geometry, name)));
  if (geometry.type === "Point" || geometry.type === "LineString") return 0;
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() unsupported geometry type`, {
    type: geometry.type,
  });
}

function geometryLength(geometry: Record<string, unknown>, name: string): number {
  if (geometry.type === "BBox") {
    const box = bboxFromRecord(geometry, name);
    return 2 * (box.maxx - box.minx + (box.maxy - box.miny));
  }
  if (geometry.type === "LineString") return pathLength(lineStringPoints(geometry, name));
  if (geometry.type === "Polygon") return pathLength(polygonPoints(geometry, name));
  if (geometry.type === "Point") return 0;
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() unsupported geometry type`, {
    type: geometry.type,
  });
}

function geometryCentroid(geometry: Record<string, unknown>): string {
  const box = envelopeFromGeometry(geometry, "st_centroid");
  return JSON.stringify({
    type: "Point",
    coordinates: [(box.minx + box.maxx) / 2, (box.miny + box.maxy) / 2],
  });
}

function pointFromGeometry(record: Record<string, unknown>, name: string): [number, number] {
  if (record.type !== "Point" || !isPosition(record.coordinates)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() Point coordinates are invalid`);
  }
  return record.coordinates;
}

function lineStringPoints(record: Record<string, unknown>, name: string): [number, number][] {
  const points = record.coordinates;
  if (!Array.isArray(points) || points.length === 0 || !points.every(isPosition)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() LineString coordinates are invalid`);
  }
  return points;
}

function polygonPoints(record: Record<string, unknown>, name: string): [number, number][] {
  const rings = record.coordinates;
  if (!Array.isArray(rings)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() Polygon coordinates are invalid`);
  }
  const points = rings.flat();
  if (points.length === 0 || !points.every(isPosition)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() Polygon coordinates are invalid`);
  }
  return points;
}

function ringArea(points: [number, number][]): number {
  let area = 0;
  for (let index = 0; index < points.length; index += 1) {
    const current = points[index];
    const next = points[(index + 1) % points.length];
    if (current === undefined || next === undefined) continue;
    area += current[0] * next[1] - next[0] * current[1];
  }
  return area / 2;
}

function pathLength(points: [number, number][]): number {
  let length = 0;
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const current = points[index];
    if (previous === undefined || current === undefined) continue;
    length += Math.hypot(current[0] - previous[0], current[1] - previous[1]);
  }
  return length;
}

function isPosition(value: unknown): value is [number, number] {
  return (
    Array.isArray(value) &&
    value.length === 2 &&
    typeof value[0] === "number" &&
    typeof value[1] === "number" &&
    Number.isFinite(value[0]) &&
    Number.isFinite(value[1])
  );
}

function finiteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function cast(args: EvalValue[]): EvalValue {
  requireArgCount("cast", args, 2);
  const value = args[0] ?? null;
  const target = args[1] ?? null;
  if (value === null) return null;
  if (typeof target !== "string") throwType("cast", "string type name", target ?? null);
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
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `Unsupported cast target ${target}`, { target });
  }
}

function datePart(name: string, args: EvalValue[], cb: (date: Date) => number): EvalValue {
  requireArgCount(name, args, 1);
  const date = parseDateArg(name, args[0] ?? null);
  return date ? cb(date) : null;
}

function dateTrunc(args: EvalValue[]): EvalValue {
  requireArgCount("date_trunc", args, 2);
  const part = args[0] ?? null;
  const value = args[1] ?? null;
  if (part === null || value === null) return null;
  if (typeof part !== "string") throwType("date_trunc", "string part", part);
  const date = parseDateArg("date_trunc", value);
  if (!date) return null;
  if (part === "year") return `${date.getUTCFullYear()}-01-01T00:00:00.000Z`;
  if (part === "month") {
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-01T00:00:00.000Z`;
  }
  if (part === "day") {
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T00:00:00.000Z`;
  }
  if (part === "hour") {
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T${pad(date.getUTCHours())}:00:00.000Z`;
  }
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `Unsupported date_trunc part ${part}`, { part });
}

function round(args: EvalValue[]): EvalValue {
  if (args.length < 1 || args.length > 2) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "round() expects 1 or 2 arguments", {
      received: args.length,
    });
  }
  const value = args[0] ?? null;
  const places = args[1] ?? 0;
  if (value === null || places === null) return null;
  if (typeof value !== "number" || typeof places !== "number") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "round() arguments must be numbers");
  }
  const scale = 10 ** places;
  return Math.round(value * scale) / scale;
}

function leastGreatest(name: string, args: EvalValue[], mode: "least" | "greatest"): EvalValue {
  if (args.length === 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects at least 1 argument`);
  }
  if (args.some((value) => value === null)) return null;
  let best = args[0] ?? null;
  for (const value of args.slice(1)) {
    if (compare(mode === "least" ? "lt" : "gt", value, best) === true) best = value;
  }
  return best;
}

function parseDateArg(name: string, value: EvalValue): Date | null {
  if (value === null) return null;
  if (typeof value !== "string" && typeof value !== "number") throwType(name, "date string", value);
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() received an invalid date`, { value });
  }
  return date;
}

function throwType(name: string, expected: string, value: EvalValue): never {
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() expects ${expected}`, {
    expected,
    received: typeof value,
  });
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}
