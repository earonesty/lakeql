import { LakeqlError } from "./errors.js";
import type { Expr, Scalar } from "./expr.js";
import { regexpMatchesValue, regexpReplaceValue } from "./regex-functions.js";
import { compareTimestampValues, isTimestampValue, timestampValueFromIso } from "./timestamp.js";
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
  "st_dwithin",
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
  if (isTimestampValue(value)) return value.toJSON();
  if (value instanceof Uint8Array) return bytesToHex(value);
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
    typeof value === "bigint" ||
    value instanceof Uint8Array ||
    isTimestampValue(value)
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
  if (left instanceof Uint8Array || right instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare binary values", {
      leftType: evalValueType(left),
      rightType: evalValueType(right),
    });
  }
  if (isTimestampValue(left) || isTimestampValue(right)) {
    const leftTimestamp = timestampEvalValue(left);
    const rightTimestamp = timestampEvalValue(right);
    if (leftTimestamp === undefined || rightTimestamp === undefined) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare timestamp with non-timestamp", {
        leftType: evalValueType(left),
        rightType: evalValueType(right),
      });
    }
    return compareOrder(op, compareTimestampValues(leftTimestamp, rightTimestamp));
  }
  if (typeof left !== typeof right && !(isNumberLike(left) && isNumberLike(right))) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare values of different types", {
      leftType: typeof left,
      rightType: typeof right,
    });
  }
  const order = left < right ? -1 : left > right ? 1 : 0;
  return compareOrder(op, order);
}

function compareOrder(op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte", order: number): boolean {
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

function timestampEvalValue(value: EvalValue) {
  if (isTimestampValue(value)) return value;
  if (typeof value === "string") return timestampValueFromIso(value);
  return undefined;
}

function evalValueType(value: EvalValue): string {
  if (value instanceof Uint8Array) return "binary";
  return isTimestampValue(value) ? "timestamp" : typeof value;
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
    case "st_dwithin":
      return stDWithin(fn, args);
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

function stDWithin(name: string, args: EvalValue[]): EvalValue {
  requireArgCount(name, args, 3);
  const left = args[0] ?? null;
  const right = args[1] ?? null;
  const distance = args[2] ?? null;
  if (left === null || right === null || distance === null) return null;
  if (typeof distance !== "number" || !Number.isFinite(distance) || distance < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "st_dwithin() expects a non-negative distance");
  }
  const a = toGeometry(parseGeometry(left, name), name);
  const b = toGeometry(parseGeometry(right, name), name);
  const ea = envelopeOf(a);
  const eb = envelopeOf(b);
  if (bboxDistance(ea, eb) > distance) return false;
  return geometryDistance(a, b) <= distance;
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
  if (value instanceof Uint8Array) return parseWkbGeometry(value, name);
  if (typeof value !== "string")
    throwType(name, "GeoJSON, WKT point, BBox JSON, or WKB bytes", value);
  const wkt = parseWktGeometry(value);
  if (wkt !== undefined) return wkt;
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

function parseWkbGeometry(value: Uint8Array, name: string): Record<string, unknown> {
  const reader = new WkbReader(value, name);
  const geometry = reader.geometry();
  if (!reader.done()) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name}() received trailing WKB bytes`);
  }
  return geometry;
}

class WkbReader {
  private offset = 0;
  private littleEndian = true;

  constructor(
    private readonly bytes: Uint8Array,
    private readonly name: string,
  ) {}

  done(): boolean {
    return this.offset === this.bytes.byteLength;
  }

  geometry(): Record<string, unknown> {
    this.byteOrder();
    const header = this.typeHeader();
    if (header.hasSrid) this.uint32();
    switch (header.type) {
      case 1:
        return { type: "Point", coordinates: this.position(header.dimensions) };
      case 2:
        return { type: "LineString", coordinates: this.positions(header.dimensions) };
      case 3:
        return {
          type: "Polygon",
          coordinates: this.rings(header.dimensions),
        };
      default:
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          `${this.name}() supports WKB Point, LineString, or Polygon geometry`,
          { wkbType: header.rawType },
        );
    }
  }

  private byteOrder(): void {
    const order = this.uint8();
    if (order === 0) {
      this.littleEndian = false;
      return;
    }
    if (order === 1) {
      this.littleEndian = true;
      return;
    }
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${this.name}() WKB byte order is invalid`);
  }

  private typeHeader(): { rawType: number; type: number; dimensions: number; hasSrid: boolean } {
    const rawType = this.uint32();
    const hasZFlag = (rawType & 0x80000000) !== 0;
    const hasMFlag = (rawType & 0x40000000) !== 0;
    const hasSrid = (rawType & 0x20000000) !== 0;
    const baseWithEwkbFlags = rawType & 0x0fffffff;
    const isoFamily = Math.trunc(baseWithEwkbFlags / 1000);
    const type = baseWithEwkbFlags % 1000;
    const hasZ = hasZFlag || isoFamily === 1 || isoFamily === 3;
    const hasM = hasMFlag || isoFamily === 2 || isoFamily === 3;
    return {
      rawType,
      type,
      dimensions: 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0),
      hasSrid,
    };
  }

  private rings(dimensions: number): [number, number][][] {
    const count = this.uint32();
    const rings: [number, number][][] = [];
    for (let index = 0; index < count; index += 1) {
      rings.push(this.positions(dimensions));
    }
    return rings;
  }

  private positions(dimensions: number): [number, number][] {
    const count = this.uint32();
    const points: [number, number][] = [];
    for (let index = 0; index < count; index += 1) {
      points.push(this.position(dimensions));
    }
    return points;
  }

  private position(dimensions: number): [number, number] {
    if (dimensions < 2) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        `${this.name}() WKB coordinate dimension is invalid`,
      );
    }
    const x = this.float64();
    const y = this.float64();
    for (let index = 2; index < dimensions; index += 1) this.float64();
    if (!Number.isFinite(x) || !Number.isFinite(y)) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `${this.name}() WKB coordinates must be finite`);
    }
    return [x, y];
  }

  private uint8(): number {
    this.require(1);
    const value = this.bytes[this.offset];
    this.offset += 1;
    return value ?? 0;
  }

  private uint32(): number {
    this.require(4);
    const view = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.offset, 4);
    const value = view.getUint32(0, this.littleEndian);
    this.offset += 4;
    return value;
  }

  private float64(): number {
    this.require(8);
    const view = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.offset, 8);
    const value = view.getFloat64(0, this.littleEndian);
    this.offset += 8;
    return value;
  }

  private require(bytes: number): void {
    if (this.offset + bytes > this.bytes.byteLength) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `${this.name}() received truncated WKB`);
    }
  }
}

function parseWktGeometry(value: string): Record<string, unknown> | undefined {
  const point =
    /^POINT\s*\(\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?)\s+([+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?)\s*\)$/iu.exec(
      value,
    );
  if (point !== null) {
    const lon = Number(point[1]);
    const lat = Number(point[2]);
    if (Number.isFinite(lon) && Number.isFinite(lat)) {
      return { type: "Point", coordinates: [lon, lat] };
    }
  }
  return undefined;
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

export function bboxDistance(left: BBox, right: BBox): number {
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

export function geometryDistance(left: GeoJsonGeometry, right: GeoJsonGeometry): number {
  if (requireGeoBackend().booleanIntersects(left, right)) return 0;
  const leftSegments = geometrySegments(left);
  const rightSegments = geometrySegments(right);
  const leftPoints = geometryPoints(left);
  const rightPoints = geometryPoints(right);
  let best = Number.POSITIVE_INFINITY;

  for (const point of leftPoints) {
    for (const other of rightPoints) best = Math.min(best, pointDistance(point, other));
    for (const segment of rightSegments)
      best = Math.min(best, pointSegmentDistance(point, segment));
  }
  for (const point of rightPoints) {
    for (const segment of leftSegments) best = Math.min(best, pointSegmentDistance(point, segment));
  }
  for (const leftSegment of leftSegments) {
    for (const rightSegment of rightSegments) {
      best = Math.min(best, segmentDistance(leftSegment, rightSegment));
    }
  }

  return best;
}

type Segment = [[number, number], [number, number]];

function geometryPoints(geometry: GeoJsonGeometry): [number, number][] {
  switch (geometry.type) {
    case "Point":
      return [geometry.coordinates];
    case "LineString":
      return geometry.coordinates;
    case "Polygon":
      return geometry.coordinates.flat();
  }
}

function geometrySegments(geometry: GeoJsonGeometry): Segment[] {
  switch (geometry.type) {
    case "Point":
      return [];
    case "LineString":
      return pathSegments(geometry.coordinates, false);
    case "Polygon":
      return geometry.coordinates.flatMap((ring) => pathSegments(ring, true));
  }
}

function pathSegments(points: [number, number][], closed: boolean): Segment[] {
  const out: Segment[] = [];
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const current = points[index];
    if (previous !== undefined && current !== undefined) out.push([previous, current]);
  }
  if (closed && points.length > 1) {
    const first = points[0];
    const last = points[points.length - 1];
    if (
      first !== undefined &&
      last !== undefined &&
      (first[0] !== last[0] || first[1] !== last[1])
    ) {
      out.push([last, first]);
    }
  }
  return out;
}

function segmentDistance(left: Segment, right: Segment): number {
  if (segmentsIntersect(left, right)) return 0;
  return Math.min(
    pointSegmentDistance(left[0], right),
    pointSegmentDistance(left[1], right),
    pointSegmentDistance(right[0], left),
    pointSegmentDistance(right[1], left),
  );
}

function pointDistance(left: [number, number], right: [number, number]): number {
  return Math.hypot(left[0] - right[0], left[1] - right[1]);
}

function pointSegmentDistance(point: [number, number], segment: Segment): number {
  const [start, end] = segment;
  const dx = end[0] - start[0];
  const dy = end[1] - start[1];
  const lengthSq = dx * dx + dy * dy;
  if (lengthSq === 0) return pointDistance(point, start);
  const t = Math.max(
    0,
    Math.min(1, ((point[0] - start[0]) * dx + (point[1] - start[1]) * dy) / lengthSq),
  );
  return pointDistance(point, [start[0] + t * dx, start[1] + t * dy]);
}

function segmentsIntersect(left: Segment, right: Segment): boolean {
  const [a, b] = left;
  const [c, d] = right;
  const abC = orientation(a, b, c);
  const abD = orientation(a, b, d);
  const cdA = orientation(c, d, a);
  const cdB = orientation(c, d, b);
  if (abC === 0 && pointOnSegment(c, left)) return true;
  if (abD === 0 && pointOnSegment(d, left)) return true;
  if (cdA === 0 && pointOnSegment(a, right)) return true;
  if (cdB === 0 && pointOnSegment(b, right)) return true;
  return abC * abD < 0 && cdA * cdB < 0;
}

function orientation(a: [number, number], b: [number, number], c: [number, number]): number {
  const cross = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]);
  if (Math.abs(cross) < Number.EPSILON) return 0;
  return cross > 0 ? 1 : -1;
}

function pointOnSegment(point: [number, number], segment: Segment): boolean {
  const [start, end] = segment;
  return (
    point[0] >= Math.min(start[0], end[0]) &&
    point[0] <= Math.max(start[0], end[0]) &&
    point[1] >= Math.min(start[1], end[1]) &&
    point[1] <= Math.max(start[1], end[1])
  );
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
  if (isTimestampValue(value)) return new Date(Number(value.epochNanoseconds / 1_000_000n));
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

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
}
