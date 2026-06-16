import { booleanContains } from "@turf/boolean-contains";
import { booleanIntersects } from "@turf/boolean-intersects";
import { col, type Expr, fn, lit } from "@laql/core";

export type Position = [number, number];

export interface PointGeometry {
  type: "Point";
  coordinates: Position;
}

export interface PolygonGeometry {
  type: "Polygon";
  coordinates: Position[][];
}

export type Geometry = PointGeometry | PolygonGeometry;

export interface BBox {
  type: "BBox";
  minx: number;
  miny: number;
  maxx: number;
  maxy: number;
}

export type GeometryLike = Geometry | BBox;

export function stPoint(lon: number, lat: number): PointGeometry {
  assertFiniteNumber(lon, "lon");
  assertFiniteNumber(lat, "lat");
  return { type: "Point", coordinates: [lon, lat] };
}

export function stBBox(minx: number, miny: number, maxx: number, maxy: number): BBox {
  assertFiniteNumber(minx, "minx");
  assertFiniteNumber(miny, "miny");
  assertFiniteNumber(maxx, "maxx");
  assertFiniteNumber(maxy, "maxy");
  if (minx > maxx || miny > maxy) {
    throw new TypeError("stBBox bounds must be ordered min <= max");
  }
  return { type: "BBox", minx, miny, maxx, maxy };
}

export function stEnvelope(geometry: GeometryLike): BBox {
  if (geometry.type === "BBox") return geometry;
  if (geometry.type === "Point") {
    const [x, y] = geometry.coordinates;
    return stBBox(x, y, x, y);
  }
  const points = geometry.coordinates.flat();
  if (points.length === 0) throw new TypeError("Polygon must contain at least one point");
  let minx = Number.POSITIVE_INFINITY;
  let miny = Number.POSITIVE_INFINITY;
  let maxx = Number.NEGATIVE_INFINITY;
  let maxy = Number.NEGATIVE_INFINITY;
  for (const [x, y] of points) {
    assertFiniteNumber(x, "x");
    assertFiniteNumber(y, "y");
    minx = Math.min(minx, x);
    miny = Math.min(miny, y);
    maxx = Math.max(maxx, x);
    maxy = Math.max(maxy, y);
  }
  return stBBox(minx, miny, maxx, maxy);
}

// Bounding boxes are the cheap prefilter; Turf decides the exact geometry only
// for candidates whose envelopes already overlap.
export function stIntersects(left: GeometryLike, right: GeometryLike): boolean {
  return (
    bboxIntersects(stEnvelope(left), stEnvelope(right)) &&
    booleanIntersects(toTurfGeometry(left), toTurfGeometry(right))
  );
}

export function stDisjoint(left: GeometryLike, right: GeometryLike): boolean {
  return !stIntersects(left, right);
}

export function stContains(left: GeometryLike, right: GeometryLike): boolean {
  return (
    bboxContains(stEnvelope(left), stEnvelope(right)) &&
    booleanContains(toTurfGeometry(left), toTurfGeometry(right))
  );
}

export function stWithin(left: GeometryLike, right: GeometryLike): boolean {
  return stContains(right, left);
}

export function stX(point: PointGeometry): number {
  return point.coordinates[0];
}

export function stY(point: PointGeometry): number {
  return point.coordinates[1];
}

export function stAsGeojson(geometry: Geometry): string {
  return JSON.stringify(geometry);
}

export function stFromGeojson(value: string | Geometry): Geometry {
  const parsed: unknown = typeof value === "string" ? JSON.parse(value) : value;
  if (!isGeometry(parsed)) throw new TypeError("GeoJSON value must be a Point or Polygon");
  return parsed;
}

export function stIntersectsExpr(left: string | Expr, right: GeometryLike | Expr): Expr {
  return fn("st_intersects", columnOrExpr(left), geometryOrExpr(right));
}

export function h3Cell(lat: number | Expr, lon: number | Expr, res: number | Expr): Expr {
  return fn("h3_cell", exprOrLiteral(lat), exprOrLiteral(lon), exprOrLiteral(res));
}

export function h3Within(column: string | Expr, origin: string | Expr, k: number | Expr): Expr {
  return fn("h3_within", columnOrExpr(column), exprOrLiteral(origin), exprOrLiteral(k));
}

export function h3Parent(cell: string | Expr, res: number | Expr): Expr {
  return fn("h3_parent", exprOrLiteral(cell), exprOrLiteral(res));
}

export function h3In(column: string | Expr, cells: string[]): Expr {
  return fn("h3_in", columnOrExpr(column), lit(JSON.stringify(cells)));
}

function bboxIntersects(left: BBox, right: BBox): boolean {
  return (
    left.maxx >= right.minx &&
    left.minx <= right.maxx &&
    left.maxy >= right.miny &&
    left.miny <= right.maxy
  );
}

function bboxContains(outer: BBox, inner: BBox): boolean {
  return (
    outer.minx <= inner.minx &&
    outer.miny <= inner.miny &&
    outer.maxx >= inner.maxx &&
    outer.maxy >= inner.maxy
  );
}

// Converts a geometry to a closed GeoJSON shape Turf accepts. BBox rectangles
// become their polygon so envelope-only inputs still get an exact comparison.
function toTurfGeometry(geometry: GeometryLike): PointGeometry | PolygonGeometry {
  if (geometry.type === "BBox") {
    return {
      type: "Polygon",
      coordinates: [
        [
          [geometry.minx, geometry.miny],
          [geometry.maxx, geometry.miny],
          [geometry.maxx, geometry.maxy],
          [geometry.minx, geometry.maxy],
          [geometry.minx, geometry.miny],
        ],
      ],
    };
  }
  if (geometry.type === "Polygon") {
    return { type: "Polygon", coordinates: geometry.coordinates.map(closeRing) };
  }
  return geometry;
}

// GeoJSON linear rings must be closed (first position === last).
function closeRing(ring: Position[]): Position[] {
  const first = ring[0];
  const last = ring[ring.length - 1];
  if (first && last && (first[0] !== last[0] || first[1] !== last[1])) {
    return [...ring, first];
  }
  return ring;
}

function columnOrExpr(value: string | Expr): Expr {
  return typeof value === "string" ? col(value) : value;
}

function geometryOrExpr(value: GeometryLike | Expr): Expr {
  if (isExpr(value)) return value;
  return lit(JSON.stringify(value));
}

function exprOrLiteral(value: string | number | Expr): Expr {
  return isExpr(value) ? value : lit(value);
}

function isExpr(value: unknown): value is Expr {
  return typeof value === "object" && value !== null && "kind" in value;
}

function isGeometry(value: unknown): value is Geometry {
  if (typeof value !== "object" || value === null) return false;
  if (!("type" in value) || !("coordinates" in value)) return false;
  const geometry = value as { type: unknown; coordinates: unknown };
  if (geometry.type === "Point") return isPosition(geometry.coordinates);
  if (geometry.type === "Polygon") {
    return (
      Array.isArray(geometry.coordinates) &&
      geometry.coordinates.every((ring) => Array.isArray(ring) && ring.every(isPosition))
    );
  }
  return false;
}

function isPosition(value: unknown): value is Position {
  return (
    Array.isArray(value) &&
    value.length === 2 &&
    typeof value[0] === "number" &&
    typeof value[1] === "number" &&
    Number.isFinite(value[0]) &&
    Number.isFinite(value[1])
  );
}

function assertFiniteNumber(value: number, label: string): void {
  if (!Number.isFinite(value)) throw new TypeError(`${label} must be a finite number`);
}
