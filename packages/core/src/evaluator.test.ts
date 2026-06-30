import { beforeAll, describe, expect, it } from "vitest";
import { LakeqlError } from "./errors.js";
import { encodeJsonLine, evaluate, jsonSafeValue, loadGeoBackend, matches } from "./evaluator.js";
import {
  and,
  between,
  col,
  eq,
  fn,
  gt,
  gte,
  ilike,
  isIn,
  isNotNull,
  isNull,
  like,
  lit,
  lt,
  lte,
  ne,
  not,
  notIn,
  or,
} from "./expr.js";
import { timestampFromEpoch } from "./timestamp.js";

const row = {
  name: " Alice ",
  city: "Los Angeles",
  amount: 12.345,
  date: "2026-06-13T15:30:00Z",
  maybe: null,
  big: 9007199254740993n,
  geom: JSON.stringify({ type: "Point", coordinates: [-118.24, 34.05] }),
  polygon: JSON.stringify({
    type: "Polygon",
    coordinates: [
      [
        [-119, 33],
        [-117, 33],
        [-117, 35],
        [-119, 35],
        [-119, 33],
      ],
    ],
  }),
  h3_8: "8829a1d757fffff",
};

describe("evaluate", () => {
  // Spatial predicates that need turf/h3 require the lazily-loaded geo backend.
  // Query execution loads it automatically; direct evaluate() callers (these
  // tests) must load it explicitly.
  beforeAll(async () => {
    await loadGeoBackend();
  });

  it("implements SQL three-valued null semantics for predicates", () => {
    expect(evaluate(eq("maybe", null), row)).toBeNull();
    expect(evaluate(isNull("maybe"), row)).toBe(true);
    expect(evaluate(isNotNull("maybe"), row)).toBe(false);
    expect(evaluate(and(eq("maybe", 1), gt("amount", 1)), row)).toBeNull();
    expect(evaluate(or(eq("maybe", 1), gt("amount", 1)), row)).toBe(true);
    expect(evaluate(not(eq("maybe", 1)), row)).toBeNull();
  });

  it("supports comparison, in, between, like, and ilike", () => {
    expect(matches(and(gt("amount", 10), between("amount", 10, 13)), row)).toBe(true);
    expect(matches(and(ne("amount", 10), lt("amount", 13), lte("amount", 12.345)), row)).toBe(true);
    expect(matches(gte("amount", 12.345), row)).toBe(true);
    expect(matches(isIn("city", ["Seattle", "Los Angeles"]), row)).toBe(true);
    expect(matches(notIn("city", ["Seattle"]), row)).toBe(true);
    expect(matches(notIn("city", ["Seattle", null]), row)).toBe(false);
    expect(matches(isIn("maybe", [null]), row)).toBe(false);
    expect(matches(like("city", "Los%"), row)).toBe(true);
    expect(matches(ilike("city", "%angeles"), row)).toBe(true);
  });

  it("evaluates arithmetic and searched CASE expressions", () => {
    expect(
      evaluate(
        {
          kind: "arithmetic",
          op: "add",
          left: { kind: "literal", value: 5 },
          right: { kind: "literal", value: 2 },
        },
        row,
      ),
    ).toBe(7);
    expect(
      evaluate(
        {
          kind: "arithmetic",
          op: "sub",
          left: { kind: "literal", value: 5 },
          right: { kind: "literal", value: 2 },
        },
        row,
      ),
    ).toBe(3);
    expect(
      evaluate(
        {
          kind: "arithmetic",
          op: "mul",
          left: { kind: "column", name: "amount" },
          right: { kind: "literal", value: 2 },
        },
        row,
      ),
    ).toBe(24.69);
    expect(
      evaluate(
        {
          kind: "arithmetic",
          op: "div",
          left: { kind: "literal", value: 5 },
          right: { kind: "literal", value: 2 },
        },
        row,
      ),
    ).toBe(2.5);
    expect(
      evaluate(
        {
          kind: "arithmetic",
          op: "mod",
          left: { kind: "literal", value: 5 },
          right: { kind: "literal", value: 2 },
        },
        row,
      ),
    ).toBe(1);
    expect(
      evaluate(
        {
          kind: "case",
          whens: [
            {
              when: gt("amount", 10),
              value: { kind: "literal", value: "large" },
            },
          ],
          else: { kind: "literal", value: "small" },
        },
        row,
      ),
    ).toBe("large");
    expect(
      evaluate(
        {
          kind: "case",
          whens: [{ when: lt("amount", 10), value: { kind: "literal", value: "small" } }],
        },
        row,
      ),
    ).toBeNull();
  });

  it("evaluates timestamp, CASE, predicate, and JSON output semantics together", () => {
    const loadedAt = timestampFromEpoch(1_700_000_000_000_123n, "micros");
    const event = {
      loaded_at: loadedAt,
      updated_at: "2023-11-14T22:13:21.000124Z",
      amount: 250,
      status: "paid",
      discount: 0,
      tags: ["web", 1n, loadedAt],
    };
    const bucket = {
      kind: "case",
      whens: [
        { when: gte("amount", 1000), value: { kind: "literal", value: "large" } },
        { when: gte("amount", 100), value: { kind: "literal", value: "medium" } },
      ],
      else: { kind: "literal", value: "small" },
    } as const;

    expect(evaluate(gt("updated_at", col("loaded_at")), event)).toBe(true);
    expect(evaluate(fn("year", col("loaded_at")), event)).toBe(2023);
    expect(evaluate(fn("date_trunc", "hour", col("loaded_at")), event)).toBe(
      "2023-11-14T22:00:00.000Z",
    );
    expect(evaluate(bucket, event)).toBe("medium");
    expect(
      matches(
        and(
          between("amount", 0, 500),
          not(isIn("status", ["cancelled", "void"])),
          eq(fn("nullif", col("discount"), 0), null),
        ),
        event,
      ),
    ).toBe(false);
    expect(
      jsonSafeValue({
        loaded_at: loadedAt,
        tags: event.tags,
      }),
    ).toEqual({
      loaded_at: "2023-11-14T22:13:20.000123Z",
      tags: ["web", 1, "2023-11-14T22:13:20.000123Z"],
    });
    expect(new TextDecoder().decode(encodeJsonLine({ loaded_at: loadedAt }))).toBe(
      '{"loaded_at":"2023-11-14T22:13:20.000123Z"}\n',
    );
  });

  it("supports the phase 1 scalar function families", () => {
    expect(evaluate(fn("lower", col("city")), row)).toBe("los angeles");
    expect(evaluate(fn("upper", col("city")), row)).toBe("LOS ANGELES");
    expect(evaluate(fn("trim", col("name")), row)).toBe("Alice");
    expect(evaluate(fn("substr", col("city"), 4, 7), row)).toBe("Angeles");
    expect(evaluate(fn("replace", col("city"), "Los", "San"), row)).toBe("San Angeles");
    expect(evaluate(fn("regexp_matches", col("city"), "Ang"), row)).toBe(true);
    expect(evaluate(fn("regexp_matches", col("city"), "^ang", "i"), row)).toBe(false);
    expect(evaluate(fn("regexp_matches", col("city"), "los", "i"), row)).toBe(true);
    expect(evaluate(fn("regexp_replace", "abc", "(b|c)", "X"), row)).toBe("aXc");
    expect(evaluate(fn("regexp_replace", "abc", "(b|c)", "X", "g"), row)).toBe("aXX");
    expect(evaluate(fn("regexp_replace", "abc", "(a)(b)", "\\2\\1"), row)).toBe("bac");
    expect(evaluate(fn("regexp_replace", "a.c", ".", "X", "l"), row)).toBe("aXc");
    expect(evaluate(fn("coalesce", lit(null), col("city")), row)).toBe("Los Angeles");
    expect(evaluate(fn("nullif", col("city"), "Los Angeles"), row)).toBeNull();
    expect(evaluate(fn("year", col("date")), row)).toBe(2026);
    expect(evaluate(fn("month", col("date")), row)).toBe(6);
    expect(evaluate(fn("day", col("date")), row)).toBe(13);
    expect(evaluate(fn("hour", col("date")), row)).toBe(15);
    expect(evaluate(fn("date_trunc", "day", col("date")), row)).toBe("2026-06-13T00:00:00.000Z");
    expect(evaluate(fn("round", col("amount"), 1), row)).toBe(12.3);
    expect(evaluate(fn("floor", col("amount")), row)).toBe(12);
    expect(evaluate(fn("ceil", col("amount")), row)).toBe(13);
    expect(evaluate(fn("abs", -3), row)).toBe(3);
    expect(evaluate(fn("least", 3, 2, 5), row)).toBe(2);
    expect(evaluate(fn("greatest", 3, 2, 5), row)).toBe(5);
    expect(evaluate(fn("date_trunc", "year", col("date")), row)).toBe("2026-01-01T00:00:00.000Z");
    expect(evaluate(fn("date_trunc", "month", col("date")), row)).toBe("2026-06-01T00:00:00.000Z");
    expect(evaluate(fn("date_trunc", "hour", col("date")), row)).toBe("2026-06-13T15:00:00.000Z");
    expect(evaluate(fn("cast", col("amount"), "string"), row)).toBe("12.345");
    expect(evaluate(fn("cast", "42", "number"), row)).toBe(42);
    expect(evaluate(fn("cast", 1, "boolean"), row)).toBe(true);
  });

  it("supports spatial and h3 scalar predicates", () => {
    const bbox = fn("st_bbox", -118.5, 34, -118, 34.3);
    const line = JSON.stringify({
      type: "LineString",
      coordinates: [
        [0, 0],
        [3, 4],
      ],
    });
    expect(evaluate(bbox, row)).toBe(
      '{"type":"BBox","minx":-118.5,"miny":34,"maxx":-118,"maxy":34.3}',
    );
    expect(evaluate(fn("st_point", -118.24, 34.05), row)).toBe(row.geom);
    expect(evaluate(fn("st_x", col("geom")), row)).toBe(-118.24);
    expect(evaluate(fn("st_y", col("geom")), row)).toBe(34.05);
    expect(evaluate(fn("st_intersects", col("geom"), bbox), row)).toBe(true);
    expect(evaluate(fn("st_disjoint", col("geom"), fn("st_bbox", 0, 0, 1, 1)), row)).toBe(true);
    expect(evaluate(fn("st_contains", col("polygon"), col("geom")), row)).toBe(true);
    expect(evaluate(fn("st_within", col("geom"), col("polygon")), row)).toBe(true);
    expect(evaluate(fn("st_dwithin", col("geom"), fn("st_point", -118.25, 34.05), 0.02), row)).toBe(
      true,
    );
    expect(evaluate(fn("st_dwithin", col("geom"), fn("st_point", -118.5, 34.05), 0.02), row)).toBe(
      false,
    );
    expect(evaluate(fn("st_intersects", col("geom"), fn("st_bbox", 0, 0, 1, 1)), row)).toBe(false);
    expect(
      evaluate(fn("st_distance", fn("st_bbox", 0, 0, 1, 1), fn("st_bbox", 4, 5, 6, 7)), row),
    ).toBe(5);
    expect(
      evaluate(fn("st_distance", fn("st_bbox", 4, 5, 6, 7), fn("st_bbox", 0, 0, 1, 1)), row),
    ).toBe(5);
    expect(evaluate(fn("st_area", col("polygon")), row)).toBe(4);
    expect(evaluate(fn("st_area", bbox), row)).toBeCloseTo(0.15);
    expect(evaluate(fn("st_area", col("geom")), row)).toBe(0);
    expect(evaluate(fn("st_length", line), row)).toBe(5);
    expect(evaluate(fn("st_length", bbox), row)).toBeCloseTo(1.6);
    expect(evaluate(fn("st_length", col("polygon")), row)).toBe(8);
    expect(evaluate(fn("st_length", col("geom")), row)).toBe(0);
    expect(evaluate(fn("st_centroid", col("polygon")), row)).toBe(
      '{"type":"Point","coordinates":[-118,34]}',
    );
    expect(evaluate(fn("st_envelope", line), row)).toBe('{"minx":0,"miny":0,"maxx":3,"maxy":4}');
    expect(evaluate(fn("h3_in", col("h3_8"), JSON.stringify(["8829a1d757fffff"])), row)).toBe(true);
    expect(evaluate(fn("h3_in", col("h3_8"), JSON.stringify(["8829a1d753fffff"])), row)).toBe(
      false,
    );
    expect(evaluate(fn("h3_within", col("h3_8"), "8829a1d757fffff", 0), row)).toBe(true);
    expect(evaluate(fn("h3_within", col("h3_8"), "8829a1d753fffff", 1), row)).toBe(true);
    expect(evaluate(fn("h3_within", col("h3_8"), "8829a1d753fffff", 0), row)).toBe(false);
    expect(evaluate(fn("h3_cell", 34.05, -118.24, 8), row)).toBe("8829a1d757fffff");
    expect(evaluate(fn("h3_parent", col("h3_8"), 7), row)).toBe("8729a1d75ffffff");
  });

  it("parses WKB geometry bytes in spatial functions", () => {
    const point = wkbPoint(-118.24, 34.05);
    const bigEndianPoint = wkbPoint(2, 3, { littleEndian: false });
    const sridPoint = wkbPoint(4, 5, { srid: 4326 });
    const zPoint = wkbPoint(6, 7, { dimensions: 3 });
    const line = wkbLineString([
      [0, 0],
      [3, 4],
    ]);
    const polygon = wkbPolygon([
      [-119, 33],
      [-117, 33],
      [-117, 35],
      [-119, 35],
      [-119, 33],
    ]);
    expect(evaluate(fn("st_x", lit(point)), {})).toBe(-118.24);
    expect(evaluate(fn("st_x", lit(bigEndianPoint)), {})).toBe(2);
    expect(evaluate(fn("st_y", lit(sridPoint)), {})).toBe(5);
    expect(evaluate(fn("st_x", lit(zPoint)), {})).toBe(6);
    expect(evaluate(fn("st_length", lit(line)), {})).toBe(5);
    expect(evaluate(fn("st_contains", lit(polygon), lit(point)), {})).toBe(true);
    expect(evaluate(fn("st_within", lit(point), lit(polygon)), {})).toBe(true);
    expect(evaluate(fn("st_dwithin", lit(point), fn("st_point", -118.25, 34.05), 0.02), {})).toBe(
      true,
    );
    expect(() => evaluate(fn("st_x", lit(new Uint8Array([2, 1, 0, 0, 0]))), {})).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("st_x", lit(new Uint8Array([1, 1, 0]))), {})).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("st_x", lit(wkbPoint(Number.NaN, 1))), {})).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_x", lit(wkbGeometryHeader(4, 0))), {})).toThrowError(LakeqlError);
    expect(() => {
      const trailing = new Uint8Array(point.byteLength + 1);
      trailing.set(point);
      evaluate(fn("st_x", lit(trailing)), {});
    }).toThrowError(LakeqlError);
  });

  it("uses exact geometry, not just bounding boxes, for spatial predicates", () => {
    // Lower-left triangle (x + y <= 4); bbox is [0,0,4,4].
    const triA = '{"type":"Polygon","coordinates":[[[0,0],[4,0],[0,4],[0,0]]]}';
    // Upper-right triangle (x + y >= 6); bbox is [1,1,5,5] — overlaps triA's bbox
    // in [1,1]..[4,4], but the two triangles never actually touch.
    const triB = '{"type":"Polygon","coordinates":[[[5,5],[1,5],[5,1],[5,5]]]}';
    // A point inside triA's bounding box but outside the triangle itself.
    const outside = '{"type":"Point","coordinates":[3.5,3.5]}';
    // A point genuinely inside triA.
    const inside = '{"type":"Point","coordinates":[1,1]}';

    // Bounding boxes overlap, so the cheap prefilter passes — but the exact
    // check must report the geometries as disjoint.
    expect(evaluate(fn("st_intersects", triA, triB), {})).toBe(false);
    expect(evaluate(fn("st_disjoint", triA, triB), {})).toBe(true);
    expect(evaluate(fn("st_dwithin", triA, triB, 1.3), {})).toBe(false);
    expect(evaluate(fn("st_dwithin", triA, triB, 1.5), {})).toBe(true);
    expect(evaluate(fn("st_contains", triA, outside), {})).toBe(false);
    expect(evaluate(fn("st_within", outside, triA), {})).toBe(false);

    // Genuine relationships still hold.
    expect(evaluate(fn("st_intersects", triA, inside), {})).toBe(true);
    expect(evaluate(fn("st_dwithin", triA, inside, 0), {})).toBe(true);
    expect(evaluate(fn("st_contains", triA, inside), {})).toBe(true);
    expect(evaluate(fn("st_within", inside, triA), {})).toBe(true);
    expect(evaluate(fn("st_disjoint", triA, inside), {})).toBe(false);

    // LineStrings and unclosed polygon rings are handled too.
    const crossing = '{"type":"LineString","coordinates":[[0,0],[6,6]]}';
    expect(evaluate(fn("st_intersects", triA, crossing), {})).toBe(true);
    const openTri = '{"type":"Polygon","coordinates":[[[0,0],[4,0],[0,4]]]}';
    expect(evaluate(fn("st_contains", openTri, inside), {})).toBe(true);

    // Geometry types Lakeql cannot evaluate are rejected, not silently coerced.
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"MultiPoint","coordinates":[[0,0]]}', triA), {}),
    ).toThrowError(LakeqlError);
  });

  it("returns null from null-propagating functions", () => {
    expect(
      evaluate(
        {
          kind: "arithmetic",
          op: "add",
          left: { kind: "column", name: "maybe" },
          right: { kind: "literal", value: 1 },
        },
        row,
      ),
    ).toBeNull();
    expect(evaluate(isIn("city", ["Seattle", null]), row)).toBeNull();
    expect(evaluate(fn("lower", lit(null)), row)).toBeNull();
    expect(evaluate(fn("upper", lit(null)), row)).toBeNull();
    expect(evaluate(fn("trim", lit(null)), row)).toBeNull();
    expect(evaluate(fn("substr", lit(null), 0, 1), row)).toBeNull();
    expect(evaluate(fn("substr", col("city"), lit(null), 1), row)).toBeNull();
    expect(evaluate(fn("substr", col("city"), 0, lit(null)), row)).toBeNull();
    expect(evaluate(fn("replace", col("city"), lit(null), "x"), row)).toBeNull();
    expect(evaluate(fn("replace", lit(null), "x", "y"), row)).toBeNull();
    expect(evaluate(fn("replace", col("city"), "x", lit(null)), row)).toBeNull();
    expect(evaluate(fn("regexp_matches", lit(null), "x"), row)).toBeNull();
    expect(evaluate(fn("regexp_matches", col("city"), lit(null)), row)).toBeNull();
    expect(evaluate(fn("regexp_replace", lit(null), "x", "y"), row)).toBeNull();
    expect(evaluate(fn("regexp_replace", col("city"), "x", lit(null)), row)).toBeNull();
    expect(evaluate(fn("nullif", lit(null), "x"), row)).toBeNull();
    expect(evaluate(fn("cast", lit(null), "number"), row)).toBeNull();
    expect(evaluate(fn("cast", "not-a-number", "number"), row)).toBeNull();
    expect(evaluate(fn("year", lit(null)), row)).toBeNull();
    expect(evaluate(fn("date_trunc", "day", lit(null)), row)).toBeNull();
    expect(evaluate(fn("date_trunc", lit(null), col("date")), row)).toBeNull();
    expect(evaluate(fn("round", lit(null)), row)).toBeNull();
    expect(evaluate(fn("round", col("amount"), lit(null)), row)).toBe(12);
    expect(evaluate(fn("floor", lit(null)), row)).toBeNull();
    expect(evaluate(fn("ceil", lit(null)), row)).toBeNull();
    expect(evaluate(fn("abs", lit(null)), row)).toBeNull();
    expect(evaluate(fn("least", 1, lit(null)), row)).toBeNull();
    expect(evaluate(fn("greatest", 1, lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_x", lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_y", lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_intersects", lit(null), col("geom")), row)).toBeNull();
    expect(evaluate(fn("st_contains", lit(null), col("geom")), row)).toBeNull();
    expect(evaluate(fn("st_within", lit(null), col("geom")), row)).toBeNull();
    expect(evaluate(fn("st_disjoint", lit(null), col("geom")), row)).toBeNull();
    expect(evaluate(fn("st_dwithin", lit(null), col("geom"), 1), row)).toBeNull();
    expect(evaluate(fn("st_dwithin", col("geom"), lit(null), 1), row)).toBeNull();
    expect(evaluate(fn("st_dwithin", col("geom"), col("polygon"), lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_distance", lit(null), col("geom")), row)).toBeNull();
    expect(evaluate(fn("st_area", lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_length", lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_centroid", lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_envelope", lit(null)), row)).toBeNull();
    expect(evaluate(fn("h3_in", lit(null), JSON.stringify(["8829a1d757fffff"])), row)).toBeNull();
    expect(evaluate(fn("h3_in", col("h3_8"), lit(null)), row)).toBeNull();
    expect(evaluate(fn("h3_within", lit(null), "8829a1d757fffff", 1), row)).toBeNull();
    expect(evaluate(fn("h3_within", col("h3_8"), lit(null), 1), row)).toBeNull();
    expect(evaluate(fn("h3_within", col("h3_8"), "8829a1d757fffff", lit(null)), row)).toBeNull();
    expect(evaluate(fn("h3_parent", lit(null), 7), row)).toBeNull();
    expect(evaluate(fn("h3_parent", col("h3_8"), lit(null)), row)).toBeNull();
  });

  it("throws typed errors for unknown columns and functions", () => {
    expect(() => evaluate(eq("missing", 1), row)).toThrowError(LakeqlError);
    expect(() => evaluate(eq("city", 1), row)).toThrowError(LakeqlError);
    expect(() => evaluate(eq("nested", 1), { nested: { value: 1 } })).toThrowError(LakeqlError);
    expect(() => evaluate(fn("not_a_function", 1), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("lower"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("lower", 1), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("substr", col("city"), "x", 1), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("replace", col("city"), 1, "x"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("regexp_matches", col("city")), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("regexp_matches", col("city"), "*"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("regexp_matches", col("city"), "x", "g"), row)).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("regexp_replace", col("city"), "x", "y", "z"), row)).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("cast", col("city"), "unknown"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("date_trunc", 1, col("date")), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("date_trunc", "week", col("date")), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("year", "not-a-date"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("round"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("round", col("city")), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("least"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_bbox", -118, 34, -119, 35), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_bbox", "x", 34, -118, 35), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_point", "x", 34), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_x", col("polygon")), row)).toThrowError(LakeqlError);
    expect(() =>
      evaluate(fn("st_envelope", '{"type":"MultiPoint","coordinates":[]}'), row),
    ).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_intersects", "not-json", col("geom")), row)).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("st_intersects", "{}", col("geom")), row)).toThrowError(LakeqlError);
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"LineString","coordinates":[]}', col("geom")), row),
    ).toThrowError(LakeqlError);
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"Point","coordinates":["x",1]}', col("geom")), row),
    ).toThrowError(LakeqlError);
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"Polygon","coordinates":[]}', col("geom")), row),
    ).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_dwithin", col("geom"), col("polygon"), -1), row)).toThrowError(
      LakeqlError,
    );
    expect(() =>
      evaluate(fn("st_length", '{"type":"LineString","coordinates":[]}'), row),
    ).toThrowError(LakeqlError);
    expect(() =>
      evaluate(fn("st_area", '{"type":"MultiPoint","coordinates":[]}'), row),
    ).toThrowError(LakeqlError);
    expect(() =>
      evaluate(fn("st_length", '{"type":"MultiPoint","coordinates":[]}'), row),
    ).toThrowError(LakeqlError);
    expect(() => evaluate(fn("st_area", '{"type":"Polygon","coordinates":{}}'), row)).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("h3_in", col("h3_8"), "{}"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("h3_in", 1, "[]"), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("h3_within", 1, "8829a1d757fffff", 1), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("h3_within", col("h3_8"), "8829a1d757fffff", -1), row)).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("h3_within", col("h3_8"), "invalid", 1), row)).toThrowError(
      LakeqlError,
    );
    expect(() => evaluate(fn("h3_cell", 34.05, -118.24, 16), row)).toThrowError(LakeqlError);
    expect(() => evaluate(fn("h3_parent", "invalid", 7), row)).toThrowError(LakeqlError);
    expect(() => evaluate(like("amount", "%"), row)).toThrowError(LakeqlError);
    expect(() => matches(lit(1), row)).toThrowError(LakeqlError);
  });

  it("maps unsafe bigint values to strings for JSON output", () => {
    expect(jsonSafeValue({ big: 9007199254740993n, small: 12n })).toEqual({
      big: "9007199254740993",
      small: 12,
    });
  });
});

function wkbPoint(
  x: number,
  y: number,
  options: { littleEndian?: boolean; dimensions?: 2 | 3 | 4; srid?: number } = {},
): Uint8Array {
  const dimensions = options.dimensions ?? 2;
  const sridBytes = options.srid === undefined ? 0 : 4;
  const bytes = new Uint8Array(1 + 4 + sridBytes + dimensions * 8);
  const view = new DataView(bytes.buffer);
  const littleEndian = options.littleEndian ?? true;
  const rawType =
    (dimensions === 2 ? 1 : dimensions === 3 ? 1001 : 3001) |
    (options.srid === undefined ? 0 : 0x20000000);
  view.setUint8(0, littleEndian ? 1 : 0);
  view.setUint32(1, rawType, littleEndian);
  let offset = 5;
  if (options.srid !== undefined) {
    view.setUint32(offset, options.srid, littleEndian);
    offset += 4;
  }
  view.setFloat64(offset, x, littleEndian);
  view.setFloat64(offset + 8, y, littleEndian);
  for (let index = 2; index < dimensions; index += 1) {
    view.setFloat64(offset + index * 8, 10 + index, littleEndian);
  }
  return bytes;
}

function wkbLineString(points: [number, number][]): Uint8Array {
  const bytes = wkbGeometryHeader(2, 4 + points.length * 16);
  const view = new DataView(bytes.buffer);
  view.setUint32(5, points.length, true);
  let offset = 9;
  for (const [x, y] of points) {
    view.setFloat64(offset, x, true);
    view.setFloat64(offset + 8, y, true);
    offset += 16;
  }
  return bytes;
}

function wkbPolygon(points: [number, number][]): Uint8Array {
  const bytes = wkbGeometryHeader(3, 4 + 4 + points.length * 16);
  const view = new DataView(bytes.buffer);
  view.setUint32(5, 1, true);
  view.setUint32(9, points.length, true);
  let offset = 13;
  for (const [x, y] of points) {
    view.setFloat64(offset, x, true);
    view.setFloat64(offset + 8, y, true);
    offset += 16;
  }
  return bytes;
}

function wkbGeometryHeader(type: number, payloadBytes: number): Uint8Array {
  const bytes = new Uint8Array(1 + 4 + payloadBytes);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, 1);
  view.setUint32(1, type, true);
  return bytes;
}
