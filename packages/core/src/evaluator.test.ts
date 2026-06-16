import { describe, expect, it } from "vitest";
import { LaQLError } from "./errors.js";
import { evaluate, jsonSafeValue, matches } from "./evaluator.js";
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

  it("supports the phase 1 scalar function families", () => {
    expect(evaluate(fn("lower", col("city")), row)).toBe("los angeles");
    expect(evaluate(fn("upper", col("city")), row)).toBe("LOS ANGELES");
    expect(evaluate(fn("trim", col("name")), row)).toBe("Alice");
    expect(evaluate(fn("substr", col("city"), 4, 7), row)).toBe("Angeles");
    expect(evaluate(fn("replace", col("city"), "Los", "San"), row)).toBe("San Angeles");
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
    expect(evaluate(fn("st_contains", triA, outside), {})).toBe(false);
    expect(evaluate(fn("st_within", outside, triA), {})).toBe(false);

    // Genuine relationships still hold.
    expect(evaluate(fn("st_intersects", triA, inside), {})).toBe(true);
    expect(evaluate(fn("st_contains", triA, inside), {})).toBe(true);
    expect(evaluate(fn("st_within", inside, triA), {})).toBe(true);
    expect(evaluate(fn("st_disjoint", triA, inside), {})).toBe(false);

    // LineStrings and unclosed polygon rings are handled too.
    const crossing = '{"type":"LineString","coordinates":[[0,0],[6,6]]}';
    expect(evaluate(fn("st_intersects", triA, crossing), {})).toBe(true);
    const openTri = '{"type":"Polygon","coordinates":[[[0,0],[4,0],[0,4]]]}';
    expect(evaluate(fn("st_contains", openTri, inside), {})).toBe(true);

    // Geometry types LaQL cannot evaluate are rejected, not silently coerced.
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"MultiPoint","coordinates":[[0,0]]}', triA), {}),
    ).toThrowError(LaQLError);
  });

  it("returns null from null-propagating functions", () => {
    expect(evaluate(fn("lower", lit(null)), row)).toBeNull();
    expect(evaluate(fn("substr", lit(null), 0, 1), row)).toBeNull();
    expect(evaluate(fn("replace", col("city"), lit(null), "x"), row)).toBeNull();
    expect(evaluate(fn("year", lit(null)), row)).toBeNull();
    expect(evaluate(fn("date_trunc", "day", lit(null)), row)).toBeNull();
    expect(evaluate(fn("round", lit(null)), row)).toBeNull();
    expect(evaluate(fn("least", 1, lit(null)), row)).toBeNull();
    expect(evaluate(fn("st_intersects", lit(null), col("geom")), row)).toBeNull();
    expect(evaluate(fn("st_length", lit(null)), row)).toBeNull();
    expect(evaluate(fn("h3_in", lit(null), JSON.stringify(["8829a1d757fffff"])), row)).toBeNull();
    expect(evaluate(fn("h3_within", lit(null), "8829a1d757fffff", 1), row)).toBeNull();
  });

  it("throws typed errors for unknown columns and functions", () => {
    expect(() => evaluate(eq("missing", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(eq("city", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(eq("nested", 1), { nested: { value: 1 } })).toThrowError(LaQLError);
    expect(() => evaluate(fn("not_a_function", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("lower"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("lower", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("substr", col("city"), "x", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("replace", col("city"), 1, "x"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("cast", col("city"), "unknown"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("date_trunc", 1, col("date")), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("date_trunc", "week", col("date")), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("year", "not-a-date"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("round"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("round", col("city")), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("least"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("st_bbox", -118, 34, -119, 35), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("st_bbox", "x", 34, -118, 35), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("st_point", "x", 34), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("st_x", col("polygon")), row)).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_envelope", '{"type":"MultiPoint","coordinates":[]}'), row),
    ).toThrowError(LaQLError);
    expect(() => evaluate(fn("st_intersects", "not-json", col("geom")), row)).toThrowError(
      LaQLError,
    );
    expect(() => evaluate(fn("st_intersects", "{}", col("geom")), row)).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"LineString","coordinates":[]}', col("geom")), row),
    ).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"Point","coordinates":["x",1]}', col("geom")), row),
    ).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_intersects", '{"type":"Polygon","coordinates":[]}', col("geom")), row),
    ).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_length", '{"type":"LineString","coordinates":[]}'), row),
    ).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_area", '{"type":"MultiPoint","coordinates":[]}'), row),
    ).toThrowError(LaQLError);
    expect(() =>
      evaluate(fn("st_length", '{"type":"MultiPoint","coordinates":[]}'), row),
    ).toThrowError(LaQLError);
    expect(() => evaluate(fn("st_area", '{"type":"Polygon","coordinates":{}}'), row)).toThrowError(
      LaQLError,
    );
    expect(() => evaluate(fn("h3_in", col("h3_8"), "{}"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("h3_in", 1, "[]"), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("h3_within", 1, "8829a1d757fffff", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("h3_within", col("h3_8"), "8829a1d757fffff", -1), row)).toThrowError(
      LaQLError,
    );
    expect(() => evaluate(fn("h3_within", col("h3_8"), "invalid", 1), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("h3_cell", 34.05, -118.24, 16), row)).toThrowError(LaQLError);
    expect(() => evaluate(fn("h3_parent", "invalid", 7), row)).toThrowError(LaQLError);
    expect(() => evaluate(like("amount", "%"), row)).toThrowError(LaQLError);
    expect(() => matches(lit(1), row)).toThrowError(LaQLError);
  });

  it("maps unsafe bigint values to strings for JSON output", () => {
    expect(jsonSafeValue({ big: 9007199254740993n, small: 12n })).toEqual({
      big: "9007199254740993",
      small: 12,
    });
  });
});
