import { col, lit } from "@laql/core";
import { describe, expect, it } from "vitest";
import {
  h3Cell,
  h3In,
  h3Parent,
  h3Within,
  stAsGeojson,
  stBBox,
  stContains,
  stDisjoint,
  stEnvelope,
  stFromGeojson,
  stIntersects,
  stIntersectsExpr,
  stPoint,
  stWithin,
  stX,
  stY,
} from "./index.js";

describe("GeoJSON helpers", () => {
  it("creates points, bboxes, envelopes, and GeoJSON round trips", () => {
    const point = stPoint(-118.24, 34.05);
    const polygon = stFromGeojson({
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
    });

    expect(stX(point)).toBe(-118.24);
    expect(stY(point)).toBe(34.05);
    expect(stEnvelope(point)).toEqual(stBBox(-118.24, 34.05, -118.24, 34.05));
    expect(stEnvelope(polygon)).toEqual(stBBox(-119, 33, -117, 35));
    expect(stFromGeojson(stAsGeojson(point))).toEqual(point);
  });

  it("evaluates spatial predicates", () => {
    const losAngeles = stBBox(-118.9, 33.7, -118.1, 34.4);
    const downtown = stBBox(-118.3, 34, -118.2, 34.1);
    const sanDiego = stBBox(-117.3, 32.6, -117.1, 32.8);

    expect(stIntersects(losAngeles, downtown)).toBe(true);
    expect(stContains(losAngeles, downtown)).toBe(true);
    expect(stWithin(downtown, losAngeles)).toBe(true);
    expect(stDisjoint(losAngeles, sanDiego)).toBe(true);
  });

  it("uses exact geometry beyond bounding boxes", () => {
    // Two triangles whose bounding boxes overlap but whose shapes never touch.
    const triA = stFromGeojson({
      type: "Polygon",
      coordinates: [
        [
          [0, 0],
          [4, 0],
          [0, 4],
          [0, 0],
        ],
      ],
    });
    const triB = stFromGeojson({
      type: "Polygon",
      coordinates: [
        [
          [5, 5],
          [1, 5],
          [5, 1],
          [5, 5],
        ],
      ],
    });

    // Envelopes overlap ([0,0,4,4] vs [1,1,5,5]) so the bbox prefilter passes...
    const ea = stEnvelope(triA);
    const eb = stEnvelope(triB);
    expect(ea.maxx >= eb.minx && ea.maxy >= eb.miny).toBe(true);
    // ...but the exact geometry check must report the shapes as disjoint.
    expect(stIntersects(triA, triB)).toBe(false);
    expect(stDisjoint(triA, triB)).toBe(true);
    expect(stContains(triA, stPoint(3.5, 3.5))).toBe(false); // in bbox, outside triangle
    expect(stContains(triA, stPoint(1, 1))).toBe(true); // genuinely inside

    // Unclosed rings are closed before the exact check, and an obviously
    // separate box stays disjoint (exercises the bbox-contains miss path).
    const openTriangle = stFromGeojson({
      type: "Polygon",
      coordinates: [
        [
          [0, 0],
          [4, 0],
          [0, 4],
        ],
      ],
    });
    expect(stContains(openTriangle, stPoint(1, 1))).toBe(true);
    expect(stContains(triA, stBBox(10, 10, 12, 12))).toBe(false);
  });

  it("throws on invalid geometry inputs", () => {
    expect(() => stBBox(2, 0, 1, 1)).toThrow(/bounds/u);
    expect(() => stPoint(Number.NaN, 0)).toThrow(/finite/u);
    expect(() => stEnvelope({ type: "Polygon", coordinates: [] })).toThrow(/at least one/u);
    expect(() => stFromGeojson('{"type":"LineString","coordinates":[]}')).toThrow(
      /Point or Polygon/u,
    );
  });
});

describe("H3 and spatial expression builders", () => {
  it("builds core expressions using documented function names", () => {
    expect(stIntersectsExpr("geom", stBBox(-119, 33, -118, 34))).toEqual({
      kind: "call",
      fn: "st_intersects",
      args: [
        { kind: "column", name: "geom" },
        {
          kind: "literal",
          value: '{"type":"BBox","minx":-119,"miny":33,"maxx":-118,"maxy":34}',
        },
      ],
    });
    expect(h3Cell(lit(34.05), lit(-118.24), 8)).toMatchObject({
      kind: "call",
      fn: "h3_cell",
    });
    expect(h3Within("h3_8", "8829a1d757fffff", 2)).toEqual({
      kind: "call",
      fn: "h3_within",
      args: [
        { kind: "column", name: "h3_8" },
        { kind: "literal", value: "8829a1d757fffff" },
        { kind: "literal", value: 2 },
      ],
    });
    expect(h3Parent(col("h3_8"), 7)).toMatchObject({ fn: "h3_parent" });
    expect(h3In("h3_8", ["a", "b"])).toMatchObject({ fn: "h3_in" });
  });
});
