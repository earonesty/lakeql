import { describe, expect, it } from "vitest";
import type { LanceRowIdSegment } from "./proto.js";
import { resolveRequestedRowIds } from "./rowids.js";

describe("stable Lance row-ID resolution", () => {
  it("resolves every supported segment representation and fragment offset", () => {
    const fragments = [
      {
        physicalRows: 4,
        segments: [{ kind: "range", start: 10n, end: 14n }] satisfies LanceRowIdSegment[],
      },
      {
        physicalRows: 7,
        segments: [
          {
            kind: "range_with_holes",
            start: 20n,
            end: 25n,
            holes: [21n, 23n],
          },
          {
            kind: "range_with_bitmap",
            start: 30n,
            end: 35n,
            bitmap: Uint8Array.of(0b1011_0000),
          },
          { kind: "sorted_array", values: [50n] },
        ] satisfies LanceRowIdSegment[],
      },
      {
        physicalRows: 3,
        segments: [{ kind: "array", values: [70n, 65n, 90n] }] satisfies LanceRowIdSegment[],
      },
    ];

    const resolved = resolveRequestedRowIds(
      [13n, 20n, 22n, 30n, 32n, 33n, 50n, 65n, 90n, 999n],
      fragments,
    );

    expect(Object.fromEntries(resolved)).toEqual({
      "13": { fragmentIndex: 0, rowOffset: 3 },
      "20": { fragmentIndex: 1, rowOffset: 0 },
      "22": { fragmentIndex: 1, rowOffset: 1 },
      "30": { fragmentIndex: 1, rowOffset: 3 },
      "32": { fragmentIndex: 1, rowOffset: 4 },
      "33": { fragmentIndex: 1, rowOffset: 5 },
      "50": { fragmentIndex: 1, rowOffset: 6 },
      "65": { fragmentIndex: 2, rowOffset: 1 },
      "90": { fragmentIndex: 2, rowOffset: 2 },
    });
  });

  it.each([
    {
      name: "reversed range",
      physicalRows: 0,
      segment: { kind: "range", start: 2n, end: 1n },
    },
    {
      name: "missing range bound",
      physicalRows: 0,
      segment: { kind: "range", start: 1n },
    },
    {
      name: "unsorted holes",
      physicalRows: 2,
      segment: {
        kind: "range_with_holes",
        start: 1n,
        end: 5n,
        holes: [3n, 2n],
      },
    },
    {
      name: "out-of-range hole",
      physicalRows: 2,
      segment: {
        kind: "range_with_holes",
        start: 1n,
        end: 4n,
        holes: [4n],
      },
    },
    {
      name: "short bitmap",
      physicalRows: 1,
      segment: {
        kind: "range_with_bitmap",
        start: 0n,
        end: 9n,
        bitmap: Uint8Array.of(0xff),
      },
    },
    {
      name: "unsorted sorted-array",
      physicalRows: 2,
      segment: { kind: "sorted_array", values: [8n, 8n] },
    },
    {
      name: "unsafe row offset",
      physicalRows: 0,
      segment: {
        kind: "range",
        start: 0n,
        end: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      },
    },
  ] satisfies {
    name: string;
    physicalRows: number;
    segment: LanceRowIdSegment;
  }[])("rejects an invalid $name segment", ({ physicalRows, segment }) => {
    expect(() =>
      resolveRequestedRowIds([0n], [{ physicalRows, segments: [segment] }]),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_LANCE_READ_ERROR",
      }),
    );
  });

  it("rejects a valid sequence whose length disagrees with the fragment", () => {
    expect(() =>
      resolveRequestedRowIds(
        [99n],
        [
          {
            physicalRows: 3,
            segments: [{ kind: "array", values: [1n, 2n] }],
          },
        ],
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_LANCE_READ_ERROR",
        message: expect.stringContaining("does not match"),
      }),
    );
  });
});
