import { describe, expect, it } from "vitest";
import {
  type LanceVectorCandidate,
  parsePositiveReference,
  parseVectorIndexDescription,
  validateIvfFlatAuxiliary,
} from "./vector.js";
import { insertVectorCandidate, lanceVectorDistance } from "./vector-math.js";

describe("Lance vector scoring", () => {
  it("uses Lance L2, dot, and cosine distance semantics", () => {
    const left = Float32Array.of(1, 2);
    const right = Float32Array.of(3, 4);

    expect(lanceVectorDistance(left, right, "l2")).toBe(8);
    expect(lanceVectorDistance(left, right, "dot")).toBe(-10);
    expect(lanceVectorDistance(left, right, "cosine")).toBeCloseTo(1 - 11 / Math.sqrt(5 * 25));
    expect(lanceVectorDistance(Float32Array.of(0, 0), right, "cosine")).toBe(1);
    expect(lanceVectorDistance(left, Float32Array.of(0, 0), "cosine")).toBe(1);
  });

  it("retains a bounded deterministic top-k across distance and row-ID ties", () => {
    const candidates: LanceVectorCandidate[] = [];
    for (const candidate of [
      { rowId: 8n, distance: 2 },
      { rowId: 7n, distance: 2 },
      { rowId: 9n, distance: 1 },
      { rowId: 1n, distance: 3 },
    ]) {
      insertVectorCandidate(candidates, candidate, 3);
    }

    expect(candidates).toEqual([
      { rowId: 9n, distance: 1 },
      { rowId: 7n, distance: 2 },
      { rowId: 8n, distance: 2 },
    ]);
  });

  it("validates vector index descriptions and global-buffer references", () => {
    expect(
      parseVectorIndexDescription(
        new TextEncoder().encode('{"type":"IVF_FLAT","distance_type":"cosine"}'),
      ),
    ).toEqual({ type: "IVF_FLAT", metric: "cosine" });
    for (const bytes of [
      undefined,
      Uint8Array.of(255),
      new TextEncoder().encode("null"),
      new TextEncoder().encode('{"type":1,"distance_type":"l2"}'),
      new TextEncoder().encode('{"type":"IVF_FLAT","distance_type":"other"}'),
    ]) {
      expect(() => parseVectorIndexDescription(bytes)).toThrowError(
        expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
      );
    }

    expect(parsePositiveReference(new TextEncoder().encode("2"), "test")).toBe(2);
    for (const bytes of [undefined, new TextEncoder().encode("0")]) {
      expect(() => parsePositiveReference(bytes, "test")).toThrowError(
        expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
      );
    }
  });

  it("validates ordered IVF_FLAT auxiliary partitions", () => {
    const fields = [
      { id: 0, name: "_rowid", parentId: -1, logicalType: "uint64", nullable: false },
      {
        id: 1,
        name: "flat",
        parentId: -1,
        logicalType: "fixed_size_list:float:2",
        nullable: false,
      },
    ];
    const indexed = {
      id: 1,
      name: "vector",
      parentId: -1,
      logicalType: "fixed_size_list:float:2",
      nullable: false,
    };
    expect(() => validateIvfFlatAuxiliary(fields, 3, [0, 2], [2, 1], indexed)).not.toThrow();
    expect(() => validateIvfFlatAuxiliary([], 0, [], [], indexed)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    expect(() => validateIvfFlatAuxiliary(fields, 3, [0, 1], [2, 1], indexed)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    expect(() => validateIvfFlatAuxiliary(fields, 4, [0, 2], [2, 1], indexed)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });
});
