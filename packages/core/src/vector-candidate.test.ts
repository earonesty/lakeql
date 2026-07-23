import { describe, expect, it } from "vitest";
import { LakeqlError } from "./errors.js";
import {
  mergePhysicalScoredCandidates,
  type PhysicalVectorCandidateBlock,
  physicalVectorCandidateBytes,
  scorePhysicalVectorCandidates,
  validatePhysicalVectorCandidateBlock,
} from "./vector-candidate.js";

describe("physical vector candidates", () => {
  const block: PhysicalVectorCandidateBlock = {
    rowCount: 5,
    dimensions: 2,
    vectors: Float32Array.of(1, 0, 0, 1, 1, 1, 0, 0, -1, 0),
    rowIdsLow: Uint32Array.of(10, 11, 12, 13, 14),
    rowIdsHigh: Uint32Array.of(1, 1, 1, 1, 1),
    valid: Uint8Array.of(1, 1, 1, 1, 0),
  };

  it("scores dot, l2, and cosine metrics with stable bounded ordering", () => {
    expect(
      scorePhysicalVectorCandidates(block, { query: [1, 0], metric: "dot", limit: 3 }),
    ).toEqual({
      rowIdsLow: Uint32Array.of(10, 12, 11),
      rowIdsHigh: Uint32Array.of(1, 1, 1),
      scores: Float32Array.of(1, 1, 0),
      sourceIndices: Uint32Array.of(0, 2, 1),
    });
    expect(scorePhysicalVectorCandidates(block, { query: [1, 0], metric: "l2", limit: 2 })).toEqual(
      {
        rowIdsLow: Uint32Array.of(10, 12),
        rowIdsHigh: Uint32Array.of(1, 1),
        scores: Float32Array.of(0, 1),
        sourceIndices: Uint32Array.of(0, 2),
      },
    );
    const cosine = scorePhysicalVectorCandidates(block, {
      query: [1, 0],
      metric: "cosine-distance",
      limit: 5,
    });
    expect(cosine.rowIdsLow).toEqual(Uint32Array.of(10, 12, 11, 13));
    expect(cosine.scores.slice(0, 3)).toEqual(
      Float32Array.of(0, Math.fround(1 - Math.fround(1 / Math.fround(Math.sqrt(2)))), 1),
    );
    expect(Number.isNaN(cosine.scores[3])).toBe(true);
  });

  it("merges tile candidates by score and stable source position", () => {
    const left = scorePhysicalVectorCandidates(block, { query: [1, 0], metric: "dot", limit: 2 });
    const right = {
      rowIdsLow: Uint32Array.of(20, 21),
      rowIdsHigh: Uint32Array.of(2, 2),
      scores: Float32Array.of(2, 1),
      sourceIndices: Uint32Array.of(8, 9),
    };
    expect(mergePhysicalScoredCandidates([left, right], "dot", 3)).toEqual({
      rowIdsLow: Uint32Array.of(20, 10, 12),
      rowIdsHigh: Uint32Array.of(2, 1, 1),
      scores: Float32Array.of(2, 1, 1),
      sourceIndices: Uint32Array.of(8, 0, 2),
    });
  });

  it("validates shapes, query precision, limits, and scored buffers", () => {
    expect(physicalVectorCandidateBytes(block)).toBe(5 * 2 * 4 + 5 * 4 + 5 * 4 + 5);
    expect(() =>
      validatePhysicalVectorCandidateBlock({ ...block, vectors: Float32Array.of(1) }),
    ).toThrowError(LakeqlError);
    for (const invalid of [
      { ...block, rowCount: -1 },
      { ...block, dimensions: 0 },
      { ...block, valid: Uint8Array.of(1) },
    ]) {
      expect(() => validatePhysicalVectorCandidateBlock(invalid)).toThrowError(LakeqlError);
    }
    expect(() =>
      scorePhysicalVectorCandidates(block, { query: [1], metric: "dot", limit: 1 }),
    ).toThrow("dimensions");
    expect(() =>
      scorePhysicalVectorCandidates(block, { query: [0.1, 0], metric: "dot", limit: 1 }),
    ).toThrow("finite f32");
    const { valid: _valid, ...withoutValidity } = block;
    expect(
      scorePhysicalVectorCandidates(withoutValidity, {
        query: [0, 0],
        metric: "cosine-distance",
        limit: 0,
      }).scores,
    ).toHaveLength(0);
    expect(() => mergePhysicalScoredCandidates([], "l2", -1)).toThrow("non-negative");
    expect(() =>
      mergePhysicalScoredCandidates(
        [
          {
            rowIdsLow: new Uint32Array(),
            rowIdsHigh: new Uint32Array(),
            scores: Float32Array.of(1),
            sourceIndices: new Uint32Array(),
          },
        ],
        "dot",
        1,
      ),
    ).toThrow("different lengths");
  });

  it("orders NaN tile results last and applies ascending distance order", () => {
    const merged = mergePhysicalScoredCandidates(
      [
        {
          rowIdsLow: Uint32Array.of(1, 2, 3),
          rowIdsHigh: Uint32Array.of(0, 0, 0),
          scores: Float32Array.of(Number.NaN, 2, 1),
          sourceIndices: Uint32Array.of(0, 1, 2),
        },
      ],
      "l2",
      3,
    );
    expect(merged.rowIdsLow).toEqual(Uint32Array.of(3, 2, 1));
    expect(Number.isNaN(merged.scores[2])).toBe(true);
  });
});
