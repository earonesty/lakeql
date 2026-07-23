import { LakeqlError } from "./errors.js";

export type PhysicalVectorMetric = "cosine-distance" | "dot" | "l2";

export interface PhysicalVectorCandidateBlock {
  readonly rowCount: number;
  readonly dimensions: number;
  readonly vectors: Float32Array;
  readonly rowIdsLow: Uint32Array;
  readonly rowIdsHigh: Uint32Array;
  readonly valid?: Uint8Array;
}

export interface PhysicalScoredCandidates {
  readonly rowIdsLow: Uint32Array;
  readonly rowIdsHigh: Uint32Array;
  readonly scores: Float32Array;
  readonly sourceIndices: Uint32Array;
}

export interface PhysicalVectorScoreOptions {
  readonly query: readonly number[] | Float32Array;
  readonly metric: PhysicalVectorMetric;
  readonly limit: number;
}

interface Candidate {
  index: number;
  score: number;
}

export function scorePhysicalVectorCandidates(
  block: PhysicalVectorCandidateBlock,
  options: PhysicalVectorScoreOptions,
): PhysicalScoredCandidates {
  validatePhysicalVectorCandidateBlock(block);
  validateVectorScoreOptions(block.dimensions, options);
  const query = Float32Array.from(options.query);
  const retained: Candidate[] = [];
  for (let index = 0; index < block.rowCount; index += 1) {
    if (block.valid !== undefined && block.valid[index] !== 1) continue;
    const score = physicalVectorScore(
      block.vectors,
      index,
      block.dimensions,
      query,
      options.metric,
    );
    insertCandidate(retained, { index, score }, options.limit, options.metric);
  }
  const rowIdsLow = new Uint32Array(retained.length);
  const rowIdsHigh = new Uint32Array(retained.length);
  const scores = new Float32Array(retained.length);
  const sourceIndices = new Uint32Array(retained.length);
  for (let index = 0; index < retained.length; index += 1) {
    const candidate = retained[index];
    if (candidate === undefined) continue;
    rowIdsLow[index] = block.rowIdsLow[candidate.index] ?? 0;
    rowIdsHigh[index] = block.rowIdsHigh[candidate.index] ?? 0;
    scores[index] = candidate.score;
    sourceIndices[index] = candidate.index;
  }
  return { rowIdsLow, rowIdsHigh, scores, sourceIndices };
}

export function mergePhysicalScoredCandidates(
  inputs: readonly PhysicalScoredCandidates[],
  metric: PhysicalVectorMetric,
  limit: number,
): PhysicalScoredCandidates {
  if (!Number.isInteger(limit) || limit < 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Vector candidate limit must be non-negative",
      {
        limit,
      },
    );
  }
  const retained: Array<Candidate & { low: number; high: number }> = [];
  for (const input of inputs) {
    validateScoredCandidates(input);
    for (let index = 0; index < input.scores.length; index += 1) {
      const candidate = {
        index: input.sourceIndices[index] ?? 0,
        score: input.scores[index] ?? Number.NaN,
        low: input.rowIdsLow[index] ?? 0,
        high: input.rowIdsHigh[index] ?? 0,
      };
      const position = insertionIndex(retained, candidate, metric);
      retained.splice(position, 0, candidate);
      if (retained.length > limit) retained.pop();
    }
  }
  return {
    rowIdsLow: Uint32Array.from(retained, (candidate) => candidate.low),
    rowIdsHigh: Uint32Array.from(retained, (candidate) => candidate.high),
    scores: Float32Array.from(retained, (candidate) => candidate.score),
    sourceIndices: Uint32Array.from(retained, (candidate) => candidate.index),
  };
}

export function validatePhysicalVectorCandidateBlock(block: PhysicalVectorCandidateBlock): void {
  if (!Number.isInteger(block.rowCount) || block.rowCount < 0 || block.rowCount > 0xffff_ffff) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Vector candidate row count must fit an unsigned 32-bit index",
      { rowCount: block.rowCount },
    );
  }
  if (!Number.isInteger(block.dimensions) || block.dimensions < 1) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Vector candidate dimensions must be a positive integer",
      { dimensions: block.dimensions },
    );
  }
  const expectedValues = block.rowCount * block.dimensions;
  if (
    !Number.isSafeInteger(expectedValues) ||
    block.vectors.length !== expectedValues ||
    block.rowIdsLow.length !== block.rowCount ||
    block.rowIdsHigh.length !== block.rowCount ||
    (block.valid !== undefined && block.valid.length !== block.rowCount)
  ) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Vector candidate buffers do not match the declared shape",
      {
        rowCount: block.rowCount,
        dimensions: block.dimensions,
        vectorValues: block.vectors.length,
        rowIdsLow: block.rowIdsLow.length,
        rowIdsHigh: block.rowIdsHigh.length,
        validity: block.valid?.length,
      },
    );
  }
}

export function physicalVectorCandidateBytes(block: PhysicalVectorCandidateBlock): number {
  return (
    block.vectors.byteLength +
    block.rowIdsLow.byteLength +
    block.rowIdsHigh.byteLength +
    (block.valid?.byteLength ?? 0)
  );
}

function validateVectorScoreOptions(dimensions: number, options: PhysicalVectorScoreOptions): void {
  if (options.query.length !== dimensions) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Vector query dimensions do not match the candidate block",
      { expected: dimensions, actual: options.query.length },
    );
  }
  if (!Number.isInteger(options.limit) || options.limit < 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Vector candidate limit must be non-negative",
      {
        limit: options.limit,
      },
    );
  }
  for (let index = 0; index < options.query.length; index += 1) {
    const value = options.query[index];
    if (value === undefined || !Number.isFinite(value) || Math.fround(value) !== value) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector query values must be finite f32 numbers", {
        index,
        value,
      });
    }
  }
}

function physicalVectorScore(
  vectors: Float32Array,
  row: number,
  dimensions: number,
  query: Float32Array,
  metric: PhysicalVectorMetric,
): number {
  let dot = Math.fround(0);
  let vectorNorm = Math.fround(0);
  let queryNorm = Math.fround(0);
  let l2 = Math.fround(0);
  const offset = row * dimensions;
  for (let dimension = 0; dimension < dimensions; dimension += 1) {
    const value = vectors[offset + dimension] ?? 0;
    const target = query[dimension] ?? 0;
    dot = Math.fround(dot + Math.fround(value * target));
    if (metric === "l2") {
      const delta = Math.fround(value - target);
      l2 = Math.fround(l2 + Math.fround(delta * delta));
    } else if (metric === "cosine-distance") {
      vectorNorm = Math.fround(vectorNorm + Math.fround(value * value));
      queryNorm = Math.fround(queryNorm + Math.fround(target * target));
    }
  }
  if (metric === "dot") return dot;
  if (metric === "l2") return l2;
  if (vectorNorm === 0 || queryNorm === 0) return Number.NaN;
  const denominator = Math.fround(Math.sqrt(Math.fround(vectorNorm * queryNorm)));
  return Math.fround(1 - Math.fround(dot / denominator));
}

function insertCandidate(
  retained: Candidate[],
  candidate: Candidate,
  limit: number,
  metric: PhysicalVectorMetric,
): void {
  const position = insertionIndex(retained, candidate, metric);
  retained.splice(position, 0, candidate);
  if (retained.length > limit) retained.pop();
}

function insertionIndex(
  retained: readonly Candidate[],
  candidate: Candidate,
  metric: PhysicalVectorMetric,
): number {
  let low = 0;
  let high = retained.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const current = retained[middle];
    if (current !== undefined && compareCandidates(current, candidate, metric) <= 0)
      low = middle + 1;
    else high = middle;
  }
  return low;
}

function compareCandidates(
  left: Candidate,
  right: Candidate,
  metric: PhysicalVectorMetric,
): number {
  const leftNan = Number.isNaN(left.score);
  const rightNan = Number.isNaN(right.score);
  if (leftNan || rightNan) {
    if (leftNan !== rightNan) return leftNan ? 1 : -1;
  } else if (left.score !== right.score) {
    return metric === "dot" ? right.score - left.score : left.score - right.score;
  }
  return left.index - right.index;
}

function validateScoredCandidates(input: PhysicalScoredCandidates): void {
  const length = input.scores.length;
  if (
    input.rowIdsLow.length !== length ||
    input.rowIdsHigh.length !== length ||
    input.sourceIndices.length !== length
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Scored candidate buffers have different lengths");
  }
}
