import type { LanceVectorCandidate, LanceVectorMetric } from "./vector.js";

export function lanceVectorDistance(
  left: Float32Array,
  right: Float32Array,
  metric: LanceVectorMetric,
): number {
  let dot = 0;
  let leftNorm = 0;
  let rightNorm = 0;
  let squared = 0;
  for (let index = 0; index < left.length; index += 1) {
    const leftValue = left[index] as number;
    const rightValue = right[index] as number;
    const delta = leftValue - rightValue;
    squared += delta * delta;
    dot += leftValue * rightValue;
    leftNorm += leftValue * leftValue;
    rightNorm += rightValue * rightValue;
  }
  if (metric === "l2") return squared;
  if (metric === "dot") return 1 - dot;
  if (leftNorm === 0 || rightNorm === 0) return 1;
  return 1 - dot / Math.sqrt(leftNorm * rightNorm);
}

export function insertVectorCandidate(
  candidates: LanceVectorCandidate[],
  candidate: LanceVectorCandidate,
  k: number,
): void {
  let position = candidates.findIndex(
    (current) =>
      candidate.distance < current.distance ||
      (candidate.distance === current.distance && candidate.rowId < current.rowId),
  );
  if (position < 0) position = candidates.length;
  candidates.splice(position, 0, candidate);
  if (candidates.length > k) candidates.pop();
}
