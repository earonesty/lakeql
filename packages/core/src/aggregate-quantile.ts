import { LakeqlError } from "./errors.js";
import { heapSort } from "./heap-sort.js";

export interface QuantileAggregateConfig {
  quantile?: number;
}

export function continuousQuantile(values: readonly number[], quantile: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sortedQuantile(sorted, quantile);
}

/** Private numeric aggregate backing may be reordered; constant sort scratch. */
export function continuousQuantileInPlace(values: number[], quantile: number): number | null {
  heapSort(values, (left, right) => left - right);
  return sortedQuantile(values, quantile);
}

function sortedQuantile(sorted: readonly number[], quantile: number): number | null {
  if (sorted.length === 0) return null;
  const rank = quantile * (sorted.length - 1);
  const lowerIndex = Math.floor(rank);
  const upperIndex = Math.ceil(rank);
  const lower = sorted[lowerIndex];
  const upper = sorted[upperIndex];
  if (lower === undefined || upper === undefined) return null;
  if (lowerIndex === upperIndex) return lower;
  const fraction = rank - lowerIndex;
  // Opposite finite extremes can overflow the subtraction even though their
  // interpolated result is representable. Same-sign differences cannot overflow.
  const difference = upper - lower;
  return Number.isFinite(lower) && Number.isFinite(upper) && !Number.isFinite(difference)
    ? lower * (1 - fraction) + upper * fraction
    : lower + difference * fraction;
}

export function requiredQuantile(aggregate: QuantileAggregateConfig): number {
  if (
    typeof aggregate.quantile !== "number" ||
    !Number.isFinite(aggregate.quantile) ||
    aggregate.quantile < 0 ||
    aggregate.quantile > 1
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "quantile must be a number between 0 and 1", {
      aggregate,
    });
  }
  return aggregate.quantile;
}
