import type { VectorAggregateState, VectorAggregateValue } from "./vector-aggregate.js";

/** First encountered value wins equal-frequency ties. */
export function finalizeMode(
  state: Extract<VectorAggregateState, { op: "mode" }>,
): VectorAggregateValue {
  let best: { value: Exclude<VectorAggregateValue, null>; count: number } | undefined;
  for (const entry of state.values.values()) {
    if (best === undefined || entry.count > best.count) best = entry;
  }
  return best?.value ?? null;
}
