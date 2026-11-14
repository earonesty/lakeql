import type { Vector } from "./batch.js";

const routing = new WeakSet<object>();

/** Register atomic allocations covering routing/directories, never child payload.
 * This provenance permits alias-aware pruning but says nothing about row order.
 */
export function registerUnionRoutingAllocation(vector: Extract<Vector, { type: "union" }>): void {
  routing.add(vector);
}

export function isOwnedUnionRoutingAllocation(
  value: unknown,
): value is Extract<Vector, { type: "union" }> {
  return typeof value === "object" && value !== null && routing.has(value);
}
