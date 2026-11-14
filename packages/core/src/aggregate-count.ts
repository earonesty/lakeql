/** Serialized row counts must be exactly representable nonnegative integers. */
export function isAggregateCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
