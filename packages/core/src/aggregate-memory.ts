import { RunningCounter } from "./running-counter.js";

/**
 * Buffering aggregates (count_distinct, approx_count_distinct, mode, median, quantile) charge a
 * memory budget the UTF-8 bytes of `JSON.stringify` of the list they buffer. They keep that size
 * as values arrive, so a budgeted aggregate stays linear in its input.
 *
 * `memoryBytes` is 0 while the list has not been measured: the state was built without a memory
 * budget, restored from a snapshot, or rebuilt from sorted runs. A measured list is never 0 bytes,
 * since even `[]` is 2, so the first budgeted update measures the whole list once and every later
 * one adds to it.
 */
export const unmeasuredMemoryBytes = 0;

const textEncoder = new TextEncoder();
// JSON.stringify escapes control characters, so printable ASCII covers most of what it writes.
const printableAscii = /^[ -~]*$/;

/** UTF-8 bytes of one buffered item as `JSON.stringify` writes it inside a list. */
export function jsonItemBytes(value: string | number): number {
  const json = JSON.stringify(value);
  return printableAscii.test(json) ? json.length : textEncoder.encode(json).byteLength;
}

/** UTF-8 bytes of `JSON.stringify(values)` for a whole list. */
export function jsonListBytes(values: Iterable<string | number>): number {
  const usage = new RunningCounter();
  for (const value of values) usage.add(jsonItemBytes(value));
  return usage.listBytes;
}

/**
 * Bytes of a measured list after appending one item: `[` and `]`, each item, and a comma between
 * items. `itemsBefore` is the list's length before the append.
 */
export function jsonListBytesAfterAppend(
  listBytes: number,
  itemsBefore: number,
  itemBytes: number,
): number {
  return RunningCounter.appendListBytes(listBytes, itemsBefore, itemBytes);
}
