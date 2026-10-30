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
  let bytes = 2;
  let count = 0;
  for (const value of values) {
    bytes += jsonItemBytes(value);
    count += 1;
  }
  return count === 0 ? bytes : bytes + count - 1;
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
  return itemsBefore === 0 ? 2 + itemBytes : listBytes + 1 + itemBytes;
}
