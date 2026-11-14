import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type BookmarkInit, createBookmark } from "./manifest.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import type { Bookmark, Row } from "./types.js";

/** Normalize with admitted scratch, then detach all retained payload, including
 * expression literals. Input must remain immutable during this synchronous call.
 * The existing bookmark constructor remains the single normalization contract.
 */
export function createOwnedBookmark(
  init: BookmarkInit,
  parent: MemoryScope,
): OwnedMemory<Bookmark> {
  const scratch = parent.childScope();
  try {
    const normalized = scratch.allocate(
      bookmarkNormalizationBytes(init),
      "bookmark normalization workspace",
      () => createBookmark(init),
    ).value;
    return detachOwnedRow(
      normalized as unknown as Row,
      parent.budget,
    ) as unknown as OwnedMemory<Bookmark>;
  } finally {
    scratch.dispose();
  }
}

/** Covers constructed records, arrays, byte copies and sorting directories.
 * Aliased input strings/expressions remain borrowed until the detached output is
 * admitted. Runtime sort/enumerator internals are not portable heap measures.
 */
function bookmarkNormalizationBytes(init: BookmarkInit): number {
  let bytes = 2 * c.object + 13 * c.property;
  const array = (count: number) => c.array + count * c.arraySlot;
  const record = (value: object) => {
    let count = 0;
    for (const key in value) if (Object.hasOwn(value, key)) count++;
    return count;
  };
  const query = init.query;
  if (query !== undefined) {
    bytes += c.object + 12 * c.property;
    if (query.select !== undefined) bytes += array(query.select.length);
    if (query.orderBy !== undefined)
      bytes += array(query.orderBy.length) + query.orderBy.length * (c.object + 3 * c.property);
    for (const values of [query.projections, query.windows]) {
      if (values === undefined) continue;
      const count = record(values);
      // Sorted keys, sorting workspace and window-validation values overlap.
      bytes += c.object + count * c.property + 3 * array(count);
    }
  }
  const write = init.writeState;
  if (write !== undefined) {
    bytes += c.object + 3 * c.property;
    if (write.multipart !== undefined) {
      const count = write.multipart.parts.length;
      bytes += c.object + 3 * c.property + count * (c.object + 3 * c.property) + 2 * array(count);
    }
  }
  const operators = init.operatorState;
  if (operators !== undefined) {
    bytes += c.object + 5 * c.property;
    for (const state of [operators.groupBy, operators.topK, operators.sort]) {
      if (state === undefined) continue;
      bytes += state instanceof Uint8Array ? c.view + state.byteLength : c.object + c.property;
    }
    if (operators.sketches !== undefined) {
      const count = record(operators.sketches);
      bytes += c.object + count * c.property + 2 * array(count) + count * array(2);
      for (const key in operators.sketches)
        if (Object.hasOwn(operators.sketches, key)) {
          const value = operators.sketches[key];
          if (value !== undefined) bytes += c.view + value.byteLength;
        }
    }
  }
  return bytes;
}
