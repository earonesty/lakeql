import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import type { NestedRowFragment } from "./nested-column-rows.js";

export interface NestedFragmentIndex {
  fragments: readonly NestedRowFragment[];
  firstSegments: Uint32Array;
  rowEnds: Float64Array;
}

const invalid = () =>
  new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested fragment window");

function boundary(offsets: Uint32Array, event: number): number {
  let low = 0,
    high = offsets.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    if ((offsets[middle] ?? Infinity) < event) low = middle + 1;
    else high = middle;
  }
  if (offsets[low] !== event) throw invalid();
  return low;
}

/** One admitted index per fragment set, reused across assembly retries/windows.
 * The input fragment directory and its page owners must outlive the index. */
export function indexNestedFragments(
  fragments: readonly NestedRowFragment[],
  scope: MemoryScope,
): NestedFragmentIndex {
  const construction = scope.childScope();
  try {
    const firstSegments = construction.allocate(
      c.view + fragments.length * 4,
      "nested fragment first segments",
      () => new Uint32Array(fragments.length),
    ).value;
    const rowEnds = construction.allocate(
      c.view + fragments.length * 8,
      "nested fragment row ends",
      () => new Float64Array(fragments.length),
    ).value;
    let started = 0;
    for (let i = 0; i < fragments.length; i++) {
      const fragment = fragments[i];
      if (!fragment) throw invalid();
      const rows = fragment.page.rows;
      const first = boundary(rows.eventOffsets, fragment.eventStart);
      const end = boundary(rows.eventOffsets, fragment.eventEnd);
      const continuation = first === 0 && rows.continuesPrevious;
      if (
        end <= first ||
        (continuation && started === 0) ||
        rows.valueOffsets[first] !== fragment.valueStart ||
        rows.valueOffsets[end] !== fragment.valueEnd
      )
        throw invalid();
      started += end - first - Number(continuation);
      if (!Number.isSafeInteger(started)) throw invalid();
      firstSegments[i] = first;
      rowEnds[i] = started;
    }
    const result = construction.allocate(
      c.object + 3 * c.property,
      "nested fragment index",
      () => ({ fragments, firstSegments, rowEnds }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}

/** Select complete root rows, including cross-page continuations. A prepared index
 * makes lookup logarithmic plus the number of selected fragments, avoiding a full
 * fragment scan for every small output under memory pressure. */
export function windowNestedFragments(
  input: readonly NestedRowFragment[] | NestedFragmentIndex,
  start: number,
  count: number,
  scope: MemoryScope,
): NestedRowFragment[] {
  if (
    !Number.isSafeInteger(start) ||
    start < 0 ||
    !Number.isSafeInteger(count) ||
    count < 1 ||
    !Number.isSafeInteger(start + count)
  )
    throw invalid();
  const index = "rowEnds" in input ? input : indexNestedFragments(input, scope);
  const { fragments, firstSegments, rowEnds } = index;
  if (start + count > (rowEnds[rowEnds.length - 1] ?? 0)) throw invalid();
  const output = scope.allocate(
    c.array,
    "nested window fragments",
    () => [] as NestedRowFragment[],
  ).value;
  let low = 0,
    high = fragments.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    if ((rowEnds[middle] ?? 0) <= start) low = middle + 1;
    else high = middle;
  }
  for (let i = low; i < fragments.length; i++) {
    const fragment = fragments[i];
    if (!fragment) throw invalid();
    const rows = fragment.page.rows;
    const first = firstSegments[i] as number;
    const continuation = first === 0 && rows.continuesPrevious;
    const base = (i === 0 ? 0 : (rowEnds[i - 1] as number)) - Number(continuation);
    if (base >= start + count) break;
    const segments = (rowEnds[i] as number) - base;
    const from = Math.max(0, start - base);
    const to = Math.min(segments, start + count - base);
    if (from < to) {
      const selected = scope.allocate(
        c.arraySlot + c.object + 5 * c.property,
        "nested window fragment",
        (): NestedRowFragment => ({
          page: fragment.page,
          eventStart: rows.eventOffsets[first + from] as number,
          eventEnd: rows.eventOffsets[first + to] as number,
          valueStart: rows.valueOffsets[first + from] as number,
          valueEnd: rows.valueOffsets[first + to] as number,
        }),
      ).value;
      output.push(selected);
    }
  }
  if (output.length === 0) throw invalid();
  return output;
}
