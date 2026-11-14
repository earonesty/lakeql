import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type OwnedMemory,
} from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import type { decodeCompactNestedColumnPage } from "./nested-column-page.js";

type ColumnPage = NonNullable<ReturnType<typeof decodeCompactNestedColumnPage>>;
export interface NestedRowFragment {
  page: ColumnPage;
  eventStart: number;
  eventEnd: number;
  valueStart: number;
  valueEnd: number;
}
interface Link extends NestedRowFragment {
  next: Link | undefined;
}
export interface NestedColumnRow {
  row: number;
  /** Number of complete rows; omitted for the single-row compatibility contract. */
  rowCount?: number;
  fragments: readonly NestedRowFragment[];
}

/** Seal rows by looking ahead to the next row boundary or column end. Yielded leases
 * expire on advance/close; explicitly retain to keep a row. A large spanning row is
 * bounded by the caller's budget, including all page backing and fragment metadata.
 */
export async function* readNestedColumnRows(
  pages: AsyncIterable<OwnedMemory<ColumnPage>>,
  scope: MemoryScope,
  requestedRows = 1,
  window?: { rowStart: number; rowEnd: number },
): AsyncGenerator<OwnedMemory<NestedColumnRow>> {
  const maxRows = parquetBatchSize(requestedRows);
  let pendingRows = 0;
  let pending: MemoryScope | undefined;
  let first: Link | undefined;
  let last: Link | undefined;
  let count = 0;
  let row = 0;
  const seal = (): OwnedMemory<NestedColumnRow> => {
    if (!pending || !first)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing nested row fragments");
    const fragments = pending.allocate(
      c.array + count * c.arraySlot,
      "sealed row fragment directory",
      () => {
        const output = new Array<NestedRowFragment>(count);
        let link: Link | undefined = first;
        for (let i = 0; i < count; i++) {
          if (!link)
            throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Incomplete nested row fragments");
          output[i] = link;
          link = link.next;
        }
        return output;
      },
    ).value;
    const value = pending.allocate(
      c.object + (maxRows > 1 ? 3 : 2) * c.property,
      "sealed nested row",
      () => ({
        row,
        ...(maxRows > 1 ? { rowCount: pendingRows } : {}),
        fragments,
      }),
    ).value;
    const owner = pending.retainAllocations(value);
    pending.dispose();
    pending = undefined;
    first = last = undefined;
    count = 0;
    row += pendingRows;
    pendingRows = 0;
    return owner;
  };
  try {
    for await (const owner of pages) {
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Nested row scope disposed");
      const page = owner.value;
      const rows = page.rows;
      const segments = rows.eventOffsets.length - 1;
      for (let segment = 0; segment < segments; segment++) {
        const continuation = segment === 0 && rows.continuesPrevious;
        if (continuation && !pending)
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Orphan nested row continuation");
        if (
          !continuation &&
          pending &&
          (pendingRows === maxRows ||
            row + pendingRows === window?.rowStart ||
            row + pendingRows === window?.rowEnd)
        ) {
          const sealed = seal();
          try {
            yield sealed;
          } finally {
            sealed.release();
          }
        }
        if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Nested row scope disposed");
        if (!continuation) pendingRows++;
        pending ??= scope.childScope();
        // Adjacent segments in this page need one pin and one fragment, even
        // when they represent many complete rows in the same bounded block.
        if (last?.page === page) {
          last.eventEnd = rows.eventOffsets[segment + 1] ?? 0;
          last.valueEnd = rows.valueOffsets[segment + 1] ?? 0;
          continue;
        }
        const pin = owner.retainIn(pending.budget);
        try {
          pending.adopt(pin);
        } catch (error) {
          pin.release();
          throw error;
        }
        const fragment = pending.allocate(
          c.object + 6 * c.property,
          "nested row fragment",
          (): Link => ({
            page,
            eventStart: rows.eventOffsets[segment] ?? 0,
            eventEnd: rows.eventOffsets[segment + 1] ?? 0,
            valueStart: rows.valueOffsets[segment] ?? 0,
            valueEnd: rows.valueOffsets[segment + 1] ?? 0,
            next: undefined,
          }),
        ).value;
        if (last) last.next = fragment;
        else first = fragment;
        last = fragment;
        count++;
      }
    }
    if (pending) {
      const sealed = seal();
      try {
        yield sealed;
      } finally {
        sealed.release();
      }
    }
  } finally {
    pending?.dispose();
  }
}
