import type { MemoryScope } from "lakeql-core";
import { LakeqlError, MATERIALIZATION_CHARGES } from "lakeql-core";
import { readCompactHeader } from "./compact-header.js";

/** Shared forecast for directory admission and indexed-footer headroom selection. */
export function footerDirectoryBytes(groups: number): number {
  const bytes = MATERIALIZATION_CHARGES.view + groups * 8;
  if (!Number.isSafeInteger(groups) || groups < 0 || !Number.isSafeInteger(bytes))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid footer directory capacity");
  return bytes;
}

/** Fixed-width [start,end) pairs relative to the compact metadata view.
 * The caller retains the footer backing separately. No row-group trees survive.
 */
export function indexFooterRowGroups(
  view: DataView,
  scope: MemoryScope,
  validatedCount?: number,
): Uint32Array {
  if (view.byteLength > 0xffffffff)
    throw new LakeqlError(
      "LAKEQL_PARQUET_READ_ERROR",
      "Footer exceeds 32-bit Parquet metadata extent",
    );
  const count = validatedCount ?? countFooterRowGroups(view);
  if (!Number.isSafeInteger(count) || count < 0 || count > view.byteLength)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid footer row-group count");
  const visit = (callback: (start: number, end: number, index: number) => void) =>
    visitFooterRowGroups(view, callback);
  return scope.allocate(footerDirectoryBytes(count), "Parquet footer row-group directory", () => {
    const ranges = new Uint32Array(count * 2);
    let written = 0;
    visit((start, end, index) => {
      if (index !== written || written >= count)
        throw new LakeqlError(
          "LAKEQL_PARQUET_READ_ERROR",
          "Footer changed during directory construction",
        );
      ranges[written * 2] = start;
      ranges[written * 2 + 1] = end;
      written++;
    });
    if (written !== count)
      throw new LakeqlError(
        "LAKEQL_PARQUET_READ_ERROR",
        "Footer changed during directory construction",
      );
    return ranges;
  }).value;
}

/** Validate and count without allocating a decoded group or directory. */
export function countFooterRowGroups(view: DataView): number {
  let count = 0;
  visitFooterRowGroups(view, (_start, _end, index) => {
    if (index !== count++)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Duplicate footer row-group list");
  });
  return count;
}

function visitFooterRowGroups(
  view: DataView,
  callback: (start: number, end: number, index: number) => void,
): void {
  const reader = { view, offset: 0 };
  const envelope = readCompactHeader(reader, { 4: { list: false, visitRange: callback } });
  if (reader.offset !== view.byteLength || envelope.field_4 === undefined)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid footer row-group directory");
}

export function footerRowGroupView(
  view: DataView,
  directory: Uint32Array,
  index: number,
): DataView {
  if (
    !Number.isSafeInteger(index) ||
    index < 0 ||
    index >= directory.length / 2 ||
    directory.length % 2 !== 0
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid footer row-group index");
  const start = directory[index * 2],
    end = directory[index * 2 + 1];
  if (start === undefined || end === undefined || start >= end || end > view.byteLength)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid footer row-group extent");
  return new DataView(view.buffer, view.byteOffset + start, end - start);
}
