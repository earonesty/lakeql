import type { ColumnMetaData } from "hyparquet";
import { LakeqlError, type MemoryScope } from "lakeql-core";
import { preparePageWindow } from "./page-window.js";
import type { StoreAsyncBuffer } from "./types.js";

/** Inspect the legacy missing-dictionary-offset shape without separate page-body reads.
 * The caller supplies a validated next-column/footer boundary, never an estimate.
 * Recovery is allowed only when complete page headers prove exactly one omitted
 * initial dictionary header and the declared value count. Metadata is not mutated.
 */
export async function dictionaryColumnEnd(
  file: StoreAsyncBuffer,
  column: ColumnMetaData,
  physicalEnd: number,
  scope?: MemoryScope,
): Promise<number> {
  const start = Number(column.dictionary_page_offset ?? column.data_page_offset);
  const size = Number(column.total_compressed_size);
  const declaredEnd = start + size;
  const fail = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid dictionary column extent");
  if (
    !Number.isSafeInteger(start) ||
    start < 4 ||
    !Number.isSafeInteger(size) ||
    size < 0 ||
    !Number.isSafeInteger(declaredEnd) ||
    !Number.isSafeInteger(physicalEnd) ||
    declaredEnd > physicalEnd ||
    physicalEnd > file.byteLength
  )
    throw fail();
  if (
    column.dictionary_page_offset !== undefined ||
    (column.encodings.length > 0 &&
      !column.encodings.some(
        (encoding) => encoding === "PLAIN_DICTIONARY" || encoding === "RLE_DICTIONARY",
      ))
  )
    return declaredEnd;
  const first = await preparePageWindow(file, start, physicalEnd, 0, scope);
  if (first === undefined || first.header.type !== "DICTIONARY_PAGE") return declaredEnd;
  const possibleEnd = declaredEnd + first.headerBytes;
  // No fallback is permitted to consume another column or footer.
  const limit = Math.min(possibleEnd, physicalEnd);
  let at = first.bodyEnd;
  let values = 0n;
  while (at < declaredEnd) {
    const page = await preparePageWindow(file, start, limit, at - start, scope);
    if (page === undefined || page.bodyEnd <= at) throw fail();
    const count =
      page.header.type === "DATA_PAGE"
        ? page.header.data_page_header?.num_values
        : page.header.type === "DATA_PAGE_V2"
          ? page.header.data_page_header_v2?.num_values
          : undefined;
    if (count === undefined || !Number.isSafeInteger(count) || count < 0) throw fail();
    values += BigInt(count);
    if (values > column.num_values) throw fail();
    at = page.bodyEnd;
  }
  if (at === declaredEnd) return declaredEnd;
  if (at !== possibleEnd || possibleEnd > physicalEnd || values !== column.num_values) throw fail();
  return possibleEnd;
}
