import type { ColumnMetaData } from "hyparquet";

/** Some writers emit zero for an unused dictionary offset on PLAIN columns.
 * A real dictionary at offset zero is retained when dictionary encoding is
 * declared, which also supports standalone column streams used by owned readers.
 */
export function dictionaryPageOffset(column: ColumnMetaData): bigint | undefined {
  const offset = column.dictionary_page_offset;
  return offset === 0n &&
    !column.encodings.some(
      (encoding) => encoding === "RLE_DICTIONARY" || encoding === "PLAIN_DICTIONARY",
    )
    ? undefined
    : offset;
}
