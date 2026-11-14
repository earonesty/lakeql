import type { ColumnDecoder, ColumnMetaData } from "hyparquet/src/types.js";
import { lakeqlParquetCompressors } from "./compressors.js";
import { nestedPackedCapability } from "./nested-packed-capability.js";
import { supportsNestedValueEncoding } from "./nested-value-capability.js";

/** Metadata preflight for supported physical columns. RLE/BIT_PACKED in the column encoding
 * set can describe levels; actual page headers remain authoritative and validated.
 * This does not validate schema layout or promise malformed data can be decoded.
 */
export function canReadNestedColumn(column: ColumnMetaData, decoder: ColumnDecoder): boolean {
  if (column.type !== decoder.type || column.codec !== decoder.codec) return false;
  if (column.codec !== "UNCOMPRESSED" && !Object.hasOwn(lakeqlParquetCompressors, column.codec))
    return false;
  if (column.encodings.length === 0) return false;
  if (
    (column.type === "BYTE_ARRAY" || column.type === "FIXED_LEN_BYTE_ARRAY") &&
    nestedPackedCapability(decoder.element) === undefined
  )
    return false;
  let values = false;
  for (const encoding of column.encodings) {
    if (encoding === "RLE" || encoding === "BIT_PACKED") continue;
    if (!supportsNestedValueEncoding(column.type, encoding)) return false;
    values = true;
  }
  return values;
}
