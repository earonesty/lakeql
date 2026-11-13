import type { ColumnDecoder, Encoding } from "hyparquet/src/types.js";

/** Physical value-encoding capability, independent of compression, logical type
 * conversion, level encoding and dictionary availability. */
export function supportsNestedValueEncoding(
  type: ColumnDecoder["type"],
  encoding: Encoding | undefined,
): boolean {
  switch (encoding) {
    case "PLAIN":
      return true;
    case "RLE_DICTIONARY":
    case "PLAIN_DICTIONARY":
      return type !== "BOOLEAN";
    case "DELTA_BINARY_PACKED":
      return type === "INT32" || type === "INT64";
    case "DELTA_BYTE_ARRAY":
    case "DELTA_LENGTH_BYTE_ARRAY":
      return type === "BYTE_ARRAY";
    case "BYTE_STREAM_SPLIT":
      return type === "INT32" || type === "INT64" || type === "FLOAT" || type === "DOUBLE";
    default:
      return false;
  }
}
