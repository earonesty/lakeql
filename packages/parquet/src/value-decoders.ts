import type { ColumnDecoder } from "hyparquet/src/types.js";
import type { MemoryScope } from "lakeql-core";
import { readAlpValues } from "./alp-values.js";

/** One decoder registry for public, fallback and owned page execution. */
export function parquetValueDecoders(
  scope?: MemoryScope,
): NonNullable<ColumnDecoder["valueDecoders"]> {
  return { ALP: (reader, type, count) => readAlpValues(reader, type, count, scope) };
}
