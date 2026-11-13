import type { DecodedArray } from "hyparquet";
import { convert } from "hyparquet/src/convert.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import type { MemoryScope } from "lakeql-core";
import { convertDecimalValues, isDecimal } from "./decimal.js";
import { convertFloat16Values } from "./float16-values.js";
import { convertJsonValues } from "./json-values.js";
import { convertTemporalValues } from "./temporal-values.js";

/** Shared physical-to-logical conversion for flat and nested leaf values.
 * Physical input ownership remains with the caller; conversion allocations use scope.
 * Dictionary indices must be resolved separately, never converted as physical values.
 */
export function convertDecodedValues(
  values: DecodedArray,
  columnDecoder: ColumnDecoder,
  scope?: MemoryScope,
): DecodedArray {
  if (!isDecimal(columnDecoder.element)) {
    if (scope !== undefined) {
      if (columnDecoder.element.converted_type === "JSON") return convertJsonValues(values, scope);
      if (
        columnDecoder.element.converted_type === undefined &&
        columnDecoder.element.logical_type?.type === "FLOAT16"
      )
        return convertFloat16Values(values, scope);
    }
    const temporal = convertTemporalValues(values, columnDecoder, scope);
    if (temporal !== undefined) return temporal;
    return convert(values, columnDecoder);
  }
  return convertDecimalValues(
    values,
    columnDecoder.element,
    columnDecoder.pathInSchema.join("."),
    scope,
  );
}
