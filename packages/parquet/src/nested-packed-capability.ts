import type { SchemaElement } from "hyparquet/src/types.js";
import { isDecimal } from "./decimal.js";

/** Packed leaf representation/conversion capability. Physical widths and logical
 * parameters still require validation by their decoder before producing values.
 */
export function nestedPackedCapability(
  element: SchemaElement,
): "binary" | "text" | "converted" | undefined {
  if (
    isDecimal(element) ||
    element.converted_type === "JSON" ||
    element.converted_type === "BSON" ||
    element.logical_type?.type === "BSON" ||
    (element.converted_type === undefined &&
      (element.logical_type?.type === "FLOAT16" || element.logical_type?.type === "TIMESTAMP"))
  )
    return "converted";
  if (element.converted_type === "UTF8" || element.logical_type?.type === "STRING") return "text";
  if (element.converted_type === undefined && element.logical_type === undefined) return "binary";
  return undefined;
}
