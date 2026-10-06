import type { DecodedArray } from "hyparquet";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError } from "lakeql-core";

export function isIntegerDecimal(element: ColumnDecoder["element"]): boolean {
  const decimal = element.converted_type === "DECIMAL" || element.logical_type?.type === "DECIMAL";
  return decimal && (element.type === "INT32" || element.type === "INT64");
}

export function convertIntegerDecimalValues(
  values: DecodedArray,
  element: ColumnDecoder["element"],
  column: string,
): number[] {
  const scale =
    element.scale ?? (element.logical_type?.type === "DECIMAL" ? element.logical_type.scale : 0);
  const factor = 10 ** -scale;
  return Array.from(values, (value) => {
    if (typeof value === "bigint") {
      const numberValue = Number(value);
      if (!Number.isSafeInteger(numberValue) || BigInt(numberValue) !== value) {
        throw unsafeDecimalValue(column, element, value);
      }
      return numberValue * factor;
    }
    if (typeof value === "number" && Number.isSafeInteger(value)) return value * factor;
    throw unsafeDecimalValue(column, element, value);
  });
}

function unsafeDecimalValue(
  column: string,
  element: ColumnDecoder["element"],
  value: unknown,
): LakeqlError {
  return new LakeqlError(
    "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
    "Parquet decimal value exceeds exact JavaScript integer precision",
    {
      column,
      feature: "decimal-value-precision",
      physicalType: element.type,
      precision: element.precision,
      scale:
        element.scale ??
        (element.logical_type?.type === "DECIMAL" ? element.logical_type.scale : undefined),
      value: typeof value === "bigint" ? value.toString() : value,
    },
  );
}
