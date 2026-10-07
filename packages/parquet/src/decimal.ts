import type { DecodedArray } from "hyparquet";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError } from "lakeql-core";
import type { ParquetMetadata } from "./types.js";

export function isIntegerDecimal(element: ColumnDecoder["element"]): boolean {
  const decimal = element.converted_type === "DECIMAL" || element.logical_type?.type === "DECIMAL";
  return decimal && (element.type === "INT32" || element.type === "INT64");
}

export function convertIntegerDecimalValues(
  values: DecodedArray,
  element: ColumnDecoder["element"],
  column: string,
): DecodedArray {
  const scale =
    element.scale ?? (element.logical_type?.type === "DECIMAL" ? element.logical_type.scale : 0);
  const factor = 10 ** -scale;
  return Array.from(values, (value) => {
    if (value === null || value === undefined) return value;
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

/**
 * hyparquet converts integer-backed decimals through Number before callers can
 * validate the unscaled integer. Remove only the decimal annotation supplied
 * to the decoder, then apply LakeQL's checked conversion to the raw integers.
 */
export function rawIntegerDecimalMetadata(
  metadata: ParquetMetadata,
  columns: readonly string[] | undefined,
): ParquetMetadata {
  const selected = columns === undefined ? undefined : new Set(columns);
  let changed = false;
  const schema = metadata.schema.map((element, index) => {
    if (index === 0 || (selected !== undefined && !selected.has(element.name))) return element;
    if (!isIntegerDecimal(element)) return element;
    changed = true;
    const raw = { ...element };
    delete raw.converted_type;
    delete raw.logical_type;
    return raw;
  });
  return changed ? { ...metadata, schema } : metadata;
}

export function normalizeIntegerDecimalColumns(
  metadata: ParquetMetadata,
  columns: readonly string[],
  values: Record<string, ArrayLike<unknown>>,
): Record<string, ArrayLike<unknown>> {
  const normalized = { ...values };
  for (const column of columns) {
    const element = topLevelSchemaElement(metadata, column);
    const columnValues = normalized[column];
    if (element === undefined || columnValues === undefined || !isIntegerDecimal(element)) continue;
    normalized[column] = convertIntegerDecimalValues(columnValues as DecodedArray, element, column);
  }
  return normalized;
}

export function normalizeIntegerDecimalRows(
  metadata: ParquetMetadata,
  columns: readonly string[],
  rows: Record<string, unknown>[],
): Record<string, unknown>[] {
  const decimalColumns = columns.flatMap((column) => {
    const element = topLevelSchemaElement(metadata, column);
    return element !== undefined && isIntegerDecimal(element) ? [[column, element] as const] : [];
  });
  if (decimalColumns.length === 0) return rows;
  return rows.map((row) => {
    const normalized = { ...row };
    for (const [column, element] of decimalColumns) {
      normalized[column] = convertIntegerDecimalValues(
        [row[column]] as DecodedArray,
        element,
        column,
      )[0];
    }
    return normalized;
  });
}

function topLevelSchemaElement(
  metadata: ParquetMetadata,
  column: string,
): ColumnDecoder["element"] | undefined {
  return metadata.schema.slice(1).find((element) => element.name === column);
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
