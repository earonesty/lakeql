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
  const scalarIndexes = new Set(topLevelScalarSchemaEntries(metadata).map(([index]) => index));
  let changed = false;
  const schema = metadata.schema.map((element, index) => {
    if (!scalarIndexes.has(index) || (selected !== undefined && !selected.has(element.name))) {
      return element;
    }
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
  return topLevelScalarSchemaEntries(metadata).find(([, element]) => element.name === column)?.[1];
}

function topLevelScalarSchemaEntries(
  metadata: ParquetMetadata,
): Array<readonly [number, ColumnDecoder["element"]]> {
  const entries: Array<readonly [number, ColumnDecoder["element"]]> = [];
  let index = 1;
  for (
    let child = 0;
    child < schemaChildCount(metadata.schema[0]) && index < metadata.schema.length;
    child += 1
  ) {
    const element = metadata.schema[index];
    if (element === undefined) break;
    if (schemaChildCount(element) === 0) entries.push([index, element]);
    index = skipSchemaSubtree(metadata.schema, index);
  }
  return entries;
}

function skipSchemaSubtree(schema: ParquetMetadata["schema"], index: number): number {
  const element = schema[index];
  if (element === undefined) return index + 1;
  let next = index + 1;
  for (let child = 0; child < schemaChildCount(element) && next < schema.length; child += 1) {
    next = skipSchemaSubtree(schema, next);
  }
  return next;
}

function schemaChildCount(element: ColumnDecoder["element"] | undefined): number {
  const count = element?.num_children;
  if (typeof count === "number" && Number.isInteger(count) && count > 0) return count;
  if (typeof count === "bigint" && count > 0n && count <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(count);
  }
  return 0;
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
