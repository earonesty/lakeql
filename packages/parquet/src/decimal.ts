import type { DecodedArray } from "hyparquet";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import type { ParquetMetadata } from "./types.js";

export function isIntegerDecimal(element: ColumnDecoder["element"]): boolean {
  return isDecimal(element) && (element.type === "INT32" || element.type === "INT64");
}

export function isDecimal(element: ColumnDecoder["element"]): boolean {
  return element.converted_type === "DECIMAL" || element.logical_type?.type === "DECIMAL";
}

export function convertDecimalValues(
  values: DecodedArray,
  element: ColumnDecoder["element"],
  column: string,
  scope?: MemoryScope,
): DecodedArray {
  const convert = decimalConverter(element, column);
  if (scope === undefined) return Array.from(values, convert);
  const dense = ArrayBuffer.isView(values) || values.every((value) => value != null);
  const c = MATERIALIZATION_CHARGES;
  const bytes = dense ? values.length * 8 : c.array + values.length * (c.arraySlot + c.number);
  const scratch = values.length === 0 ? 0 : decimalConversionScratch(element);
  const stage = scratch > 0 ? scope.childStage(bytes + scratch, "decimal conversion") : undefined;
  try {
    const result = (stage ?? scope).allocate(bytes, "decimal values", () => {
      if (!dense) return Array.from(values, convert);
      const output = new Float64Array(values.length);
      for (let index = 0; index < values.length; index++) {
        output[index] = convert(values[index]) as number;
      }
      return output;
    });
    return stage === undefined ? result.value : scope.adopt(result.retain()).value;
  } finally {
    stage?.dispose();
  }
}

/** Convert a scalar bound without constructing input/output arrays. */
export function convertDecimalValue(
  value: unknown,
  element: ColumnDecoder["element"],
  column: string,
  scope?: MemoryScope,
): number | null | undefined {
  if (scope === undefined) return decimalConverter(element, column)(value);
  const cost = MATERIALIZATION_CHARGES;
  const scratch = decimalConversionScratch(element);
  const stage = scope.childStage(cost.number + scratch, "decimal statistic conversion");
  try {
    const result = stage.allocate(cost.number, "decimal statistic", () =>
      decimalConverter(element, column)(value),
    );
    return scope.adopt(result.retain()).value;
  } finally {
    stage.dispose();
  }
}

/** Serial conversion reuses one bounded string workspace across all values. */
function decimalConversionScratch(element: ColumnDecoder["element"]): number {
  const scale =
    element.scale ?? (element.logical_type?.type === "DECIMAL" ? element.logical_type.scale : 0);
  const c = MATERIALIZATION_CHARGES;
  // Two safe integers and their combined exponent string, independent of scale magnitude.
  return scale > 22 ? 3 * c.string + (17 + 16 + 35) * c.stringCodeUnit : 0;
}

function decimalConverter(element: ColumnDecoder["element"], column: string) {
  const scale =
    element.scale ?? (element.logical_type?.type === "DECIMAL" ? element.logical_type.scale : 0);
  if (!Number.isSafeInteger(scale) || scale < 0) {
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
      "Parquet decimal scale must be a nonnegative safe integer",
      { column, feature: "decimal-scale", scale },
    );
  }
  // Powers through 10^22 are exact binary64 integers. Division therefore
  // rounds once; multiplication by 10^-scale would round the factor first.
  // Larger powers require decimal parsing to avoid a rounded divisor (or
  // overflow). The string length is bounded by two safe integers, not scale.
  const divisor = scale <= 22 ? 10 ** scale : undefined;
  const scaled = (value: number): number =>
    divisor === undefined ? Number(`${value}e-${scale}`) : value / divisor;
  return (value: unknown): number | null | undefined => {
    if (value === null || value === undefined) return value;
    if (value instanceof Uint8Array && value.length > 0) {
      // Signed big-endian accumulation stays bounded even for sign-extended
      // physical values. Never construct an input-sized BigInt or string.
      let integer = (value[0] ?? 0) >= 128 ? -1 : 0;
      for (const byte of value) {
        integer = integer * 256 + byte;
        if (!Number.isSafeInteger(integer)) throw unsafeDecimalValue(column, element, value);
      }
      return scaled(integer);
    }
    if (typeof value === "bigint") {
      const numberValue = Number(value);
      if (!Number.isSafeInteger(numberValue) || BigInt(numberValue) !== value) {
        throw unsafeDecimalValue(column, element, value);
      }
      return scaled(numberValue);
    }
    if (typeof value === "number" && Number.isSafeInteger(value)) return scaled(value);
    throw unsafeDecimalValue(column, element, value);
  };
}

/**
 * Decode decimal leaves without upstream conversion so callers can
 * validate the unscaled integer and apply one correctly rounded scaling. Remove only the decimal annotation supplied
 * to the decoder, then apply LakeQL's checked conversion to the raw integers.
 */
export function rawDecimalMetadata(
  metadata: ParquetMetadata,
  columns: readonly string[] | undefined,
): ParquetMetadata {
  let schema: ParquetMetadata["schema"] | undefined;
  for (const [index, element] of selectedDecimalEntries(metadata, columns)) {
    schema ??= metadata.schema.slice();
    const raw = { ...element };
    delete raw.converted_type;
    delete raw.logical_type;
    schema[index] = raw;
  }
  return schema === undefined ? metadata : { ...metadata, schema };
}

export function normalizeDecimalColumns(
  metadata: ParquetMetadata,
  columns: readonly string[],
  values: Record<string, ArrayLike<unknown>>,
): Record<string, ArrayLike<unknown>> {
  let normalized: typeof values | undefined;
  for (const [, element] of selectedDecimalEntries(metadata, columns)) {
    const column = element.name;
    const columnValues = values[column];
    if (columnValues === undefined) continue;
    normalized ??= { ...values };
    normalized[column] = convertDecimalValues(columnValues as DecodedArray, element, column);
  }
  return normalized ?? values;
}

export function normalizeDecimalRows(
  metadata: ParquetMetadata,
  columns: readonly string[],
  rows: Record<string, unknown>[],
): Record<string, unknown>[] {
  const decimalColumns = selectedDecimalEntries(metadata, columns).map(
    ([, element]) => [element.name, decimalConverter(element, element.name)] as const,
  );
  if (decimalColumns.length === 0) return rows;
  return rows.map((row) => {
    const normalized = { ...row };
    for (const [column, convert] of decimalColumns) normalized[column] = convert(row[column]);
    return normalized;
  });
}

function selectedDecimalEntries(metadata: ParquetMetadata, columns: readonly string[] | undefined) {
  const selected = columns === undefined ? undefined : new Set(columns);
  return topLevelScalarSchemaEntries(metadata).filter(
    ([, element]) => isDecimal(element) && (selected === undefined || selected.has(element.name)),
  );
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
  let pending = 1;
  while (pending > 0 && index < schema.length) {
    pending += schemaChildCount(schema[index]) - 1;
    index++;
  }
  return index;
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
