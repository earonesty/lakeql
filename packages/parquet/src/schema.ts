import { LakeqlError } from "lakeql-core";
import type { ParquetMetadata } from "./types.js";

export interface RejectUnsupportedParquetSchemaOptions {
  columns?: readonly string[] | undefined;
  /** Permit exact value-checked decoding for top-level scalar decimal leaves. */
  allowValueCheckedScalarDecimals?: boolean;
}

export function rejectUnsupportedParquetSchema(
  metadata: Pick<ParquetMetadata, "schema">,
  options: RejectUnsupportedParquetSchemaOptions = {},
): void {
  const schema = metadata.schema;
  if (!Array.isArray(schema) || schema.length === 0) return;
  const selected = options.columns === undefined ? undefined : new Set(options.columns);
  // One path and remaining-child stack: linear traversal, O(depth) workspace.
  // Join the path only when reporting a rejected leaf, not at every group.
  const remaining = [schemaChildCount(schema[0])];
  const path: string[] = [];
  let index = 1;
  while (remaining.length > 0 && index < schema.length) {
    const depth = remaining.length - 1;
    const children = remaining[depth] ?? 0;
    if (children === 0) {
      remaining.pop();
      continue;
    }
    remaining[depth] = children - 1;
    const element = schema[index];
    if (element === undefined) break;
    const name = String(element.name ?? `field_${index}`);
    if (depth === 0 && selected !== undefined && !selected.has(name)) {
      index = skipParquetSchemaSubtree(schema, index);
      continue;
    }
    path.length = depth;
    path.push(name);
    rejectUnsupportedParquetLeaf(element, path, options);
    index++;
    const count = schemaChildCount(element);
    if (count > 0) remaining.push(count);
  }
}

type ParquetSchemaElement = NonNullable<ParquetMetadata["schema"]>[number];

function skipParquetSchemaSubtree(schema: ParquetSchemaElement[], index: number): number {
  let pending = 1;
  while (pending > 0 && index < schema.length) {
    pending += schemaChildCount(schema[index]) - 1;
    index++;
  }
  return index;
}

function schemaChildCount(element: ParquetSchemaElement | undefined): number {
  const count = element?.num_children;
  if (typeof count === "number" && Number.isInteger(count) && count > 0) return count;
  if (typeof count === "bigint" && count > 0n && count <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(count);
  }
  return 0;
}

function rejectUnsupportedParquetLeaf(
  element: ParquetSchemaElement,
  path: string[],
  options: RejectUnsupportedParquetSchemaOptions,
): void {
  const convertedType = String(element.converted_type ?? "").toUpperCase();
  const logicalType = logicalTypeRecord(element.logical_type);
  const logicalTypeName = parquetLogicalTypeName(element.logical_type);
  const decimalPrecision =
    typeof element.precision === "number"
      ? element.precision
      : logicalType?.type === "DECIMAL" && typeof logicalType.precision === "number"
        ? logicalType.precision
        : undefined;
  if (
    (convertedType === "DECIMAL" || logicalTypeName === "DECIMAL") &&
    decimalPrecision !== undefined &&
    decimalPrecision > 15 &&
    !(
      options.allowValueCheckedScalarDecimals === true &&
      path.length === 1 &&
      (element.type === "INT32" ||
        element.type === "INT64" ||
        element.type === "BYTE_ARRAY" ||
        element.type === "FIXED_LEN_BYTE_ARRAY")
    )
  ) {
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
      "Parquet decimals above precision 15 are not supported",
      { column: path.join("."), feature: "decimal-precision", precision: decimalPrecision },
    );
  }
}

function parquetLogicalTypeName(value: unknown): string | undefined {
  if (typeof value === "string") return value.toUpperCase();
  const record = logicalTypeRecord(value);
  if (record === undefined) return undefined;
  if (typeof record.type === "string") return record.type.toUpperCase();
  const keys = Object.keys(record);
  if (keys.length === 0) return undefined;
  return keys[0]?.toUpperCase();
}

function logicalTypeRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
