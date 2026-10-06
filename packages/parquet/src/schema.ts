import { LakeqlError } from "lakeql-core";
import type { ParquetMetadata } from "./types.js";

export interface RejectUnsupportedParquetSchemaOptions {
  columns?: readonly string[] | undefined;
  allowValueCheckedInt64Decimals?: boolean;
}

export function rejectUnsupportedParquetSchema(
  metadata: ParquetMetadata,
  options: RejectUnsupportedParquetSchemaOptions = {},
): void {
  const schema = metadata.schema;
  if (!Array.isArray(schema) || schema.length === 0) return;
  const selected = options.columns === undefined ? undefined : new Set(options.columns);
  const root = schema[0];
  const childCount = schemaChildCount(root);
  let index = 1;
  for (let child = 0; child < childCount && index < schema.length; child += 1) {
    index = rejectUnsupportedParquetSchemaNode(schema, index, [], selected, options);
  }
}

type ParquetSchemaElement = NonNullable<ParquetMetadata["schema"]>[number];

function rejectUnsupportedParquetSchemaNode(
  schema: ParquetSchemaElement[],
  index: number,
  path: string[],
  selected: ReadonlySet<string> | undefined,
  options: RejectUnsupportedParquetSchemaOptions,
): number {
  const element = schema[index];
  if (element === undefined) return index + 1;
  const name = String(element.name ?? `field_${index}`);
  const nodePath = [...path, name];
  if (selected !== undefined && path.length === 0 && !selected.has(name)) {
    return skipParquetSchemaSubtree(schema, index);
  }
  const childCount = schemaChildCount(element);
  rejectUnsupportedParquetLeaf(element, nodePath, options);
  if (childCount === 0) return index + 1;
  if (isSupportedNestedParquetGroup(element)) {
    return skipParquetSchemaSubtree(schema, index);
  }
  throw new LakeqlError(
    "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
    "Parquet struct columns are not supported",
    {
      column: nodePath.join("."),
      feature: "struct",
    },
  );
}

function skipParquetSchemaSubtree(schema: ParquetSchemaElement[], index: number): number {
  const element = schema[index];
  if (element === undefined) return index + 1;
  let next = index + 1;
  for (let child = 0; child < schemaChildCount(element) && next < schema.length; child += 1) {
    next = skipParquetSchemaSubtree(schema, next);
  }
  return next;
}

function schemaChildCount(element: ParquetSchemaElement | undefined): number {
  const count = element?.num_children;
  if (typeof count === "number" && Number.isInteger(count) && count > 0) return count;
  if (typeof count === "bigint" && count > 0n && count <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(count);
  }
  return 0;
}

function isSupportedNestedParquetGroup(element: ParquetSchemaElement): boolean {
  const convertedType = String(element.converted_type ?? "").toUpperCase();
  if (convertedType === "LIST" || convertedType === "MAP" || convertedType === "MAP_KEY_VALUE") {
    return true;
  }
  const logicalType = parquetLogicalTypeName(element.logical_type);
  return logicalType === "LIST" || logicalType === "MAP";
}

function rejectUnsupportedParquetLeaf(
  element: ParquetSchemaElement,
  path: string[],
  options: RejectUnsupportedParquetSchemaOptions,
): void {
  const column = path.join(".");
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
    !(options.allowValueCheckedInt64Decimals === true && element.type === "INT64")
  ) {
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
      "Parquet decimals above precision 15 are not supported",
      { column, feature: "decimal-precision", precision: decimalPrecision },
    );
  }
}

function parquetLogicalTypeName(value: unknown): string | undefined {
  if (typeof value === "string") return value.toUpperCase();
  if (typeof value !== "object" || value === null) return undefined;
  const keys = Object.keys(value);
  if (keys.length === 0) return undefined;
  return keys[0]?.toUpperCase();
}

function logicalTypeRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
