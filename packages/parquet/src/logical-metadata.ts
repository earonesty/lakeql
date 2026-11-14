import { EdgeInterpolationAlgorithms } from "hyparquet/src/constants.js";
import type { ThriftObject, ThriftType } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import { decodeMetadataText } from "./metadata-text.js";

const object = (value: ThriftType | undefined) => value as ThriftObject | undefined;
const text = (value: ThriftType | undefined, scope?: MemoryScope) =>
  decodeMetadataText(value as Uint8Array | undefined, scope);
const SIMPLE_TYPES = [
  [1, "STRING"],
  [2, "MAP"],
  [3, "LIST"],
  [4, "ENUM"],
  [6, "DATE"],
  [11, "NULL"],
  [12, "JSON"],
  [13, "BSON"],
  [14, "UUID"],
  [15, "FLOAT16"],
] as const;
const TIME_TYPES = [
  [7, "TIME"],
  [8, "TIMESTAMP"],
] as const;
export function convertLogicalMetadata(
  raw: ThriftObject | undefined,
  scope?: MemoryScope,
): unknown {
  if (raw === undefined) return undefined;
  const allocate = <T>(fields: number, create: () => T): T =>
    scope === undefined
      ? create()
      : scope.allocate(
          MATERIALIZATION_CHARGES.object + fields * MATERIALIZATION_CHARGES.property,
          "converted logical metadata",
          create,
        ).value;
  for (const [field, type] of SIMPLE_TYPES)
    if (raw[`field_${field}`]) return allocate(1, () => ({ type }));
  const decimal = object(raw.field_5);
  if (decimal)
    return allocate(3, () => ({
      type: "DECIMAL",
      scale: decimal.field_1,
      precision: decimal.field_2,
    }));
  for (const [field, type] of TIME_TYPES) {
    const value = object(raw[`field_${field}`]);
    if (value) {
      const unit = object(value.field_2);
      return allocate(3, () => ({
        type,
        isAdjustedToUTC: value.field_1,
        unit: unit?.field_1
          ? "MILLIS"
          : unit?.field_2
            ? "MICROS"
            : unit?.field_3
              ? "NANOS"
              : undefined,
      }));
    }
  }
  const integer = object(raw.field_10);
  if (integer)
    return allocate(3, () => ({
      type: "INTEGER",
      bitWidth: integer.field_1,
      isSigned: integer.field_2,
    }));
  const variant = object(raw.field_16);
  if (variant)
    return allocate(2, () => ({ type: "VARIANT", specification_version: variant.field_1 }));
  const geometry = object(raw.field_17);
  if (geometry)
    return allocate(2, () => ({ type: "GEOMETRY", crs: text(geometry.field_1, scope) }));
  const geography = object(raw.field_18);
  if (geography)
    return allocate(3, () => ({
      type: "GEOGRAPHY",
      crs: text(geography.field_1, scope),
      algorithm: EdgeInterpolationAlgorithms[geography.field_2 as number],
    }));
  return raw;
}
