import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import {
  MATERIALIZATION_CHARGES as c,
  decimalFromParts,
  LakeqlError,
  type MemoryScope,
  timestampFromEpoch,
} from "lakeql-core";

import { int96EpochNanos, resolveInt96Interpretation } from "./int96-interpretation.js";
import type { ParquetMetadata } from "./types.js";

const stringDecoder = new TextDecoder("utf-8", { ignoreBOM: true });

export const lakeqlParquetParsers = {
  ...DEFAULT_PARSERS,
  decimalFromUnscaled,
  timestampFromInt96(value: bigint) {
    return timestampFromEpoch(int96EpochNanos(value, "julian-nanos"), "nanos");
  },
  stringFromBytes(value: Uint8Array | undefined) {
    return value === undefined ? undefined : stringDecoder.decode(value);
  },
  timestampFromMilliseconds(value: bigint | number | Uint8Array) {
    return timestampFromEpoch(timestampEpoch(value), "millis");
  },
  timestampFromMicroseconds(value: bigint | number | Uint8Array) {
    return timestampFromEpoch(timestampEpoch(value), "micros");
  },
  timestampFromNanoseconds(value: bigint | number | Uint8Array) {
    return timestampFromEpoch(timestampEpoch(value), "nanos");
  },
  geometryFromBytes(value: Uint8Array | undefined) {
    return value;
  },
  geographyFromBytes(value: Uint8Array | undefined) {
    return value;
  },
};

/** FLBA12 TIMESTAMP counts are signed little-endian two's-complement, unlike
 * legacy physical INT96's Julian-day/nanoseconds layout (decoded upstream).
 */
function timestampEpoch(value: bigint | number | Uint8Array): bigint {
  if (!(value instanceof Uint8Array)) return BigInt(value);
  if (value.byteLength !== 12)
    throw new LakeqlError(
      "LAKEQL_PARQUET_READ_ERROR",
      "Timestamp byte carrier must contain 12 bytes",
      { length: value.byteLength },
    );
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  return (BigInt(view.getInt32(8, true)) << 64n) | view.getBigUint64(0, true);
}

const sparkParsers = {
  ...lakeqlParquetParsers,
  timestampFromInt96(value: bigint) {
    return timestampFromEpoch(int96EpochNanos(value, "spark-micros"), "nanos");
  },
};

/** Resolve writer compatibility once per read; parser tables are shared constants. */
export function parquetParsersForMetadata(
  metadata: Pick<ParquetMetadata, "key_value_metadata"> & Partial<Pick<ParquetMetadata, "schema">>,
  scope?: MemoryScope,
) {
  const parsers =
    resolveInt96Interpretation(metadata) === "spark-micros" ? sparkParsers : lakeqlParquetParsers;
  if (
    scope === undefined ||
    (metadata.schema !== undefined &&
      !metadata.schema.some((element) => element.logical_type?.type === "VARIANT"))
  )
    return parsers;
  let properties = 0;
  for (const key in parsers) if (Object.hasOwn(parsers, key)) properties++;
  return scope.allocate(2 * c.object + properties * c.property, "variant parser table", () => ({
    ...parsers,
    decimalFromUnscaled: (coefficient: bigint, scale: number) =>
      decimalFromUnscaled(coefficient, scale, scope),
  })).value;
}

function decimalFromUnscaled(coefficient: bigint, scale: number, scope?: MemoryScope) {
  const value = Number(coefficient);
  if (!Number.isSafeInteger(value)) return decimalFromParts(coefficient, scale, scope);
  // Safe coefficients retain the numeric contract with one scaling rounding.
  if (scale <= 22) return value / 10 ** scale;
  const scratch = scope?.reserve(3 * c.string + 128, "variant decimal scaling text");
  try {
    return Number(`${value}e-${scale}`);
  } finally {
    scratch?.release();
  }
}
