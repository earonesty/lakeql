import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { LakeqlError, timestampFromEpoch } from "lakeql-core";

import { int96EpochNanos, resolveInt96Interpretation } from "./int96-interpretation.js";
import type { ParquetMetadata } from "./types.js";

const stringDecoder = new TextDecoder("utf-8", { ignoreBOM: true });

export const lakeqlParquetParsers = {
  ...DEFAULT_PARSERS,
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
export function parquetParsersForMetadata(metadata: Pick<ParquetMetadata, "key_value_metadata">) {
  return resolveInt96Interpretation(metadata) === "spark-micros"
    ? sparkParsers
    : lakeqlParquetParsers;
}
