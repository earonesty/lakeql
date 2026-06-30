import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { timestampFromEpoch } from "lakeql-core";

export const lakeqlParquetParsers = {
  ...DEFAULT_PARSERS,
  timestampFromMilliseconds(value: bigint | number) {
    return timestampFromEpoch(BigInt(value), "millis");
  },
  timestampFromMicroseconds(value: bigint | number) {
    return timestampFromEpoch(BigInt(value), "micros");
  },
  timestampFromNanoseconds(value: bigint | number) {
    return timestampFromEpoch(BigInt(value), "nanos");
  },
  geometryFromBytes(value: Uint8Array | undefined) {
    return value;
  },
  geographyFromBytes(value: Uint8Array | undefined) {
    return value;
  },
};
