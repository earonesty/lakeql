import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError } from "lakeql-core";
import type { ParquetMetadata, ReadParquetBatchOptions } from "./types.js";

export type Int96Interpretation = "julian-nanos" | "spark-micros";
export type TemporalColumnDecoder = ColumnDecoder & { int96Interpretation?: Int96Interpretation };
export type NestedReadOptions = ReadParquetBatchOptions & {
  /** Auto uses Spark writer metadata; explicit modes handle missing writer identity. */
  int96Interpretation?: "auto" | Int96Interpretation;
};

export function resolveInt96Interpretation(
  metadata: Pick<ParquetMetadata, "key_value_metadata">,
  mode: NestedReadOptions["int96Interpretation"] = "auto",
): Int96Interpretation {
  if (mode === "julian-nanos" || mode === "spark-micros") return mode;
  if (mode !== "auto")
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid INT96 interpretation");
  return metadata.key_value_metadata?.some(
    (entry) => entry.key === "org.apache.spark.version" && !!entry.value,
  )
    ? "spark-micros"
    : "julian-nanos";
}

/** Spark's signed microsecond arithmetic is a writer compatibility contract,
 * separate from the ordinary Julian-day/unsigned nanosecond INT96 layout.
 * https://github.com/apache/spark/blob/v3.4.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala
 */
export function int96EpochNanos(value: bigint, mode: Int96Interpretation): bigint {
  const day = value >> 64n;
  const low = value & 0xffffffffffffffffn;
  if (mode === "julian-nanos") return (day - 2440588n) * 86400000000000n + low;
  const nanos = BigInt.asIntN(64, low);
  if (nanos % 1000n !== 0n)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Spark INT96 contains sub-microsecond data");
  const days = BigInt.asIntN(32, day - 2440588n);
  return BigInt.asIntN(64, days * 86400000000n + nanos / 1000n) * 1000n;
}
