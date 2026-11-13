import type { DecodedArray } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, type MemoryScope } from "lakeql-core";

import { int96EpochNanos, type TemporalColumnDecoder } from "./int96-interpretation.js";

/** Known temporal conversions create one output array and per-value objects.
 * Reserve before invoking parsers; decoded physical input remains separately owned.
 */
export function convertTemporalValues(
  values: DecodedArray,
  decoder: TemporalColumnDecoder,
  scope: MemoryScope,
): DecodedArray | undefined {
  const { element, parsers } = decoder;
  const converted = element.converted_type;
  const logical = element.logical_type;
  let parser: ((value: never) => unknown) | undefined;
  const int96 = converted === undefined && element.type === "INT96";
  if (int96)
    parser = (value: bigint) =>
      parsers.timestampFromNanoseconds(
        int96EpochNanos(value, decoder.int96Interpretation ?? "julian-nanos"),
      );
  else if (converted === "DATE") parser = parsers.dateFromDays;
  else if (converted === "TIMESTAMP_MILLIS") parser = parsers.timestampFromMilliseconds;
  else if (converted === "TIMESTAMP_MICROS") parser = parsers.timestampFromMicroseconds;
  else if (converted === undefined && logical?.type === "TIMESTAMP") {
    parser =
      logical.unit === "NANOS"
        ? parsers.timestampFromNanoseconds
        : logical.unit === "MICROS"
          ? parsers.timestampFromMicroseconds
          : parsers.timestampFromMilliseconds;
  }
  if (parser === undefined) return undefined;
  let bytes = c.array + values.length * (c.arraySlot + c.timestamp + c.bigint);
  // Custom timestamp carriers can require a temporary view during scalar parsing.
  // Retaining its charge conservatively avoids hiding this parser workspace.
  for (const value of values) if (value instanceof Uint8Array) bytes += c.view;
  const stage = int96
    ? scope.childStage(
        bytes + (decoder.int96Interpretation === "spark-micros" ? 8 : 5) * c.bigint,
        "INT96 temporal conversion",
      )
    : undefined;
  try {
    const output = (stage ?? scope).allocate(bytes, "converted temporal values", () => {
      const output = new Array(values.length);
      for (let index = 0; index < values.length; index++)
        output[index] = parser(values[index] as never);
      return output as DecodedArray;
    });
    return stage === undefined ? output.value : scope.adopt(output.retain()).value;
  } finally {
    stage?.dispose();
  }
}
