import type { DataReader } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import type { PackedBinaryValues } from "./binary-values.js";
import { readDeltaValues } from "./delta-values.js";

/** Validate decoded lengths before allocating expanded payload. Prefix/suffix
 * arrays are temporary; one packed payload replaces per-value reconstruction.
 */
export function readPackedDeltaBinaryValues(
  reader: DataReader,
  count: number,
  prefixes: boolean,
  scope: MemoryScope,
): PackedBinaryValues {
  const construction = scope.childScope();
  const scratch = construction.childScope();
  const input = { view: reader.view, offset: reader.offset };
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid delta binary value extent");
  try {
    const prefix = prefixes
      ? (readDeltaValues(input, count, true, scratch) as Int32Array)
      : undefined;
    const suffix = readDeltaValues(input, count, true, scratch) as Int32Array;
    let total = 0,
      encoded = 0,
      previous = 0;
    for (let i = 0; i < count; i++) {
      const p = prefix?.[i] ?? 0,
        s = suffix[i] ?? -1;
      if (p < 0 || p > previous || s < 0) throw invalid();
      previous = p + s;
      total += previous;
      encoded += s;
      if (
        !Number.isSafeInteger(total) ||
        total > 0xffffffff ||
        encoded > input.view.byteLength - input.offset
      )
        throw invalid();
    }
    const ranges = construction.allocate(
      count * 8,
      "delta binary ranges",
      () => new Uint32Array(count * 2),
    ).value;
    const payload = construction.allocate(prefixes ? total : c.view, "delta binary payload", () =>
      prefixes
        ? new Uint8Array(total)
        : new Uint8Array(input.view.buffer, input.view.byteOffset + input.offset, encoded),
    ).value;
    let out = 0,
      previousStart = 0;
    for (let i = 0; i < count; i++) {
      const p = prefix?.[i] ?? 0,
        s = suffix[i] ?? 0;
      ranges[i * 2] = out;
      ranges[i * 2 + 1] = p + s;
      if (prefixes) {
        // The validated prefix refers entirely to the preceding value.
        payload.copyWithin(out, previousStart, previousStart + p);
        for (let j = 0; j < s; j++) payload[out + p + j] = input.view.getUint8(input.offset + j);
      }
      input.offset += s;
      previousStart = out;
      out += p + s;
    }
    const result: PackedBinaryValues = {
      count,
      payload,
      ranges,
      value(index) {
        if (!Number.isSafeInteger(index) || index < 0 || index >= count) throw invalid();
        const start = ranges[index * 2] ?? 0;
        return payload.subarray(start, start + (ranges[index * 2 + 1] ?? 0));
      },
    };
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    reader.offset = input.offset;
    return result;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}
