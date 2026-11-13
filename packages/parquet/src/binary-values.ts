import type { DataReader } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope } from "lakeql-core";

/** Payload aliases the input; callers must retain its ownership separately. */
export interface PackedBinaryValues {
  readonly count: number;
  readonly payload: Uint8Array;
  /** Interleaved relative start/length pairs; absent for fixed-width input. */
  readonly ranges?: Uint32Array;
  readonly fixedLength?: number;
  value(index: number): Uint8Array;
}

/** Validate all lengths before admitting offset storage. No per-value view array. */
export function readPackedBinaryValues(
  reader: DataReader,
  count: number,
  fixedLength: number | undefined,
  scope: MemoryScope,
): PackedBinaryValues {
  const invalid = () =>
    new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid PLAIN binary value range");
  const start = reader.offset;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    !Number.isSafeInteger(start) ||
    start < 0 ||
    start > reader.view.byteLength
  )
    throw invalid();
  let end = start;
  if (fixedLength !== undefined) {
    if (
      !Number.isSafeInteger(fixedLength) ||
      fixedLength <= 0 ||
      !Number.isSafeInteger(count * fixedLength)
    )
      throw invalid();
    end += count * fixedLength;
    if (end > reader.view.byteLength) throw invalid();
  } else {
    if (count > Math.floor((reader.view.byteLength - start) / 4)) throw invalid();
    for (let index = 0; index < count; index += 1) {
      if (end + 4 > reader.view.byteLength) throw invalid();
      const length = reader.view.getUint32(end, true);
      end += 4;
      if (length > reader.view.byteLength - end) throw invalid();
      end += length;
    }
  }
  // This layout uses 32-bit relative offsets, independent of the source buffer offset.
  if (fixedLength === undefined && end - start > 0xffffffff) throw invalid();
  const payload = new Uint8Array(reader.view.buffer, reader.view.byteOffset + start, end - start);
  let ranges: Uint32Array | undefined;
  if (fixedLength === undefined) {
    ranges = scope.allocate(count * 8, "Parquet binary ranges", () => {
      const pairs = new Uint32Array(count * 2);
      let offset = start;
      for (let index = 0; index < count; index += 1) {
        const length = reader.view.getUint32(offset, true);
        offset += 4;
        pairs[index * 2] = offset - start;
        pairs[index * 2 + 1] = length;
        offset += length;
      }
      return pairs;
    }).value;
  }
  reader.offset = end;
  return {
    count,
    payload,
    ...(ranges === undefined ? {} : { ranges }),
    ...(fixedLength === undefined ? {} : { fixedLength }),
    value(index) {
      if (!Number.isSafeInteger(index) || index < 0 || index >= count) throw invalid();
      const offset = ranges === undefined ? index * (fixedLength ?? 0) : (ranges[index * 2] ?? 0);
      const length = ranges === undefined ? (fixedLength ?? 0) : (ranges[index * 2 + 1] ?? 0);
      return payload.subarray(offset, offset + length);
    },
  };
}
