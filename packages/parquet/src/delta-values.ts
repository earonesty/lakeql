import type { DataReader } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope } from "lakeql-core";

/** Decode integer deltas into admitted output; bit widths remain views of input. */
export function readDeltaValues(
  reader: DataReader,
  count: number,
  int32: boolean,
  scope: MemoryScope,
): Int32Array | BigInt64Array {
  const invalid = () => new LakeqlError("LAKEQL_VALIDATION_ERROR", "Malformed delta binary page");
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    !Number.isSafeInteger(reader.offset) ||
    reader.offset < 0
  )
    throw invalid();
  let offset = reader.offset;
  const byte = () => {
    if (offset >= reader.view.byteLength) throw invalid();
    return reader.view.getUint8(offset++);
  };
  const unsigned = (bits: number): bigint => {
    let value = 0n;
    for (let shift = 0; shift < bits; shift += 7) {
      const part = byte();
      value |= BigInt(part & 127) << BigInt(shift);
      if (part < 128) {
        if (value >= 1n << BigInt(bits)) throw invalid();
        return value;
      }
    }
    throw invalid();
  };
  const signed = () => {
    const value = unsigned(64);
    return (value >> 1n) ^ -(value & 1n);
  };
  const blockSize = Number(unsigned(32));
  const miniblocks = Number(unsigned(32));
  const encodedCount = Number(unsigned(32));
  if (
    blockSize === 0 ||
    blockSize % 128 !== 0 ||
    miniblocks === 0 ||
    blockSize % miniblocks !== 0 ||
    (blockSize / miniblocks) % 32 !== 0 ||
    encodedCount !== count
  )
    throw invalid();
  let value = signed();
  if (count === 0) {
    const empty = scope.allocate(0, "Parquet delta values", () =>
      int32 ? new Int32Array() : new BigInt64Array(),
    ).value;
    reader.offset = offset;
    return empty;
  }

  const output = scope.allocate(count * (int32 ? 4 : 8), "Parquet delta values", () =>
    int32 ? new Int32Array(count) : new BigInt64Array(count),
  ).value;
  const put = (index: number) => {
    if (output instanceof Int32Array) output[index] = Number(BigInt.asIntN(32, value));
    else output[index] = value;
  };
  put(0);
  let written = 1;
  const perMiniblock = blockSize / miniblocks;
  while (written < count) {
    const minimum = signed();
    const widths = offset;
    if (miniblocks > reader.view.byteLength - offset) throw invalid();
    offset += miniblocks;
    for (let block = 0; block < miniblocks && written < count; block += 1) {
      const width = reader.view.getUint8(widths + block);
      if (width > (int32 ? 32 : 64)) throw invalid();
      const size = (perMiniblock * width) / 8;
      if (size > reader.view.byteLength - offset) throw invalid();
      const needed = Math.min(perMiniblock, count - written);
      if (output instanceof Int32Array) {
        let current = Number(BigInt.asIntN(32, value));
        const increment = Number(BigInt.asIntN(32, minimum));
        for (let index = 0; index < needed; index += 1) {
          let delta = 0;
          for (let bit = 0; bit < width; ) {
            const position = index * width + bit;
            const intraByte = position % 8;
            const take = Math.min(8 - intraByte, width - bit);
            const chunk =
              (reader.view.getUint8(offset + Math.floor(position / 8)) >> intraByte) &
              ((1 << take) - 1);
            delta += chunk * 2 ** bit;
            bit += take;
          }
          // All operands fit in 32 bits; the sum is exact as a JS number.
          current = (current + increment + delta) | 0;
          output[written++] = current;
        }
        value = BigInt(current);
        offset += size;
        continue;
      }
      for (let index = 0; index < needed; index += 1) {
        let delta = 0n;
        for (let bit = 0; bit < width; ) {
          const position = index * width + bit;
          const intraByte = position % 8;
          const take = Math.min(8 - intraByte, width - bit);
          const chunk =
            (reader.view.getUint8(offset + Math.floor(position / 8)) >> intraByte) &
            ((1 << take) - 1);
          delta |= BigInt(chunk) << BigInt(bit);
          bit += take;
        }
        value += minimum + delta;
        put(written++);
      }
      offset += size;
    }
  }
  reader.offset = offset;
  return output;
}
