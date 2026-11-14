import type { DataReader, ParquetType } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

// Decimal literals are correctly rounded; runtime pow is not the ALP contract.
const powers = [
  1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17,
  1e18,
];
const inverse = [
  1, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15,
  1e-16, 1e-17, 1e-18,
];

/** Decode a complete ALP value section into one admitted output backing.
 * Preflight validates all vector extents and positions before allocation. No
 * per-vector integer/exception arrays are materialized. BigInt arithmetic is VM
 * scratch, not a portable heap measurement. Exception bits bypass JS numbers.
 */
export function readAlpValues(
  reader: DataReader,
  type: ParquetType,
  count: number,
  scope?: MemoryScope,
): Float32Array | Float64Array {
  if (type !== "FLOAT" && type !== "DOUBLE") invalid("physical type");
  if (!Number.isSafeInteger(count) || count < 0) invalid("value count");
  const width = type === "FLOAT" ? 4 : 8;
  visit(reader, count, width);
  const decode = () => {
    const output = width === 4 ? new Float32Array(count) : new Float64Array(count);
    visit(reader, count, width, output);
    return output;
  };
  const output =
    scope === undefined
      ? decode()
      : scope.allocate(count * width + 2 * c.view, "ALP output and bit view", decode).value;
  reader.offset = reader.view.byteLength;
  return output;
}

function visit(
  reader: DataReader,
  count: number,
  width: number,
  output?: Float32Array | Float64Array,
): void {
  const view = reader.view;
  const start = reader.offset;
  const end = view.byteLength;
  const requireBytes = (offset: number, bytes: number) => {
    if (!Number.isSafeInteger(offset) || offset < start || bytes < 0 || bytes > end - offset)
      invalid("truncated extent");
  };
  requireBytes(start, 7);
  if (view.getUint8(start) !== 0 || view.getUint8(start + 1) !== 0) invalid("mode");
  const log = view.getUint8(start + 2);
  if (log < 3 || log > 15 || view.getInt32(start + 3, true) !== count) invalid("page header");
  const size = 2 ** log;
  const vectors = Math.ceil(count / size);
  const offsets = start + 7;
  requireBytes(offsets, vectors * 4);
  let next = offsets + vectors * 4;
  const bits =
    output === undefined
      ? undefined
      : new DataView(output.buffer, output.byteOffset, output.byteLength);
  for (let v = 0; v < vectors; v++) {
    const position = offsets + view.getUint32(offsets + v * 4, true);
    if (position !== next) invalid("vector offset");
    requireBytes(position, width + 5);
    const exponent = view.getUint8(position);
    const factor = view.getUint8(position + 1);
    const exceptions = view.getUint16(position + 2, true);
    const bitWidth = view.getUint8(position + 4 + width);
    const length = Math.min(size, count - v * size);
    if (
      exponent > (width === 4 ? 10 : 18) ||
      factor > exponent ||
      bitWidth > width * 8 ||
      exceptions > length
    )
      invalid("vector header");
    const packed = position + width + 5;
    const positions = packed + Math.ceil((length * bitWidth) / 8);
    const values = positions + exceptions * 2;
    next = values + exceptions * width;
    requireBytes(packed, next - packed);
    for (let e = 0; e < exceptions; e++) {
      if (view.getUint16(positions + e * 2, true) >= length) invalid("exception position");
    }
    if (output === undefined) continue;
    const base =
      width === 4
        ? BigInt(view.getInt32(position + 4, true))
        : view.getBigInt64(position + 4, true);
    const multiplier = powers[factor] ?? 1;
    const divisor = inverse[exponent] ?? 1;
    for (let i = 0; i < length; i++) {
      let delta = 0n;
      let remaining = bitWidth;
      let bit = i * bitWidth;
      let shift = 0n;
      while (remaining > 0) {
        const offset = bit % 8;
        const take = Math.min(remaining, 8 - offset);
        const part = (view.getUint8(packed + Math.floor(bit / 8)) >>> offset) & ((1 << take) - 1);
        delta |= BigInt(part) << shift;
        shift += BigInt(take);
        remaining -= take;
        bit += take;
      }
      const integer = Number(BigInt.asIntN(width * 8, base + delta));
      output[v * size + i] =
        width === 4
          ? Math.fround(
              Math.fround(Math.fround(integer) * Math.fround(multiplier)) * Math.fround(divisor),
            )
          : integer * multiplier * divisor;
    }
    if (bits !== undefined) {
      for (let e = 0; e < exceptions; e++) {
        const target = (v * size + view.getUint16(positions + e * 2, true)) * width;
        if (width === 4) bits.setUint32(target, view.getUint32(values + e * width, true), true);
        else bits.setBigUint64(target, view.getBigUint64(values + e * width, true), true);
      }
    }
  }
  if (next !== end) invalid("trailing bytes");
}

function invalid(detail: string): never {
  throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Invalid ALP ${detail}`);
}
