import { LakeqlError } from "lakeql";

/** Read the portable 32-bit Roaring format without expanding the bitmap. */
export function roaringContains(bytes: Uint8Array): (value: number) => boolean {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const u16 = (offset: number): number => {
    if (offset < 0 || offset + 2 > bytes.length) invalid();
    return view.getUint16(offset, true);
  };
  const u32 = (offset: number): number => {
    if (offset < 0 || offset + 4 > bytes.length) invalid();
    return view.getUint32(offset, true);
  };
  const cookie = u32(0);
  const hasRuns = (cookie & 0xffff) === 12347;
  if (!hasRuns && cookie !== 12346) invalid();
  const count = hasRuns ? (cookie >>> 16) + 1 : u32(4);
  if (count > 65536) invalid();
  const directory = hasRuns ? 4 + Math.ceil(count / 8) : 8;
  const hasOffsets = !hasRuns || count >= 4;
  let offset = directory + count * 4 + (hasOffsets ? count * 4 : 0);
  const dataStart = offset;
  const runAt = (i: number) => hasRuns && ((bytes[4 + (i >>> 3)] ?? 0) & (1 << (i & 7))) !== 0;
  for (let i = 0; i < count; i++) {
    const key = u16(directory + i * 4);
    const cardinality = u16(directory + i * 4 + 2) + 1;
    if (i > 0 && key <= u16(directory + (i - 1) * 4)) invalid();
    if (hasOffsets && u32(directory + count * 4 + i * 4) !== offset) invalid();
    const isRun = runAt(i);
    const runs = isRun ? u16(offset) : -1;
    const size = isRun ? 2 + runs * 4 : cardinality > 4096 ? 8192 : cardinality * 2;
    if (offset + size > bytes.length) invalid();
    if (isRun) {
      let previous = -1;
      let actual = 0;
      for (let j = 0; j < runs; j++) {
        const start = u16(offset + 2 + j * 4);
        const length = u16(offset + 4 + j * 4) + 1;
        if (start <= previous || start + length > 65536) invalid();
        previous = start + length - 1;
        actual += length;
      }
      if (actual !== cardinality) invalid();
    } else if (cardinality <= 4096) {
      for (let j = 1; j < cardinality; j++) {
        if (u16(offset + j * 2) <= u16(offset + (j - 1) * 2)) invalid();
      }
    }
    offset += size;
  }
  if (offset !== bytes.length) invalid();
  return (value) => {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) return false;
    const key = Math.floor(value / 65536);
    const low = value & 0xffff;
    let left = 0;
    let right = count;
    while (left < right) {
      const middle = Math.floor((left + right) / 2);
      if (u16(directory + middle * 4) < key) left = middle + 1;
      else right = middle;
    }
    if (left === count || u16(directory + left * 4) !== key) return false;
    const chosen = left;
    let position = hasOffsets ? u32(directory + count * 4 + chosen * 4) : dataStart;
    // Only run-container bitmaps with fewer than four containers omit offsets.
    if (!hasOffsets) {
      for (let i = 0; i < chosen; i++) {
        const cardinality = u16(directory + i * 4 + 2) + 1;
        position += runAt(i) ? 2 + u16(position) * 4 : cardinality > 4096 ? 8192 : cardinality * 2;
      }
    }
    const container = {
      offset: position,
      cardinality: u16(directory + chosen * 4 + 2) + 1,
      runs: runAt(chosen) ? u16(position) : -1,
    };
    if (container.runs >= 0) {
      for (let i = 0; i < container.runs; i++) {
        const start = u16(container.offset + 2 + i * 4);
        if (low < start) return false;
        if (low <= start + u16(container.offset + 4 + i * 4)) return true;
      }
      return false;
    }
    if (container.cardinality > 4096)
      return ((bytes[container.offset + (low >>> 3)] ?? 0) & (1 << (low & 7))) !== 0;
    left = 0;
    right = container.cardinality;
    while (left < right) {
      const middle = Math.floor((left + right) / 2);
      if (u16(container.offset + middle * 2) < low) left = middle + 1;
      else right = middle;
    }
    return left < container.cardinality && u16(container.offset + left * 2) === low;
  };
}
function invalid(): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", "Invalid portable Roaring fragment bitmap");
}
