/**
 * Decoder for the `bitpacking` crate's `BitPacker4x` blocks (128 `u32` values), the layout
 * Lance uses for inverted-index posting blocks and position blocks.
 *
 * The block is four independent lanes: value `4 * j + lane` is the `j`-th `num_bits`-wide
 * field of lane `lane`, and lane words are interleaved (`word(k, lane) = 4 * k + lane`).
 */
export const BLOCK_LEN = 128;

/** Number of bytes occupied by a packed block of `numBits`-wide values. */
export function packedBlockBytes(numBits: number): number {
  return (numBits * BLOCK_LEN) / 8;
}

/** Unpack one block into `out[0..128)`. */
export function unpackBlock(
  bytes: Uint8Array,
  offset: number,
  numBits: number,
  out: Uint32Array,
): void {
  if (numBits > 32) throw new RangeError("Packed block width exceeds 32 bits");
  if (numBits === 0) {
    out.fill(0, 0, BLOCK_LEN);
    return;
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, packedBlockBytes(numBits));
  const mask = numBits === 32 ? 0xffffffff : (1 << numBits) - 1;
  for (let lane = 0; lane < 4; lane += 1) {
    for (let j = 0; j < 32; j += 1) {
      const bit = j * numBits;
      const word = bit >>> 5;
      const shift = bit & 31;
      let value = view.getUint32((4 * word + lane) * 4, true) >>> shift;
      if (shift + numBits > 32) {
        value |= view.getUint32((4 * (word + 1) + lane) * 4, true) << (32 - shift);
      }
      out[4 * j + lane] = (value & mask) >>> 0;
    }
  }
}

/** Unpack a delta-coded block whose first predecessor is `initial`. */
export function unpackSortedBlock(
  bytes: Uint8Array,
  offset: number,
  numBits: number,
  initial: number,
  out: Uint32Array,
): void {
  unpackBlock(bytes, offset, numBits, out);
  let previous = initial;
  for (let i = 0; i < BLOCK_LEN; i += 1) {
    previous = (previous + (out[i] as number)) >>> 0;
    out[i] = previous;
  }
}
