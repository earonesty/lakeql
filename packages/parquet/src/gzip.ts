import { LakeqlError } from "lakeql-core";

/** Fixed backing for lengths, three canonical Huffman tables, offsets and CRC32.
 * RFC 1951/1952: no growing output, history window, or per-block backing allocation.
 * The caller supplies the complete output and scratch; views borrow those buffers.
 */
export const GZIP_WORKSPACE_BYTES = 2152;
const LITERAL = { counts: 160, symbols: 208 };
const DISTANCE = { counts: 176, symbols: 496 };
const CODE_LENGTH = { counts: 192, symbols: 528 };
const LENGTH_ORDER = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];
type Tree = typeof LITERAL;

export function decodeGzipInto(
  input: Uint8Array,
  output: Uint8Array,
  scratch: Uint8Array,
): Uint8Array {
  const invalid = (message: string): never => {
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Invalid GZIP: ${message}`);
  };
  if (scratch.byteLength < GZIP_WORKSPACE_BYTES || scratch.byteOffset % 4 !== 0)
    invalid("workspace extent/alignment");
  const overlaps = (a: Uint8Array, b: Uint8Array) =>
    a.buffer === b.buffer &&
    a.byteOffset < b.byteOffset + b.byteLength &&
    b.byteOffset < a.byteOffset + a.byteLength;
  if (overlaps(input, output) || overlaps(input, scratch) || overlaps(output, scratch))
    invalid("overlapping input/output/workspace");
  const lengths = scratch.subarray(0, 320);
  const words = new Uint16Array(scratch.buffer, scratch.byteOffset, 563);
  const crcTable = new Uint32Array(scratch.buffer, scratch.byteOffset + 1128, 256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
    crcTable[i] = crc >>> 0;
  }
  const checksum = (bytes: Uint8Array, begin: number, end: number) => {
    let crc = 0xffffffff;
    for (let i = begin; i < end; i++)
      crc = (crc >>> 8) ^ (crcTable[(crc ^ (bytes[i] ?? 0)) & 255] ?? 0);
    return (crc ^ 0xffffffff) >>> 0;
  };
  let offset = 0,
    written = 0,
    bits = 0,
    available = 0;
  const byte = () => {
    if (offset >= input.length) invalid("truncated input");
    return input[offset++] ?? 0;
  };
  const uint16 = () => byte() | (byte() << 8);
  const uint32 = () => (uint16() | (uint16() << 16)) >>> 0;
  const readBits = (count: number) => {
    while (available < count) {
      bits |= byte() << available;
      available += 8;
    }
    const value = bits & ((1 << count) - 1);
    bits >>>= count;
    available -= count;
    return value;
  };
  const align = () => {
    bits = 0;
    available = 0;
  };
  const build = (
    tree: Tree,
    start: number,
    count: number,
    allowEmpty: boolean,
    allowSingle: boolean,
  ) => {
    words.fill(0, tree.counts, tree.counts + 16);
    let symbols = 0;
    for (let i = 0; i < count; i++) {
      const length = lengths[start + i] ?? 0;
      if (length > 15) invalid("Huffman code length");
      words[tree.counts + length] = (words[tree.counts + length] ?? 0) + 1;
      if (length > 0) symbols++;
    }
    if (symbols === 0 && allowEmpty) return;
    let left = 1;
    for (let length = 1; length <= 15; length++) {
      left = left * 2 - (words[tree.counts + length] ?? 0);
      if (left < 0) invalid("oversubscribed Huffman tree");
    }
    if (left > 0 && !(allowSingle && symbols === 1 && words[tree.counts + 1] === 1))
      invalid("incomplete Huffman tree");
    words[548] = 0;
    for (let length = 1; length < 15; length++)
      words[548 + length] = (words[547 + length] ?? 0) + (words[tree.counts + length] ?? 0);
    for (let symbol = 0; symbol < count; symbol++) {
      const length = lengths[start + symbol] ?? 0;
      if (length === 0) continue;
      const position = words[547 + length] ?? 0;
      words[tree.symbols + position] = symbol;
      words[547 + length] = position + 1;
    }
  };
  const symbol = (tree: Tree) => {
    let code = 0,
      first = 0,
      index = 0;
    for (let length = 1; length <= 15; length++) {
      code |= readBits(1);
      const count = words[tree.counts + length] ?? 0;
      if (code < first + count) return words[tree.symbols + index + code - first] ?? 0;
      index += count;
      first = (first + count) * 2;
      code *= 2;
    }
    return invalid("invalid Huffman symbol");
  };
  const capacity = (count: number) => {
    if (count > output.length - written) invalid("output exceeds declared size");
  };
  let members = 0;
  while (offset < input.length) {
    const headerStart = offset;
    if (byte() !== 31 || byte() !== 139 || byte() !== 8) invalid("header signature or method");
    const flags = byte();
    if (flags & 224) invalid("reserved header flags");
    for (let i = 0; i < 6; i++) byte();
    if (flags & 4) {
      const length = uint16();
      for (let i = 0; i < length; i++) byte();
    }
    if (flags & 8)
      while (byte() !== 0) {
        /* filename */
      }
    if (flags & 16)
      while (byte() !== 0) {
        /* comment */
      }
    if (flags & 2) {
      const expected = checksum(input, headerStart, offset) & 0xffff;
      if (uint16() !== expected) invalid("header checksum");
    }
    const memberStart = written;
    let final = 0;
    do {
      final = readBits(1);
      const type = readBits(2);
      if (type === 0) {
        align();
        const length = uint16();
        if ((length ^ uint16()) !== 0xffff) invalid("stored block length");
        capacity(length);
        for (let i = 0; i < length; i++) output[written++] = byte();
        continue;
      }
      if (type === 3) invalid("reserved block type");
      if (type === 1) {
        lengths.fill(8, 0, 144);
        lengths.fill(9, 144, 256);
        lengths.fill(7, 256, 280);
        lengths.fill(8, 280, 288);
        build(LITERAL, 0, 288, false, false);
        lengths.fill(5, 0, 32);
        build(DISTANCE, 0, 32, false, false);
      } else {
        const literalCount = readBits(5) + 257;
        const distanceCount = readBits(5) + 1;
        const codeCount = readBits(4) + 4;
        if (literalCount > 286) invalid("literal alphabet extent");
        lengths.fill(0, 0, 19);
        for (let i = 0; i < codeCount; i++) lengths[LENGTH_ORDER[i] ?? 0] = readBits(3);
        build(CODE_LENGTH, 0, 19, false, false);
        let position = 0;
        while (position < literalCount + distanceCount) {
          const value = symbol(CODE_LENGTH);
          if (value < 16) {
            lengths[position++] = value;
            continue;
          }
          if (value === 16 && position === 0) invalid("missing repeated code length");
          const repeat =
            value === 16 ? readBits(2) + 3 : value === 17 ? readBits(3) + 3 : readBits(7) + 11;
          if (repeat > literalCount + distanceCount - position) invalid("code length run extent");
          lengths.fill(
            value === 16 ? (lengths[position - 1] ?? 0) : 0,
            position,
            position + repeat,
          );
          position += repeat;
        }
        if (lengths[256] === 0) invalid("missing end-of-block code");
        build(LITERAL, 0, literalCount, false, true);
        build(DISTANCE, literalCount, distanceCount, true, true);
      }
      for (;;) {
        const value = symbol(LITERAL);
        if (value < 256) {
          capacity(1);
          output[written++] = value;
          continue;
        }
        if (value === 256) break;
        if (value > 285) invalid("reserved length symbol");
        const index = value - 257;
        const extra = index < 8 || index === 28 ? 0 : (index >> 2) - 1;
        const length =
          (index < 8 ? index + 3 : index === 28 ? 258 : (4 + (index & 3)) * 2 ** extra + 3) +
          readBits(extra);
        const distanceCode = symbol(DISTANCE);
        if (distanceCode > 29) invalid("reserved distance symbol");
        const distanceExtra = distanceCode < 4 ? 0 : (distanceCode >> 1) - 1;
        const distance =
          (distanceCode < 4
            ? distanceCode + 1
            : (2 + (distanceCode & 1)) * 2 ** distanceExtra + 1) + readBits(distanceExtra);
        if (distance > written - memberStart) invalid("distance before member output");
        capacity(length);
        for (let i = 0; i < length; i++) {
          output[written] = output[written - distance] ?? 0;
          written++;
        }
      }
    } while (!final);
    align();
    if (uint32() !== checksum(output, memberStart, written)) invalid("data checksum");
    if (uint32() !== (written - memberStart) >>> 0) invalid("member size");
    members++;
  }
  if (members === 0 || written !== output.length) invalid("output size does not match declaration");
  return output;
}
