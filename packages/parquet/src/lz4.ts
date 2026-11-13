import { LakeqlError } from "lakeql-core";

/** LZ4 raw block decoder into caller-admitted exact output; no scratch backing.
 * Format: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
 */
export function decodeLz4Into(input: Uint8Array, output: Uint8Array): Uint8Array {
  const written = decodeBlock(input, output);
  if (written !== output.length) fail();
  return output;
}

function fail(): never {
  throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid LZ4 block or output length");
}

function decodeBlock(input: Uint8Array, output: Uint8Array): number {
  if (
    input.buffer === output.buffer &&
    input.byteOffset < output.byteOffset + output.length &&
    output.byteOffset < input.byteOffset + input.length
  )
    fail();
  if (input.length === 0 && output.length === 0) return 0;
  let read = 0;
  let written = 0;
  const length = (initial: number, capacity: number) => {
    let size = initial;
    if (initial === 15) {
      let extra: number;
      do {
        if (read >= input.length) fail();
        extra = input[read++] ?? fail();
        if (extra > capacity - size) fail();
        size += extra;
      } while (extra === 255);
    }
    if (size > capacity) fail();
    return size;
  };
  while (read < input.length) {
    const token = input[read++] ?? fail();
    const literals = length(token >>> 4, output.length - written);
    if (literals > input.length - read) fail();
    for (let i = 0; i < literals; i++) output[written++] = input[read++] ?? fail();
    if (read === input.length) {
      return written;
    }
    if (input.length - read < 2) fail();
    const offset = (input[read++] ?? fail()) | ((input[read++] ?? fail()) << 8);
    if (offset === 0 || offset > written) fail();
    const match = length(token & 15, output.length - written - 4) + 4;
    // Forward copying intentionally supports overlapping matches (offset < match).
    for (let i = 0; i < match; i++) {
      output[written] = output[written - offset] ?? fail();
      written++;
    }
  }
  return fail(); // A complete block ends with a literal sequence, not a match.
}

/** Deprecated Parquet LZ4 accepts Hadoop block framing and historical raw blocks.
 * Both attempts share the exact caller output; no second output or scratch copy.
 */
export function decodeLegacyLz4Into(input: Uint8Array, output: Uint8Array): Uint8Array {
  if (
    input.buffer === output.buffer &&
    input.byteOffset < output.byteOffset + output.length &&
    output.byteOffset < input.byteOffset + input.length
  )
    fail();
  try {
    let read = 0;
    let written = 0;
    const word = () => {
      if (input.length - read < 4) fail();
      const value =
        (input[read] ?? 0) * 0x1000000 +
        (input[read + 1] ?? 0) * 0x10000 +
        (input[read + 2] ?? 0) * 0x100 +
        (input[read + 3] ?? 0);
      read += 4;
      return value;
    };
    while (read < input.length) {
      const blockLength = word();
      if (blockLength === 0) {
        if (read !== input.length || written !== output.length) fail();
        return output;
      }
      if (blockLength > output.length - written) fail();
      const end = written + blockLength;
      while (written < end) {
        const compressed = word();
        if (compressed === 0 || compressed > input.length - read) fail();
        const count = decodeBlock(
          input.subarray(read, read + compressed),
          output.subarray(written, end),
        );
        if (count === 0) fail();
        written += count;
        read += compressed;
      }
    }
    if (written !== output.length) fail();
    return output;
  } catch (error) {
    if (!(error instanceof LakeqlError) || error.code !== "LAKEQL_PARQUET_READ_ERROR") throw error;
    // A raw block does not have a magic discriminator. Validate it independently;
    // every output byte must be overwritten, and matches cannot read prior state.
    return decodeLz4Into(input, output);
  }
}
