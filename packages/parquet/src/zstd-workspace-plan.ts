import { LakeqlError } from "lakeql-core";
import { ZSTD_REUSABLE_WORKSPACE_BYTES } from "./compressors.js";

// One block can create weights, a temporary weight FSE table, a Huffman
// table and three sequence tables. Raw/RLE blocks allocate no entropy tables.
const COMPRESSED_BLOCK_WORKSPACE_BYTES = 292 + 768 + 4096 + 2 * 2560 + 1536;

/** Header-only total-new typed workspace bound for the reusable built-in codec.
 * Does not decode entropy data or certify output size/content correctness.
 */
export function planZstdWorkspaceBytes(input: Uint8Array): number {
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  let offset = 0;
  let extent = 0;
  const requireBytes = (count: number) => {
    if (count > input.length - offset)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Truncated ZSTD framing");
  };
  while (offset < input.length) {
    requireBytes(4);
    const magic = view.getUint32(offset, true);
    if ((magic & 0xfffffff0) === 0x184d2a50) {
      requireBytes(8);
      const length = view.getUint32(offset + 4, true);
      requireBytes(8 + length);
      offset += 8 + length;
      continue;
    }
    if (magic !== 0xfd2fb528)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid ZSTD frame magic");
    requireBytes(5);
    const flags = input[offset + 4] ?? 0;
    if (flags & 8) throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid ZSTD frame flags");
    const single = (flags >>> 5) & 1;
    const sizeFlag = flags >>> 6;
    const dictionaryFlag = flags & 3;
    const headerBytes =
      5 +
      (1 - single) +
      (dictionaryFlag === 3 ? 4 : dictionaryFlag) +
      (sizeFlag === 0 ? single : 1 << sizeFlag);
    requireBytes(headerBytes);
    offset += headerBytes;
    extent = Math.max(extent, 12); // Offset backing is reusable across frames.
    let last = false;
    while (!last) {
      requireBytes(3);
      const header =
        (input[offset] ?? 0) | ((input[offset + 1] ?? 0) << 8) | ((input[offset + 2] ?? 0) << 16);
      offset += 3;
      last = (header & 1) !== 0;
      const type = (header >>> 1) & 3;
      if (type === 3)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Reserved ZSTD block type");
      const storedBytes = type === 1 ? 1 : header >>> 3;
      requireBytes(storedBytes);
      offset += storedBytes;
      if (type === 2)
        extent = Math.min(ZSTD_REUSABLE_WORKSPACE_BYTES, extent + COMPRESSED_BLOCK_WORKSPACE_BYTES);
    }
    if (flags & 4) {
      requireBytes(4);
      offset += 4;
    }
  }
  return extent;
}
