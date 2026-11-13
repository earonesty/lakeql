import { snappyUncompress } from "hyparquet/src/snappy.js";
import type { Compressors } from "hyparquet/src/types.js";
import { decompressZstdInto as decompressInto, LakeqlError, type MemoryScope } from "lakeql-core";
import { decodeGzipInto, GZIP_WORKSPACE_BYTES } from "./gzip.js";
import { decodeLegacyLz4Into, decodeLz4Into } from "./lz4.js";

/** Conservative total-new backing bound for the exact-size reusable ZSTD pool.
 * Per block: at most three old and three new sequence FSE tables, plus one
 * temporary Huffman FSE table. FSE logs range from 5 through 9. At most two
 * Huffman tables coexist, with power-of-two backing sizes 4 through 4096.
 * Include one weight workspace, frame offsets, and six sequence RLE tables.
 * Summing each possible size class bounds retained free buffers across blocks
 * and frames, not merely live workspace. Output and JS metadata are separate.
 */
export const ZSTD_REUSABLE_WORKSPACE_BYTES =
  7 * (640 + 768 + 1024 + 1536 + 2560) + 2 * (8192 - 4) + 292 + 12 + 6 * 3;

export const lakeqlParquetCompressors: Compressors = {
  SNAPPY: decodeSnappy,
  LZ4: (input, outputLength) => decodeLegacyLz4Into(input, new Uint8Array(outputLength)),
  LZ4_RAW: (input, outputLength) => decodeLz4Into(input, new Uint8Array(outputLength)),
  GZIP(input, outputLength) {
    if (input.length === 0 && outputLength === 0) return new Uint8Array(0);
    return decodeGzipInto(
      input,
      new Uint8Array(outputLength),
      new Uint8Array(GZIP_WORKSPACE_BYTES),
    );
  },
  ZSTD(input, outputLength) {
    return decompressInto(input, new Uint8Array(outputLength));
  },
};

/** Keeps decompression outputs charged for the caller-controlled decode lifetime.
 * ZSTD typed workspace is admitted separately and released after decoding.
 * JavaScript decoder objects and module-level constant tables are not included.
 */
export function scopedParquetCompressors(
  scope: MemoryScope | undefined,
  reuseZstdWorkspace = false,
): Compressors {
  if (scope === undefined) return lakeqlParquetCompressors;
  return {
    LZ4(input, outputLength) {
      return scope.allocate(outputLength, "legacy LZ4 page output", () =>
        decodeLegacyLz4Into(input, new Uint8Array(outputLength)),
      ).value;
    },
    LZ4_RAW(input, outputLength) {
      return scope.allocate(outputLength, "LZ4 raw page output", () =>
        decodeLz4Into(input, new Uint8Array(outputLength)),
      ).value;
    },
    GZIP(input, outputLength) {
      const workspace = scope.childScope();
      try {
        if (input.length === 0 && outputLength === 0)
          return scope.allocate(0, "empty GZIP output", () => new Uint8Array(0)).value;
        const scratch = workspace.allocate(
          GZIP_WORKSPACE_BYTES,
          "GZIP workspace",
          () => new Uint8Array(GZIP_WORKSPACE_BYTES),
        ).value;
        return scope.allocate(outputLength, "GZIP page output", () =>
          decodeGzipInto(input, new Uint8Array(outputLength), scratch),
        ).value;
      } finally {
        workspace.dispose();
      }
    },
    ZSTD(input, outputLength) {
      let workspace: MemoryScope | undefined;
      const allocations = new Map<ArrayBufferLike, Uint8Array>();
      // Admission counts total new backing, not just simultaneously live bytes.
      // Reuse dead tables in admitted stages; incremental scopes can release
      // them immediately to minimize live memory instead.
      const reuse = scope.hasAdmission || reuseZstdWorkspace;
      const busy = new Set<ArrayBufferLike>();
      let allocatedWorkspaceBytes = 0;
      const releaseFrame = () => {
        if (reuse) {
          busy.clear();
          return;
        }
        workspace?.dispose();
        workspace = undefined;
        allocations.clear();
      };
      try {
        return scope.allocate(outputLength, "ZSTD page output", () =>
          decompressInto(
            input,
            new Uint8Array(outputLength),
            (bytes) => {
              if (reuse) {
                for (const [backing, buffer] of allocations) {
                  if (buffer.byteLength === bytes && !busy.has(backing)) {
                    busy.add(backing);
                    buffer.fill(0);
                    return buffer;
                  }
                }
              }
              if (reuse && allocatedWorkspaceBytes + bytes > ZSTD_REUSABLE_WORKSPACE_BYTES) {
                throw new LakeqlError(
                  "LAKEQL_PARQUET_READ_ERROR",
                  "ZSTD workspace exceeds decoder allocation capability",
                );
              }
              workspace ??= scope.childScope();
              const buffer = workspace.allocate(
                bytes,
                "ZSTD codec workspace",
                () => new Uint8Array(bytes),
              ).value;
              allocations.set(buffer.buffer, buffer);
              allocatedWorkspaceBytes += bytes;
              if (reuse) busy.add(buffer.buffer);
              return buffer;
            },
            releaseFrame,
            (live) => {
              for (const [backing, buffer] of allocations) {
                if (!live.includes(backing)) {
                  if (reuse) {
                    busy.delete(backing);
                    continue;
                  }
                  workspace?.releaseAllocation(buffer);
                  allocations.delete(backing);
                }
              }
            },
          ),
        ).value;
      } finally {
        workspace?.dispose();
        allocations.clear();
        busy.clear();
      }
    },
    SNAPPY(input, outputLength) {
      return scope.allocate(outputLength, "Snappy page output", () =>
        decodeSnappy(input, outputLength),
      ).value;
    },
  };
}

function decodeSnappy(input: Uint8Array, outputLength: number): Uint8Array {
  // Data Page V2 may omit the data section entirely when it has no values.
  if (input.length === 0 && outputLength === 0) return new Uint8Array(0);
  let declared = 0;
  let complete = false;
  for (let index = 0; index < Math.min(input.length, 5); index++) {
    const byte = input[index] ?? 0;
    if (index === 4 && byte > 15) break;
    declared += (byte & 127) * 2 ** (index * 7);
    if (byte < 128) {
      complete = true;
      break;
    }
  }
  if (!complete || declared !== outputLength) {
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Snappy length does not match page output", {
      declared,
      outputLength,
    });
  }
  const output = new Uint8Array(outputLength);
  snappyUncompress(input, output);
  return output;
}
