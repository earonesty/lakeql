import { glob, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { memoryStore } from "lakeql-core";
import { beforeAll, describe, expect, it } from "vitest";
import { decodeArrowBuffer, decodeArrowDeletionFile, readDeletedRowOffsets } from "./deletions.js";
import { LanceReadContext, type MutableLanceReadStats } from "./io.js";
import type { LanceFragment } from "./proto.js";

const ROOT = resolve(import.meta.dirname, "../fixtures/deletions-v2.0.lance/_deletions");
let fixture: Uint8Array;

beforeAll(async () => {
  const paths = await Array.fromAsync(glob(`${ROOT}/*.arrow`));
  const path = paths[0];
  if (path === undefined) throw new Error("missing deletion fixture");
  fixture = new Uint8Array(await readFile(path));
});

describe("Lance Arrow deletion vectors", () => {
  it("decodes official compressed-buffer metadata and row offsets", () => {
    expect(decodeArrowDeletionFile(fixture, 16, 3)).toEqual(new Set([2, 7, 13]));
  });

  it("decodes uncompressed and Zstandard Arrow buffer envelopes", () => {
    const raw = Uint8Array.of(2, 0, 0, 0, 7, 0, 0, 0, 13, 0, 0, 0);
    const passthrough = envelope(-1n, raw);
    const zstd = Uint8Array.of(
      40,
      181,
      47,
      253,
      32,
      12,
      97,
      0,
      0,
      2,
      0,
      0,
      0,
      7,
      0,
      0,
      0,
      13,
      0,
      0,
      0,
    );

    expect(decodeArrowBuffer(raw, undefined)).toBe(raw);
    expect(decodeArrowBuffer(passthrough, 1)).toEqual(raw);
    expect(decodeArrowBuffer(envelope(12n, zstd), 1)).toEqual(raw);
    expect(() => decodeArrowBuffer(Uint8Array.of(1), 1)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
    expect(() => decodeArrowBuffer(envelope(1n, Uint8Array.of(0)), 0)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" }),
    );
    expect(() =>
      decodeArrowBuffer(envelope(BigInt(Number.MAX_SAFE_INTEGER) + 1n, Uint8Array.of()), 1),
    ).toThrowError(expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }));
  });

  it.each([
    {
      name: "leading magic",
      mutate(bytes: Uint8Array) {
        bytes[0] = 0;
      },
    },
    {
      name: "footer magic",
      mutate(bytes: Uint8Array) {
        bytes[bytes.byteLength - 1] = 0;
      },
    },
    {
      name: "footer offset",
      mutate(bytes: Uint8Array) {
        new DataView(bytes.buffer).setUint32(bytes.byteLength - 10, bytes.byteLength, true);
      },
    },
    {
      name: "record count",
      mutate() {},
      expectedRows: 4,
    },
    {
      name: "physical row bound",
      mutate() {},
      physicalRows: 10,
    },
    {
      name: "duplicate row IDs",
      mutate(bytes: Uint8Array) {
        duplicateOneDeletionValue(bytes);
      },
    },
  ])("rejects corrupt $name", ({ mutate, expectedRows = 3, physicalRows = 16 }) => {
    const bytes = fixture.slice();
    mutate(bytes);
    expect(() => decodeArrowDeletionFile(bytes, physicalRows, expectedRows)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_LANCE_READ_ERROR" }),
    );
  });

  it("defines absent, unsupported, missing, and empty deletion-object behavior", async () => {
    const store = memoryStore();
    const context = new LanceReadContext(store, {}, emptyStats(), 0, () => 0, {
      coalesceGapBytes: 0,
      maxCoalescedRangeBytes: 1024,
    });
    const fragment = {
      id: 4n,
      files: [],
      physicalRows: 16,
    } satisfies LanceFragment;
    await expect(readDeletedRowOffsets(context, "root", fragment)).resolves.toEqual(new Set());
    await expect(
      readDeletedRowOffsets(context, "root", {
        ...fragment,
        deletionFile: { fileType: 1, readVersion: 2n, id: 3n, numDeletedRows: 1n },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_LANCE_FEATURE" });
    const sparse = {
      ...fragment,
      deletionFile: { fileType: 0, readVersion: 2n, id: 3n, numDeletedRows: 1n },
    };
    await expect(readDeletedRowOffsets(context, "root", sparse)).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });
    await store.put("root/_deletions/4-2-3.arrow", new Uint8Array());
    await expect(readDeletedRowOffsets(context, "root", sparse)).rejects.toMatchObject({
      code: "LAKEQL_LANCE_READ_ERROR",
    });
  });
});

function envelope(length: bigint, bytes: Uint8Array): Uint8Array {
  const output = new Uint8Array(8 + bytes.byteLength);
  new DataView(output.buffer).setBigInt64(0, length, true);
  output.set(bytes, 8);
  return output;
}

function emptyStats(): MutableLanceReadStats {
  return {
    snapshotMetadataBytes: 0,
    dataMetadataBytes: 0,
    logicalBytesRequested: 0,
    physicalBytesRequested: 0,
    rangeRequests: 0,
    cacheHits: 0,
    cacheMisses: 0,
    peakMemoryBytes: 0,
    rowsDecoded: 0,
    fragments: new Set(),
    pages: new Set(),
  };
}

function findSequence(haystack: Uint8Array, needle: Uint8Array): number {
  outer: for (let offset = 0; offset <= haystack.byteLength - needle.byteLength; offset += 1) {
    for (let index = 0; index < needle.byteLength; index += 1) {
      if (haystack[offset + index] !== needle[index]) continue outer;
    }
    return offset;
  }
  throw new Error("expected byte sequence not found");
}

function duplicateOneDeletionValue(bytes: Uint8Array): void {
  for (const values of [
    [2, 7, 13],
    [2, 13, 7],
    [7, 2, 13],
    [7, 13, 2],
    [13, 2, 7],
    [13, 7, 2],
  ]) {
    const encoded = new Uint8Array(new Uint32Array(values).buffer);
    try {
      const offset = findSequence(bytes, encoded);
      bytes.set(encoded.subarray(0, 4), offset + 8);
      return;
    } catch {
      // Try the next producer-defined deletion ordering.
    }
  }
  throw new Error("expected deletion values not found");
}
