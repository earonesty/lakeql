import { MATERIALIZATION_CHARGES as c, jsonStringUtf8Bytes } from "lakeql-core";

/** Preserve file identity field boundaries, including absent versus empty tags.
 * Versioned namespaces keep prior delimiter-joined entries cold until eviction. */
export function fileCacheKey(
  namespace: "scan-range-v2" | "owned-scan-range-v2" | "parquet-metadata-v2",
  path: string,
  byteLength: number,
  etag: string | undefined,
  start?: number,
  end?: number,
): string {
  return JSON.stringify([namespace, path, byteLength, etag ?? null, start ?? null, end ?? null]);
}

/** Bound both coalesced and exact-range key strings while an asynchronous read
 * can still reference either one. Includes its pending record and lookup handle.
 * Caller-supplied path/tag storage is borrowed; serialized copies are charged. */
export function pendingFileCacheKeyBytes(path: string, etag: string | undefined): number {
  const characters =
    64 + jsonStringUtf8Bytes(path) + (etag === undefined ? 4 : jsonStringUtf8Bytes(etag)) + 3 * 32;
  return (
    3 * c.object +
    12 * c.property +
    c.array +
    6 * c.arraySlot +
    2 * (c.string + characters * c.stringCodeUnit)
  );
}
