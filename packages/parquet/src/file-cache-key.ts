import {
  MATERIALIZATION_CHARGES as c,
  jsonNumberCharacters,
  jsonStringUtf8Bytes,
} from "lakeql-core";

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

/** Workspace for a structured metadata identity and its optional owned prefix.
 * String sizing is allocation-free; 32 characters bounds JSON numeric output. */
export function metadataFileCacheKeyBytes(
  path: string,
  etag: string | undefined,
  owned: boolean,
): number {
  const characters =
    7 +
    jsonStringUtf8Bytes("parquet-metadata-v2") +
    jsonStringUtf8Bytes(path) +
    32 +
    (etag === undefined ? 4 : jsonStringUtf8Bytes(etag)) +
    4 +
    4;
  return (
    c.array +
    6 * c.arraySlot +
    c.string +
    characters * c.stringCodeUnit +
    (owned ? c.string + ("owned:".length + characters) * c.stringCodeUnit : 0)
  );
}

/** Allocation-free character bound for a structured borrowed range identity. */
export function borrowedRangeKeyCharacters(
  path: string,
  byteLength: number,
  etag: string | undefined,
  start: number,
  end: number,
): number {
  const characters =
    7 +
    jsonStringUtf8Bytes("scan-range-v2") +
    jsonStringUtf8Bytes(path) +
    jsonNumberCharacters(byteLength) +
    (etag === undefined ? 4 : jsonStringUtf8Bytes(etag)) +
    jsonNumberCharacters(start) +
    jsonNumberCharacters(end);
  return characters;
}

/** Serialized borrowed range identity plus its pending promise/map entry. */
export function borrowedRangeKeyBytes(
  path: string,
  byteLength: number,
  etag: string | undefined,
  start: number,
  end: number,
): number {
  const characters = borrowedRangeKeyCharacters(path, byteLength, etag, start, end);
  return (
    c.array +
    6 * c.arraySlot +
    c.string +
    characters * c.stringCodeUnit +
    2 * c.object +
    2 * c.property
  );
}
