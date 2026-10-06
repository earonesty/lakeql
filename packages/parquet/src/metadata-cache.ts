import { parquetMetadataAsync } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { CacheAdapter, CacheEntry, SharedMemoryCache } from "lakeql-core";
import { convertIntegerDecimalValues, isIntegerDecimal } from "./decimal.js";
import { lakeqlParquetParsers } from "./parsers.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";

const metadataInitialFetchSize = 64 * 1024;

export function readParquetMetadataFromFile(file: StoreAsyncBuffer): Promise<ParquetMetadata> {
  return parquetMetadataAsync(file, {
    initialFetchSize: metadataInitialFetchSize,
    parsers: lakeqlParquetParsers,
  }).then(normalizeIntegerDecimalStatistics);
}

function normalizeIntegerDecimalStatistics(metadata: ParquetMetadata): ParquetMetadata {
  for (const rowGroup of metadata.row_groups) {
    for (const column of rowGroup.columns) {
      const columnMetadata = column.meta_data;
      const statistics = columnMetadata?.statistics;
      if (columnMetadata === undefined || statistics === undefined) continue;
      const element = getSchemaPath(metadata.schema, columnMetadata.path_in_schema).at(-1)?.element;
      if (element === undefined || !isIntegerDecimal(element)) continue;
      for (const key of ["min", "max", "min_value", "max_value"] as const) {
        const value = statistics[key];
        if (value === undefined) continue;
        try {
          const converted = convertIntegerDecimalValues(
            [value],
            element,
            columnMetadata.path_in_schema.join("."),
          )[0];
          if (converted !== undefined) statistics[key] = converted;
        } catch {
          // Unsafe statistics cannot be used for pruning. Value decoding still
          // emits the typed precision error if the column is selected.
          delete statistics[key];
        }
      }
    }
  }
  return metadata;
}

export async function readCachedParquetMetadata(
  path: string,
  file: StoreAsyncBuffer,
  metadataCache: CacheAdapter<ParquetMetadata> | undefined,
): Promise<{ metadata: ParquetMetadata; cached: boolean }> {
  if (!metadataCache) return { metadata: await readParquetMetadataFromFile(file), cached: false };
  const key = metadataCacheKey(path, file.byteLength, file.etag);
  const cached = await metadataCache.get(key);
  if (cached) return { metadata: cached.value, cached: true };
  const metadata = await readParquetMetadataFromFile(file);
  await metadataCache.set(key, { value: metadata });
  return { metadata, cached: false };
}

function metadataCacheKey(path: string, byteLength: number, etag: string | undefined): string {
  return `parquet-metadata:${path}:${byteLength}:${etag ?? "no-etag"}`;
}

/**
 * Adapt a persistent byte cache to parsed Parquet metadata. The tagged encoding
 * preserves Thrift bigint and byte values that ordinary JSON caches cannot.
 */
export function encodedParquetMetadataCache(
  cache: CacheAdapter<Uint8Array>,
): CacheAdapter<ParquetMetadata> {
  return {
    async get(key) {
      const entry = await cache.get(key);
      if (entry === undefined) return undefined;
      const decoded: CacheEntry<ParquetMetadata> = {
        value: decodeParquetMetadata(entry.value),
      };
      if (entry.expiresAt !== undefined) decoded.expiresAt = entry.expiresAt;
      return decoded;
    },
    async set(key, entry) {
      const encoded: CacheEntry<Uint8Array> = {
        value: encodeParquetMetadata(entry.value),
      };
      if (entry.expiresAt !== undefined) encoded.expiresAt = entry.expiresAt;
      await cache.set(key, encoded);
    },
    delete(key) {
      return cache.delete(key);
    },
  };
}

/** Keep parsed metadata in an existing bounded shared-memory cache. */
export function sharedParquetMetadataCache(
  cache: SharedMemoryCache,
): CacheAdapter<ParquetMetadata> {
  return {
    async get(key) {
      const cached = cache.get<ParquetMetadata>(key);
      return cached === undefined ? undefined : { value: cached.value };
    },
    async set(key, entry) {
      const bytes = encodeParquetMetadata(entry.value).byteLength;
      cache.set(key, entry.value, bytes, { priority: 4 });
    },
    async delete(key) {
      cache.delete(key);
    },
  };
}

const metadataTag = "__lakeql_parquet_metadata_type";

function encodeParquetMetadata(metadata: ParquetMetadata): Uint8Array {
  const json = JSON.stringify(metadata, (_key, value: unknown) => {
    if (typeof value === "bigint") return { [metadataTag]: "bigint", value: value.toString() };
    if (value instanceof Uint8Array) return { [metadataTag]: "bytes", value: bytesToHex(value) };
    if (value instanceof ArrayBuffer) {
      return { [metadataTag]: "bytes", value: bytesToHex(new Uint8Array(value)) };
    }
    return value;
  });
  return new TextEncoder().encode(json);
}

function decodeParquetMetadata(bytes: Uint8Array): ParquetMetadata {
  return JSON.parse(new TextDecoder().decode(bytes), (_key, value: unknown) => {
    if (!isEncodedMetadataValue(value)) return value;
    if (value[metadataTag] === "bigint") return BigInt(value.value);
    return hexToBytes(value.value);
  }) as ParquetMetadata;
}

function isEncodedMetadataValue(
  value: unknown,
): value is Record<typeof metadataTag, "bigint" | "bytes"> & { value: string } {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return (
    (record[metadataTag] === "bigint" || record[metadataTag] === "bytes") &&
    typeof record.value === "string"
  );
}

function bytesToHex(bytes: Uint8Array): string {
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return hex;
}

function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/u.test(hex)) {
    throw new Error("Invalid encoded Parquet metadata bytes");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
