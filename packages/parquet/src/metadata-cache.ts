import {
  type CacheAdapter,
  type CacheEntry,
  isLakeqlError,
  LakeqlError,
  type MemoryBudget,
  type MemoryScope,
  type OwnedMemory,
  type SharedMemoryCache,
  throwIfAborted,
} from "lakeql-core";
import { inspectCompactStructure, readCompactHeader } from "./compact-header.js";
import { convertDecimalValues, isDecimal, isIntegerDecimal } from "./decimal.js";
import { incrementalParquetMetadata } from "./incremental-metadata.js";
import { metadataTag, parquetMetadataCacheBytes } from "./metadata-size.js";
import { decodeOwnedFullMetadata } from "./owned-full-metadata.js";
import { ParquetReadScope } from "./read-scope.js";
import { schemaLeafIndex } from "./schema-leaves.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";

const metadataInitialFetchSize = 64 * 1024;

export function readParquetMetadataFromFile(file: StoreAsyncBuffer): Promise<ParquetMetadata> {
  return readFooterMetadata(file);
}

/** Avoid an uncharged concatenation when metadata exceeds the initial range. */
async function readFooterMetadata(
  file: StoreAsyncBuffer,
  scope?: MemoryScope,
): Promise<ParquetMetadata> {
  if (scope === undefined) {
    const footer = await readParquetFooterBuffer(file);
    return normalizeDecimalStatistics(incrementalParquetMetadata(footer), footer);
  }
  let initialBytes = metadataInitialFetchSize;
  for (;;) {
    const acquisition = scope.childScope();
    let footer: ArrayBuffer | undefined;
    try {
      footer = await readParquetFooterBuffer(file, acquisition, initialBytes);
      const metadata = decodeOwnedFullMetadata(footer, acquisition);
      scope.adopt(acquisition.retainAllocations(undefined));
      return metadata;
    } catch (error) {
      const length =
        footer === undefined ? 0 : new DataView(footer).getUint32(footer.byteLength - 8, true) + 8;
      if (
        !isLakeqlError(error) ||
        error.code !== "LAKEQL_BUDGET_EXCEEDED" ||
        footer === undefined ||
        footer.byteLength <= length
      )
        throw error;
      // Release speculative prefix backing before one exact-footer retry.
      initialBytes = length;
    } finally {
      acquisition.dispose();
    }
  }
}

/** Shared acquisition/preflight; a trailer probe permits exact retained footer backing. */
export async function readParquetFooterBuffer(
  file: StoreAsyncBuffer,
  scope?: MemoryScope,
  initialBytes = metadataInitialFetchSize,
): Promise<ArrayBuffer> {
  if (scope !== undefined && !file.sliceOwned)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing owned footer reads");
  if (!Number.isSafeInteger(file.byteLength) || file.byteLength < 8)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet file length");
  const readCost = (start: number, end: number) => {
    const bytes = file.ownedReadAllocationBytes?.(start, end) ?? end - start;
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid owned footer read allocation cost");
    return bytes;
  };
  let fetchSize = Math.min(
    file.byteLength,
    Math.max(8, Math.min(initialBytes, scope?.availableAllocationBytes ?? initialBytes)),
  );
  // Adapter workspace/copies can exceed the output length. Geometric shrinking
  // needs no monotonic cost assumption and performs logarithmically many probes.
  while (
    scope !== undefined &&
    fetchSize > 8 &&
    readCost(file.byteLength - fetchSize, file.byteLength) > scope.availableAllocationBytes
  ) {
    fetchSize = Math.max(8, Math.floor(fetchSize / 2));
  }
  const offset = Math.max(0, file.byteLength - fetchSize);
  const read = async (start: number, end: number) => {
    if (scope !== undefined && file.sliceOwned) return file.sliceOwned(start, end, scope);
    return { value: await file.slice(start, end), release() {} };
  };
  const initial = await read(offset, file.byteLength);
  const view = new DataView(initial.value);
  if (view.byteLength < 8 || view.getUint32(view.byteLength - 4, true) !== 0x31524150)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet footer magic");
  const length = view.getUint32(view.byteLength - 8, true) + 8;
  if (length > file.byteLength)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Parquet footer exceeds file length");
  let footer = initial.value;
  if (length > initial.value.byteLength) {
    const prefixBytes = length - initial.value.byteLength;
    if (
      scope === undefined ||
      readCost(file.byteLength - length, offset) + length <= scope.availableAllocationBytes
    ) {
      // Spare capacity buys one prefix read without retransmitting the suffix.
      const prefix = await read(file.byteLength - length, offset);
      try {
        const combine = () => {
          const combined = new Uint8Array(length);
          combined.set(new Uint8Array(prefix.value));
          combined.set(new Uint8Array(initial.value), prefixBytes);
          return combined.buffer;
        };
        footer =
          scope === undefined
            ? combine()
            : scope.allocate(length, "combined Parquet footer", combine).value;
      } finally {
        prefix.release();
        initial.release();
      }
    } else {
      // Tight pools trade a repeated suffix transfer for no concatenation peak.
      initial.release();
      footer = (await read(file.byteLength - length, file.byteLength)).value;
    }
  }
  // Validate container extents and recursion before the upstream object parser
  // can allocate from untrusted lengths. Only the declared metadata is visible.
  const metadataLength = length - 8;
  const preflight = {
    view: new DataView(footer, footer.byteLength - length, metadataLength),
    offset: 0,
  };
  inspectCompactStructure(preflight);
  if (preflight.offset !== metadataLength)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Trailing footer metadata bytes");
  return footer;
}

function normalizeDecimalStatistics(
  metadata: ParquetMetadata,
  footer?: ArrayBuffer,
): ParquetMetadata {
  if (!metadata.schema.some(isDecimal)) return metadata;
  const leaves = schemaLeafIndex(metadata.schema);
  const normalize = (
    groupIndex: number,
    columnIndex: number,
    rawBounds?: Record<string, Uint8Array>,
  ) => {
    const column = metadata.row_groups[groupIndex]?.columns[columnIndex];
    if (column === undefined) return;
    const columnMetadata = column.meta_data;
    const statistics = columnMetadata?.statistics;
    if (columnMetadata === undefined || statistics === undefined) return;
    const element = leaves.get(JSON.stringify(columnMetadata.path_in_schema));
    if (element === undefined || !isDecimal(element)) return;
    // Upstream metadata conversion irreversibly scales fixed-byte decimals
    // and decodes variable-byte decimals as strings. Neither is an exact raw
    // bound that can be normalized to our value conversion contract. Preserve
    // counts, but do not allow these lossy bounds to exclude matching rows.
    for (const [key, field] of [
      ["min", 2],
      ["max", 1],
      ["min_value", 6],
      ["max_value", 5],
    ] as const) {
      const value = isIntegerDecimal(element) ? statistics[key] : rawBounds?.[`field_${field}`];
      if (value === undefined) {
        delete statistics[key];
        continue;
      }
      try {
        const converted = convertDecimalValues(
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
  };
  for (const [groupIndex, group] of metadata.row_groups.entries()) {
    for (let columnIndex = 0; columnIndex < group.columns.length; columnIndex++)
      normalize(groupIndex, columnIndex);
  }
  if (
    footer !== undefined &&
    metadata.schema.some((element) => isDecimal(element) && !isIntegerDecimal(element))
  ) {
    const view = new DataView(footer);
    const length = view.getUint32(view.byteLength - 8, true);
    const reader = { view: new DataView(footer, view.byteLength - 8 - length, length), offset: 0 };
    readCompactHeader(reader, {
      4: {
        list: (groupIndex) => ({
          1: {
            list: (columnIndex) => {
              const column = metadata.row_groups[groupIndex]?.columns[columnIndex]?.meta_data;
              if (column === undefined) return false;
              const element = leaves.get(JSON.stringify(column.path_in_schema));
              return element !== undefined && isDecimal(element) && !isIntegerDecimal(element)
                ? { 3: { 12: { 1: "binary", 2: "binary", 5: "binary", 6: "binary" } } }
                : false;
            },
            visit: (value, columnIndex) => {
              if (typeof value !== "object" || value === null) return;
              const bounds = (value as { field_3?: { field_12?: Record<string, Uint8Array> } })
                .field_3?.field_12;
              normalize(groupIndex, columnIndex, bounds);
            },
          },
        }),
        visit: () => {},
      },
    });
    if (reader.offset !== length)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Trailing footer metadata bytes");
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

export interface OwnedMetadataCache extends CacheAdapter<ParquetMetadata> {
  acquireOwned?(key: string, budget: MemoryBudget): OwnedMemory<ParquetMetadata> | undefined;
  setOwned?(key: string, owner: OwnedMemory<ParquetMetadata>): boolean;
}

/** Own footer backing and admitted decoded metadata for the complete consumer lifetime. */
export async function readOwnedParquetMetadata(
  path: string,
  file: StoreAsyncBuffer,
  budget: MemoryBudget,
  cache: CacheAdapter<ParquetMetadata> | undefined,
  signals: readonly AbortSignal[] = [],
): Promise<{ owner: OwnedMemory<ParquetMetadata>; cached: boolean }> {
  for (const signal of signals) throwIfAborted(signal);
  const ownedCache = cache as OwnedMetadataCache | undefined;
  const key = `owned:${metadataCacheKey(path, file.byteLength, file.etag)}`;
  try {
    const cached = ownedCache?.acquireOwned?.(key, budget);
    if (cached !== undefined) return { owner: cached, cached: true };
  } catch (error) {
    if (
      !isLakeqlError(error) ||
      error.code !== "LAKEQL_BUDGET_EXCEEDED" ||
      error.details.metric !== "allocated memory bytes"
    )
      throw error;
  }
  const stage = new ParquetReadScope(file, budget, signals);
  try {
    const metadata = await readFooterMetadata(stage.file, stage.allocations);
    const owner = stage.retainInputBacking(metadata);
    try {
      ownedCache?.setOwned?.(key, owner);
      return { owner, cached: false };
    } catch (error) {
      owner.release();
      throw error;
    }
  } finally {
    stage.dispose();
  }
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
export function sharedParquetMetadataCache(cache: SharedMemoryCache): OwnedMetadataCache {
  return {
    acquireOwned(key, budget) {
      return cache.acquireOwned<ParquetMetadata>(key, budget);
    },
    setOwned(key, owner) {
      return cache.setOwned(key, owner, {
        priority: 4,
        cacheBytes: owner.bytes + parquetMetadataCacheBytes(owner.value),
      });
    },
    async get(key) {
      const cached = cache.get<ParquetMetadata>(key);
      return cached === undefined ? undefined : { value: cached.value };
    },
    async set(key, entry) {
      const bytes = parquetMetadataCacheBytes(entry.value);
      cache.set(key, entry.value, bytes, { priority: 4 });
    },
    async delete(key) {
      cache.delete(key);
    },
  };
}

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
