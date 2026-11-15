import type {
  Batch,
  CachePolicy,
  MemoryBudget,
  MemoryScope,
  ObjectStoreCacheOptions,
  OwnedMemory,
  SharedMemoryCache,
  Vector,
} from "lakeql-core";
import {
  MATERIALIZATION_CHARGES as c,
  isLakeqlError,
  jsonNumberCharacters,
  jsonStringUtf8Bytes,
  retainedBatchBytes,
  retainedVectorBytes,
} from "lakeql-core";

export class DecodedColumnCache {
  constructor(
    private readonly cache: SharedMemoryCache,
    private readonly options: ObjectStoreCacheOptions,
  ) {}

  acquireOwned(key: DecodedCacheKey, budget?: MemoryBudget): OwnedMemory<Batch> | undefined {
    return this.acquireKey<Batch>("decoded-column:", key, budget);
  }

  setOwned(key: DecodedCacheKey, owner: OwnedMemory<Batch>): boolean {
    return this.retainKey("decoded-column:", key, owner);
  }

  acquireVectorOwned(key: DecodedCacheKey, budget?: MemoryBudget): OwnedMemory<Vector> | undefined {
    return this.acquireKey<Vector>("decoded-vector:", key, budget);
  }

  setVectorOwned(key: DecodedCacheKey, owner: OwnedMemory<Vector>): boolean {
    if (!this.shouldAdmitDecodedWorkProduct(owner.bytes)) return false;
    return this.retainKey("decoded-vector:", key, owner);
  }

  acquireValueOwned<T>(key: DecodedCacheKey, budget?: MemoryBudget): OwnedMemory<T> | undefined {
    return this.acquireKey<T>("decoded-value:", key, budget);
  }

  setValueOwned<T>(key: DecodedCacheKey, owner: OwnedMemory<T>): boolean {
    if (!this.shouldAdmitDecodedWorkProduct(owner.bytes)) return false;
    return this.retainKey("decoded-value:", key, owner);
  }

  get(key: DecodedCacheKey, scope?: MemoryScope): Batch | undefined {
    if (scope !== undefined) {
      const owner = optionalCachePin(() => this.acquireOwned(key, scope.budget));
      return adoptCachedOwner(scope, owner);
    }
    return this.withKey(
      "decoded-column:",
      key,
      (qualified) => this.cache.get<Batch>(qualified)?.value,
    );
  }

  set(key: DecodedCacheKey, batch: Batch, scope?: MemoryScope): void {
    if (scope !== undefined) {
      const owner = scope.retainAllocations(batch);
      try {
        this.setOwned(key, owner);
      } finally {
        owner.release();
      }
      return;
    }
    this.withKey("decoded-column:", key, (qualified) =>
      this.cache.set(qualified, batch, retainedBatchBytes(batch), {
        priority: decodedPriority(this.options.policy ?? "balanced"),
      }),
    );
  }

  getVector(key: DecodedCacheKey, scope?: MemoryScope): Vector | undefined {
    if (scope !== undefined) {
      const owner = optionalCachePin(() => this.acquireVectorOwned(key, scope.budget));
      return adoptCachedOwner(scope, owner);
    }
    return this.withKey(
      "decoded-vector:",
      key,
      (qualified) => this.cache.get<Vector>(qualified)?.value,
    );
  }

  setVector(key: DecodedCacheKey, vector: Vector, scope?: MemoryScope): boolean {
    if (scope !== undefined) {
      const owner = scope.retainAllocations(vector);
      try {
        return this.setVectorOwned(key, owner);
      } finally {
        owner.release();
      }
    }
    const bytes = retainedVectorBytes(vector);
    if (!this.shouldAdmitDecodedVector(bytes)) return false;
    return (
      this.withKey("decoded-vector:", key, (qualified) =>
        this.cache.setAllocated(qualified, bytes, () => vector, {
          priority: decodedPriority(this.options.policy ?? "balanced"),
        }),
      ) ?? false
    );
  }

  getValue<T>(key: DecodedCacheKey, scope?: MemoryScope): T | undefined {
    if (scope !== undefined) {
      const owner = optionalCachePin(() => this.acquireValueOwned<T>(key, scope.budget));
      return adoptCachedOwner(scope, owner);
    }
    return this.withKey("decoded-value:", key, (qualified) => this.cache.get<T>(qualified)?.value);
  }

  setValue<T>(key: DecodedCacheKey, value: T, bytes: number, scope?: MemoryScope): void {
    if (scope !== undefined) {
      const owner = scope.retainAllocations(value);
      try {
        this.setValueOwned(key, owner);
      } finally {
        owner.release();
      }
      return;
    }
    if (!this.shouldAdmitDecodedWorkProduct(bytes)) return;
    this.withKey("decoded-value:", key, (qualified) =>
      this.cache.set(qualified, value, bytes, {
        priority: decodedPriority(this.options.policy ?? "balanced"),
      }),
    );
  }

  private acquireKey<T>(
    prefix: string,
    key: DecodedCacheKey,
    budget?: MemoryBudget,
  ): OwnedMemory<T> | undefined {
    return this.withKey(
      prefix,
      key,
      (qualified) => this.cache.acquireOwned<T>(qualified, budget),
      budget,
    );
  }

  private retainKey<T>(prefix: string, key: DecodedCacheKey, owner: OwnedMemory<T>): boolean {
    return (
      this.withKey(
        prefix,
        key,
        (qualified) =>
          this.cache.setOwned(qualified, owner, {
            priority: decodedPriority(this.options.policy ?? "balanced"),
          }),
        owner.budget,
      ) ?? false
    );
  }

  private withKey<T>(
    prefix: string,
    key: DecodedCacheKey,
    use: (key: string) => T,
    budget?: MemoryBudget,
  ): T | undefined {
    const characters = typeof key === "string" ? key.length : key.characters;
    const bytes =
      c.string +
      (prefix.length + characters) * c.stringCodeUnit +
      (typeof key === "string" ? 0 : key.workspaceBytes);
    return this.cache.withOptionalKey(
      bytes,
      () => prefix + (typeof key === "string" ? key : key.build()),
      use,
      budget,
    );
  }

  private shouldAdmitDecodedVector(bytes: number): boolean {
    return this.shouldAdmitDecodedWorkProduct(bytes);
  }

  private shouldAdmitDecodedWorkProduct(bytes: number): boolean {
    const maxBytes = this.options.maxBytes ?? 64 * 1024 * 1024;
    const policy = this.options.policy ?? "balanced";
    if (policy === "latency") return bytes <= maxBytes;
    // Bound each decoded entry by a fraction of configured capacity, including
    // small Worker caches. Shared cache priority/eviction and pool admission decide
    // whether it fits current pressure; an absolute capacity cliff defeats reuse.
    return bytes <= Math.floor(maxBytes / 8);
  }
}

export function decodedColumnCacheKey(options: {
  path: DecodedCacheKey;
  byteLength: number;
  etag?: string;
  columns: readonly string[];
  rowStart: number;
  rowEnd: number;
}): string {
  return JSON.stringify([
    "column-v2",
    buildDecodedCacheKey(options.path),
    options.byteLength,
    options.etag ?? null,
    options.rowStart,
    options.rowEnd,
    options.columns,
  ]);
}

export function decodedColumnPageCacheKey(options: {
  path: DecodedCacheKey;
  byteLength: number;
  etag?: string;
  column: string;
  rowGroupStart: number;
  pageRowStart: number;
  pageRowEnd: number;
  pageOffset: number;
  compressedPageSize: number;
}): string {
  return JSON.stringify([
    "page-v2",
    buildDecodedCacheKey(options.path),
    options.byteLength,
    options.etag ?? null,
    options.column,
    options.rowGroupStart,
    options.pageRowStart,
    options.pageRowEnd,
    options.pageOffset,
    options.compressedPageSize,
  ]);
}

export function decodedDictionaryPageCacheKey(options: {
  path: DecodedCacheKey;
  byteLength: number;
  etag?: string;
  column: string;
  rowGroupStart: number;
  pageOffset: number;
  compressedPageSize: number;
  values: number;
}): string {
  return JSON.stringify([
    "dictionary-v2",
    buildDecodedCacheKey(options.path),
    options.byteLength,
    options.etag ?? null,
    options.column,
    options.rowGroupStart,
    options.pageOffset,
    options.compressedPageSize,
    options.values,
  ]);
}

/** A borrowed immutable identity plan; variable-sized serialization happens only
 * inside an admitted, synchronous cache operation, never across page decoding. */
export interface DecodedCacheKeyPlan {
  readonly characters: number;
  readonly jsonCharacters: number;
  readonly workspaceBytes: number;
  readonly build: () => string;
}
export type DecodedCacheKey = string | DecodedCacheKeyPlan;

export function decodedCacheKeyCharacters(key: DecodedCacheKey): number {
  return typeof key === "string" ? key.length : key.characters;
}

/** UTF8 escaped length bounds UTF16 code units without serializing the string. */
export function decodedCacheKeyJsonCharacters(key: DecodedCacheKey): number {
  return typeof key === "string" ? jsonStringUtf8Bytes(key) : key.jsonCharacters;
}

export function decodedCacheNumberCharacters(value: number | undefined): number {
  return jsonNumberCharacters(value);
}

export function decodedCacheKeyWorkspaceBytes(key: DecodedCacheKey): number {
  return typeof key === "string" ? 0 : key.workspaceBytes;
}

export function buildDecodedCacheKey(key: DecodedCacheKey): string {
  return typeof key === "string" ? key : key.build();
}

/** Compose immutable identity parts without constructing a variable-sized string. */
export function prefixDecodedCacheKey(prefix: string, key: DecodedCacheKey): DecodedCacheKeyPlan {
  const characters = prefix.length + decodedCacheKeyCharacters(key);
  return {
    characters,
    jsonCharacters: jsonStringUtf8Bytes(prefix) + decodedCacheKeyJsonCharacters(key) - 2,
    workspaceBytes: c.string + characters * c.stringCodeUnit + decodedCacheKeyWorkspaceBytes(key),
    build: () => prefix + buildDecodedCacheKey(key),
  };
}

export function decodedCacheKeyPlan(
  characters: number,
  build: () => string,
  extraWorkspaceBytes = 0,
): DecodedCacheKeyPlan {
  return {
    characters,
    jsonCharacters: 6 * characters + 2,
    workspaceBytes:
      c.array + 10 * c.arraySlot + c.string + characters * c.stringCodeUnit + extraWorkspaceBytes,
    build,
  };
}

export function planDecodedColumnCacheKey(
  options: Parameters<typeof decodedColumnCacheKey>[0],
): DecodedCacheKeyPlan {
  let columns = options.columns.length === 0 ? 2 : options.columns.length + 1;
  for (const column of options.columns) columns += jsonStringUtf8Bytes(column);
  const characters =
    19 +
    decodedCacheKeyJsonCharacters(options.path) +
    decodedCacheNumberCharacters(options.byteLength) +
    (options.etag === undefined ? 4 : jsonStringUtf8Bytes(options.etag)) +
    decodedCacheNumberCharacters(options.rowStart) +
    decodedCacheNumberCharacters(options.rowEnd) +
    columns;
  return decodedCacheKeyPlan(
    characters,
    () => decodedColumnCacheKey(options),
    decodedCacheKeyWorkspaceBytes(options.path),
  );
}

export function planDecodedColumnPageCacheKey(
  options: Parameters<typeof decodedColumnPageCacheKey>[0],
): DecodedCacheKeyPlan {
  const characters =
    20 +
    decodedCacheKeyJsonCharacters(options.path) +
    decodedCacheNumberCharacters(options.byteLength) +
    (options.etag === undefined ? 4 : jsonStringUtf8Bytes(options.etag)) +
    jsonStringUtf8Bytes(options.column) +
    decodedCacheNumberCharacters(options.rowGroupStart) +
    decodedCacheNumberCharacters(options.pageRowStart) +
    decodedCacheNumberCharacters(options.pageRowEnd) +
    decodedCacheNumberCharacters(options.pageOffset) +
    decodedCacheNumberCharacters(options.compressedPageSize);
  return decodedCacheKeyPlan(
    characters,
    () => decodedColumnPageCacheKey(options),
    decodedCacheKeyWorkspaceBytes(options.path),
  );
}

export function planDecodedDictionaryPageCacheKey(
  options: Parameters<typeof decodedDictionaryPageCacheKey>[0],
): DecodedCacheKeyPlan {
  const characters =
    25 +
    decodedCacheKeyJsonCharacters(options.path) +
    decodedCacheNumberCharacters(options.byteLength) +
    (options.etag === undefined ? 4 : jsonStringUtf8Bytes(options.etag)) +
    jsonStringUtf8Bytes(options.column) +
    decodedCacheNumberCharacters(options.rowGroupStart) +
    decodedCacheNumberCharacters(options.pageOffset) +
    decodedCacheNumberCharacters(options.compressedPageSize) +
    decodedCacheNumberCharacters(options.values);
  return decodedCacheKeyPlan(
    characters,
    () => decodedDictionaryPageCacheKey(options),
    decodedCacheKeyWorkspaceBytes(options.path),
  );
}

function decodedPriority(policy: CachePolicy): number {
  if (policy === "latency") return 4;
  if (policy === "io") return 1;
  return 2;
}

function adoptCachedOwner<T>(scope: MemoryScope, owner: OwnedMemory<T> | undefined): T | undefined {
  if (owner === undefined) return undefined;
  try {
    return scope.adopt(owner).value;
  } catch (error) {
    owner.release();
    throw error;
  }
}

/** A conservative cached backing can exceed the working set of a fresh decode. */
function optionalCachePin<T>(
  acquire: () => OwnedMemory<T> | undefined,
): OwnedMemory<T> | undefined {
  try {
    return acquire();
  } catch (error) {
    if (
      isLakeqlError(error) &&
      error.code === "LAKEQL_BUDGET_EXCEEDED" &&
      error.details.metric === "allocated memory bytes"
    )
      return undefined;
    throw error;
  }
}
