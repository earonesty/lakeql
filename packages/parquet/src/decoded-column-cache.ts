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
import { isLakeqlError, retainedBatchBytes, retainedVectorBytes } from "lakeql-core";

export class DecodedColumnCache {
  constructor(
    private readonly cache: SharedMemoryCache,
    private readonly options: ObjectStoreCacheOptions,
  ) {}

  acquireOwned(key: string, budget?: MemoryBudget): OwnedMemory<Batch> | undefined {
    return this.cache.acquireOwned<Batch>(decodedKey(key), budget);
  }

  setOwned(key: string, owner: OwnedMemory<Batch>): boolean {
    return this.cache.setOwned(decodedKey(key), owner, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  acquireVectorOwned(key: string, budget?: MemoryBudget): OwnedMemory<Vector> | undefined {
    return this.cache.acquireOwned<Vector>(decodedVectorKey(key), budget);
  }

  setVectorOwned(key: string, owner: OwnedMemory<Vector>): boolean {
    if (!this.shouldAdmitDecodedWorkProduct(owner.bytes)) return false;
    return this.cache.setOwned(decodedVectorKey(key), owner, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  acquireValueOwned<T>(key: string, budget?: MemoryBudget): OwnedMemory<T> | undefined {
    return this.cache.acquireOwned<T>(decodedValueKey(key), budget);
  }

  setValueOwned<T>(key: string, owner: OwnedMemory<T>): boolean {
    if (!this.shouldAdmitDecodedWorkProduct(owner.bytes)) return false;
    return this.cache.setOwned(decodedValueKey(key), owner, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  get(key: string, scope?: MemoryScope): Batch | undefined {
    if (scope !== undefined) {
      const owner = optionalCachePin(() => this.acquireOwned(key, scope.budget));
      return adoptCachedOwner(scope, owner);
    }
    const entry = this.cache.get<Batch>(decodedKey(key));
    if (entry === undefined) return undefined;
    return entry.value;
  }

  set(key: string, batch: Batch, scope?: MemoryScope): void {
    if (scope !== undefined) {
      const owner = scope.retainAllocations(batch);
      try {
        this.setOwned(key, owner);
      } finally {
        owner.release();
      }
      return;
    }
    this.cache.set(decodedKey(key), batch, retainedBatchBytes(batch), {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  getVector(key: string, scope?: MemoryScope): Vector | undefined {
    if (scope !== undefined) {
      const owner = optionalCachePin(() => this.acquireVectorOwned(key, scope.budget));
      return adoptCachedOwner(scope, owner);
    }
    const entry = this.cache.get<Vector>(decodedVectorKey(key));
    if (entry === undefined) return undefined;
    return entry.value;
  }

  setVector(key: string, vector: Vector, scope?: MemoryScope): boolean {
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
    return this.cache.setAllocated(decodedVectorKey(key), bytes, () => vector, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
  }

  getValue<T>(key: string, scope?: MemoryScope): T | undefined {
    if (scope !== undefined) {
      const owner = optionalCachePin(() => this.acquireValueOwned<T>(key, scope.budget));
      return adoptCachedOwner(scope, owner);
    }
    const entry = this.cache.get<T>(decodedValueKey(key));
    if (entry === undefined) return undefined;
    return entry.value;
  }

  setValue<T>(key: string, value: T, bytes: number, scope?: MemoryScope): void {
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
    this.cache.set(decodedValueKey(key), value, bytes, {
      priority: decodedPriority(this.options.policy ?? "balanced"),
    });
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
  path: string;
  byteLength: number;
  etag?: string;
  columns: readonly string[];
  rowStart: number;
  rowEnd: number;
}): string {
  return [
    options.path,
    options.byteLength,
    options.etag ?? "",
    options.rowStart,
    options.rowEnd,
    ...options.columns,
  ].join("\u001f");
}

export function decodedColumnPageCacheKey(options: {
  path: string;
  byteLength: number;
  etag?: string;
  column: string;
  rowGroupStart: number;
  pageRowStart: number;
  pageRowEnd: number;
  pageOffset: number;
  compressedPageSize: number;
}): string {
  return [
    options.path,
    options.byteLength,
    options.etag ?? "",
    options.column,
    options.rowGroupStart,
    options.pageRowStart,
    options.pageRowEnd,
    options.pageOffset,
    options.compressedPageSize,
  ].join("\u001f");
}

export function decodedDictionaryPageCacheKey(options: {
  path: string;
  byteLength: number;
  etag?: string;
  column: string;
  rowGroupStart: number;
  pageOffset: number;
  compressedPageSize: number;
  values: number;
}): string {
  return [
    options.path,
    options.byteLength,
    options.etag ?? "",
    options.column,
    options.rowGroupStart,
    options.pageOffset,
    options.compressedPageSize,
    options.values,
  ].join("\u001f");
}

function decodedKey(key: string): string {
  return `decoded-column:${key}`;
}

function decodedVectorKey(key: string): string {
  return `decoded-vector:${key}`;
}

function decodedValueKey(key: string): string {
  return `decoded-value:${key}`;
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
