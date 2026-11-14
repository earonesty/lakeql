import {
  type CacheAdapter,
  type CacheEntry,
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type ListOptions,
  MemoryScope,
  type ObjectHead,
  type ObjectInfo,
  type ObjectStore,
  type OwnedMemory,
  type PutOptions,
  readOwnedStreamWindow,
  throwIfAborted,
} from "lakeql-core";

export const PACKAGE = "lakeql-r2" as const;

export interface D1DatabaseLike {
  prepare(query: string): D1PreparedStatementLike;
}

export interface D1PreparedStatementLike {
  bind(...values: unknown[]): D1PreparedStatementLike;
  first<T = unknown>(column?: string): Promise<T | null>;
  run(): Promise<unknown>;
}

export interface CloudflareD1JsonCacheOptions {
  db: D1DatabaseLike;
  table?: string;
  prefix?: string;
  ttlMs?: number;
  now?: () => number;
  createTable?: boolean;
}

export interface R2ObjectBody {
  key: string;
  size: number;
  etag?: string;
  uploaded?: Date;
  httpMetadata?: { contentType?: string };
  /** Streaming body required by allocation-aware range reads. */
  body?: ReadableStream<Uint8Array>;
  arrayBuffer(): Promise<ArrayBuffer>;
}

export interface R2BucketLike {
  get(
    key: string,
    options?: { range?: { offset: number; length: number } },
  ): Promise<R2ObjectBody | null>;
  head(key: string): Promise<Omit<R2ObjectBody, "arrayBuffer"> | null>;
  put(
    key: string,
    value: Uint8Array | ReadableStream<Uint8Array>,
    options?: { httpMetadata?: { contentType?: string }; customMetadata?: Record<string, string> },
  ): Promise<unknown>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string; limit?: number; cursor?: string }): Promise<{
    objects: Omit<R2ObjectBody, "arrayBuffer">[];
    truncated?: boolean;
    cursor?: string;
  }>;
}

export function r2Store(bucket: R2BucketLike): ObjectStore {
  return new R2ObjectStore(bucket);
}

export function cloudflareD1JsonCache<T = unknown>(
  options: CloudflareD1JsonCacheOptions,
): CacheAdapter<T> {
  return new CloudflareD1JsonCache<T>(options);
}

export class CloudflareD1JsonCache<T> implements CacheAdapter<T> {
  private readonly db: D1DatabaseLike;
  private readonly table: string;
  private readonly prefix: string;
  private readonly ttlMs: number | undefined;
  private readonly now: () => number;
  private readonly createTable: boolean;
  private ensurePromise: Promise<void> | undefined;

  constructor(options: CloudflareD1JsonCacheOptions) {
    this.db = options.db;
    this.table = validatedD1Identifier(options.table ?? "lakeql_cache");
    this.prefix = options.prefix ?? "lakeql";
    this.ttlMs = options.ttlMs;
    this.now = options.now ?? Date.now;
    this.createTable = options.createTable ?? true;
  }

  async get(key: string): Promise<CacheEntry<T> | undefined> {
    await this.ensureTable();
    const cacheKey = this.cacheKey(key);
    const row = await this.db
      .prepare(`select value, expires_at from ${this.table} where key = ?1`)
      .bind(cacheKey)
      .first<{ value: string; expires_at: number | null }>();
    if (row === null) return undefined;
    const expiresAt = row.expires_at ?? undefined;
    if (expiresAt !== undefined && expiresAt <= this.now()) {
      await this.delete(key);
      return undefined;
    }
    const value = JSON.parse(row.value) as T;
    const entry: CacheEntry<T> = { value };
    if (expiresAt !== undefined) entry.expiresAt = expiresAt;
    return entry;
  }

  async set(key: string, entry: CacheEntry<T>): Promise<void> {
    await this.ensureTable();
    const json = jsonCacheValue(entry.value, key);
    const expiresAt =
      entry.expiresAt ?? (this.ttlMs === undefined ? undefined : this.now() + this.ttlMs);
    await this.db
      .prepare(
        `insert into ${this.table} (key, value, expires_at, updated_at)
values (?1, ?2, ?3, ?4)
on conflict(key) do update set
  value = excluded.value,
  expires_at = excluded.expires_at,
  updated_at = excluded.updated_at`,
      )
      .bind(this.cacheKey(key), json, expiresAt ?? null, this.now())
      .run();
  }

  async delete(key: string): Promise<void> {
    await this.ensureTable();
    await this.db
      .prepare(`delete from ${this.table} where key = ?1`)
      .bind(this.cacheKey(key))
      .run();
  }

  private cacheKey(key: string): string {
    return `${this.prefix}:${key}`;
  }

  private async ensureTable(): Promise<void> {
    if (!this.createTable) return;
    if (this.ensurePromise === undefined) {
      this.ensurePromise = this.db
        .prepare(
          `create table if not exists ${this.table} (
  key text primary key,
  value text not null,
  expires_at integer,
  updated_at integer not null
)`,
        )
        .run()
        .then(() => undefined);
    }
    await this.ensurePromise;
  }
}

export class R2ObjectStore implements ObjectStore {
  constructor(private readonly bucket: R2BucketLike) {}

  async get(path: string): Promise<Uint8Array | null> {
    const object = await this.bucket.get(path);
    if (!object) return null;
    return new Uint8Array(await object.arrayBuffer());
  }

  async getRange(path: string, range: { offset: number; length: number }): Promise<Uint8Array> {
    const object = await this.bucket.get(path, { range });
    if (!object) throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
    return new Uint8Array(await object.arrayBuffer());
  }

  getRangeOwnedAllocation(_path: string, range: { offset: number; length: number }) {
    return { bytes: range.length, output: "exact-buffer" as const };
  }

  async getRangeOwned(
    path: string,
    range: { offset: number; length: number },
    scope: MemoryScope,
  ): Promise<OwnedMemory<Uint8Array>> {
    if (
      !Number.isSafeInteger(range.offset) ||
      range.offset < 0 ||
      !Number.isSafeInteger(range.length) ||
      range.length < 0 ||
      !Number.isSafeInteger(range.offset + range.length)
    ) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "R2 range must use nonnegative safe integers",
      );
    }
    throwIfAborted(scope.signal);
    if (range.length === 0) return scope.allocate(0, "empty R2 range", () => new Uint8Array());
    let reservation = scope.reserve(range.length, "R2 range response");
    try {
      const object = await this.bucket.get(path, { range });
      if (scope.disposed) {
        await object?.body?.cancel().catch(() => undefined);
        throwIfAborted(scope.signal);
      }
      if (!object)
        throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
      if (object.body === undefined) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Allocation-aware R2 reads require a streaming body",
        );
      }
      // R2 size describes the whole object, not the returned range.
      if (!Number.isSafeInteger(object.size) || object.size < 0) {
        await object.body.cancel().catch(() => undefined);
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid R2 object size");
      }
      const length = Math.min(range.length, Math.max(0, object.size - range.offset));
      if (length < reservation.bytes) {
        const [body, unused] = reservation.partition([length, reservation.bytes - length]);
        unused?.release();
        if (body === undefined)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing R2 body reservation");
        reservation = body;
      }
      return await readOwnedStreamWindow(object.body, scope, {
        offset: 0,
        metric: "R2 range bytes",
        length,
        maxBytes: length,
        initialReservation: reservation,
      });
    } finally {
      reservation.release();
    }
  }

  async put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void> {
    await this.bucket.put(path, body, r2PutOptions(options));
  }

  async putOwned(path: string, body: OwnedMemory<Uint8Array>, options?: PutOptions): Promise<void> {
    const scope = new MemoryScope(body.budget);
    try {
      const input = scope.adopt(body.retain()).value;
      const putOptions = scope.allocate(2 * c.object + 3 * c.property, "R2 write options", () =>
        r2PutOptions(options),
      ).value;
      // R2 accepts the view directly. Its internal upload buffering is external;
      // retain the supplied backing until the binding settles without copying it.
      await this.bucket.put(path, input, putOptions);
    } finally {
      scope.dispose();
    }
  }

  async delete(path: string): Promise<void> {
    await this.bucket.delete(path);
  }

  async *list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo> {
    let cursor: string | undefined;
    let emitted = 0;
    do {
      const listOptions: { prefix?: string; limit?: number; cursor?: string } = { prefix };
      if (options?.limit !== undefined) listOptions.limit = options.limit - emitted;
      if (cursor !== undefined) listOptions.cursor = cursor;
      const result = await this.bucket.list(listOptions);
      for (const object of result.objects) {
        if (options?.limit !== undefined && emitted >= options.limit) return;
        yield r2Info(object);
        emitted += 1;
      }
      cursor = result.truncated === true ? result.cursor : undefined;
    } while (cursor !== undefined && (options?.limit === undefined || emitted < options.limit));
  }

  async headOwned(path: string, scope: MemoryScope): Promise<OwnedMemory<ObjectHead> | null> {
    throwIfAborted(scope.signal);
    const object = await this.bucket.head(path);
    throwIfAborted(scope.signal);
    if (object === null) return null;
    const contentType = object.httpMetadata?.contentType;
    const charge =
      c.object +
      5 * c.property +
      (object.etag === undefined ? 0 : c.string + object.etag.length * c.stringCodeUnit) +
      (object.uploaded === undefined ? 0 : c.timestamp) +
      (contentType === undefined ? 0 : c.string + contentType.length * c.stringCodeUnit);
    return scope
      .allocate<ObjectHead>(charge, "R2 object metadata", () => ({
        size: object.size,
        rangeReadMode: "native",
        ...(object.etag === undefined ? {} : { etag: object.etag }),
        ...(object.uploaded === undefined
          ? {}
          : { lastModified: new Date(object.uploaded.getTime()) }),
        ...(contentType === undefined ? {} : { contentType }),
      }))
      .retain();
  }

  async head(path: string): Promise<ObjectHead | null> {
    const object = await this.bucket.head(path);
    if (!object) return null;
    const head: ObjectHead = { size: object.size };
    if (object.etag !== undefined) head.etag = object.etag;
    if (object.uploaded !== undefined) head.lastModified = object.uploaded;
    if (object.httpMetadata?.contentType !== undefined) {
      head.contentType = object.httpMetadata.contentType;
    }
    return head;
  }
}

function r2Info(object: Omit<R2ObjectBody, "arrayBuffer">): ObjectInfo {
  const info: ObjectInfo = { path: object.key, size: object.size };
  if (object.etag !== undefined) info.etag = object.etag;
  if (object.uploaded !== undefined) info.lastModified = object.uploaded;
  return info;
}

function validatedD1Identifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "D1 cache table name is invalid", {
      table: value,
    });
  }
  return value;
}

function jsonCacheValue(value: unknown, key: string): string {
  if (value === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "D1 JSON cache value is not JSON serializable", {
      key,
    });
  }
  try {
    const json = JSON.stringify(value);
    if (json === undefined) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "D1 JSON cache value is not JSON serializable", {
        key,
      });
    }
    return json;
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "D1 JSON cache value is not JSON serializable", {
      key,
      cause,
    });
  }
}

function r2PutOptions(options?: PutOptions): {
  httpMetadata?: { contentType?: string };
  customMetadata?: Record<string, string>;
} {
  const result: {
    httpMetadata?: { contentType?: string };
    customMetadata?: Record<string, string>;
  } = {};
  if (options?.contentType) result.httpMetadata = { contentType: options.contentType };
  if (options?.metadata) result.customMetadata = options.metadata;
  return result;
}
