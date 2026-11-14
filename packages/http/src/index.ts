import {
  MATERIALIZATION_CHARGES as c,
  isLakeqlError,
  LakeqlError,
  type ListOptions,
  type MemoryBudget,
  MemoryScope,
  type ObjectHead,
  type ObjectInfo,
  type ObjectStore,
  type OwnedMemory,
  type PutOptions,
  readOwnedStream,
  readOwnedStreamWindow,
  SharedMemoryCache,
} from "lakeql-core";
import { boundedResponseBody } from "./response-body.js";

export const PACKAGE = "lakeql-http" as const;

export interface HttpStoreOptions {
  baseUrl: string;
  fetch?: typeof fetch;
  headers?: HeadersInit;
  /** Maximum decoded bytes per response, including full-response fallback. Default: 16 MiB. */
  maxResponseBytes?: number;
  /** Maximum full-response fallback cache payload bytes. Default: 16 MiB. */
  maxFullObjectCacheBytes?: number;
  objects?: ObjectInfo[];
  /** Shared pool for retained full-response backing and owned probe reuse. */
  memoryBudget?: MemoryBudget;
}

export function httpStore(options: HttpStoreOptions): ObjectStore {
  return new HttpObjectStore(options);
}

export class HttpObjectStore implements ObjectStore {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly headers: HeadersInit | undefined;
  private readonly objects: ObjectInfo[] | undefined;
  private readonly fullObjectCache: SharedMemoryCache;
  private readonly maxResponseBytes: number;

  private readonly options: HttpStoreOptions;

  constructor(options: HttpStoreOptions) {
    this.options = { ...options };
    this.baseUrl = options.baseUrl.endsWith("/") ? options.baseUrl : `${options.baseUrl}/`;
    // Bind the global fetch to globalThis: browsers throw "Illegal invocation"
    // if `fetch` is called as a method (`this.fetchImpl(...)`) with a non-global
    // `this`. A caller-supplied fetch is used as-is.
    this.fetchImpl = options.fetch ?? fetch.bind(globalThis);
    this.headers = options.headers;
    this.objects = options.objects;
    this.maxResponseBytes = options.maxResponseBytes ?? 16 * 1024 * 1024;
    const maxCacheBytes = options.maxFullObjectCacheBytes ?? 16 * 1024 * 1024;
    for (const bytes of [this.maxResponseBytes, maxCacheBytes]) {
      if (!Number.isSafeInteger(bytes) || bytes < 0) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "HTTP byte limits must be non-negative safe integers",
          { bytes },
        );
      }
    }
    this.fullObjectCache = new SharedMemoryCache({
      maxBytes: maxCacheBytes,
      ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    });
  }

  withDefaultMemoryBudget(budget: MemoryBudget, maxCacheBytes?: number): HttpObjectStore {
    if (this.options.memoryBudget !== undefined) return this;
    return new HttpObjectStore({
      ...this.options,
      memoryBudget: budget,
      ...(this.options.maxFullObjectCacheBytes !== undefined || maxCacheBytes === undefined
        ? {}
        : { maxFullObjectCacheBytes: maxCacheBytes }),
    });
  }

  async get(path: string): Promise<Uint8Array | null> {
    const response = await this.fetchPath(path, { method: "GET" });
    if (response.status === 404) {
      await discardResponse(response);
      return null;
    }
    await assertOk(response, path);
    const bytes = await boundedResponseBody(response, this.maxResponseBytes);
    this.fullObjectCache.set(path, bytes, bytes.buffer.byteLength);
    return bytes;
  }

  async getRange(path: string, range: { offset: number; length: number }): Promise<Uint8Array> {
    if (
      !Number.isSafeInteger(range.offset) ||
      !Number.isSafeInteger(range.length) ||
      !Number.isSafeInteger(range.offset + range.length) ||
      range.offset < 0 ||
      range.length < 0
    ) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Invalid range for ${path}`, {
        path,
        range,
      });
    }
    if (range.length === 0) return new Uint8Array();
    const cached = this.fullObjectCache.get<Uint8Array>(path)?.value;
    if (cached !== undefined) return cached.subarray(range.offset, range.offset + range.length);
    const response = await this.fetchPath(path, {
      method: "GET",
      headers: { Range: `bytes=${range.offset}-${range.offset + range.length - 1}` },
    });
    if (response.status === 416) {
      await discardResponse(response);
      const full = await this.fetchFullObject(path);
      return full.subarray(range.offset, range.offset + range.length);
    }
    await assertOk(response, path);
    const bytes = await boundedResponseBody(response, this.maxResponseBytes);
    // `Range` is advisory: some servers (e.g. GitHub Pages on certain assets)
    // ignore it and return 200 with the full body. When that happens, slice the
    // requested window ourselves instead of handing back the whole object.
    if (response.status !== 206 && bytes.length > range.length) {
      this.fullObjectCache.set(path, bytes, bytes.buffer.byteLength);
      return bytes.subarray(range.offset, range.offset + range.length);
    }
    return bytes;
  }

  getRangeOwnedAllocation(_path: string, range: { offset: number; length: number }) {
    return { bytes: range.length, output: "exact-buffer" as const };
  }

  async getRangeOwned(
    path: string,
    range: { offset: number; length: number },
    scope: MemoryScope,
  ): Promise<OwnedMemory<Uint8Array>> {
    if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "HTTP read scope disposed");
    if (
      !Number.isSafeInteger(range.offset) ||
      !Number.isSafeInteger(range.length) ||
      !Number.isSafeInteger(range.offset + range.length) ||
      range.offset < 0 ||
      range.length < 0
    ) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Invalid range for ${path}`, {
        path,
        range,
      });
    }
    if (range.length === 0) return scope.allocate(0, "empty HTTP range", () => new Uint8Array());
    let cached: OwnedMemory<Uint8Array> | undefined;
    try {
      cached = this.fullObjectCache.acquireOwned<Uint8Array>(path, scope.budget);
      if (cached !== undefined && range.offset + range.length <= cached.value.byteLength) {
        const bytes = cached.value;
        return scope.allocate(range.length, "cached HTTP owned range", () =>
          bytes.slice(range.offset, range.offset + range.length),
        );
      }
    } catch (error) {
      // A small child budget may fit the range but not a pin on the full cache
      // backing. Fall back to a bounded transport read after releasing the pin.
      if (!isLakeqlError(error) || error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
    } finally {
      cached?.release();
    }
    const reservation = scope.reserve(
      Math.min(range.length, this.maxResponseBytes),
      "HTTP range response",
    );
    try {
      let response = await this.fetchOwnedPath(
        path,
        {
          method: "GET",
          headers: { Range: `bytes=${range.offset}-${range.offset + range.length - 1}` },
        },
        scope,
      );
      if (response.status === 416) {
        await discardResponse(response);
        response = await this.fetchOwnedPath(path, { method: "GET" }, scope);
      }
      await assertOk(response, path);
      return await readOwnedStreamWindow(response.body, scope, {
        offset: response.status === 206 ? 0 : range.offset,
        length: range.length,
        maxBytes:
          response.status === 206
            ? Math.min(range.length, this.maxResponseBytes)
            : this.maxResponseBytes,
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
    const headers = new Headers(this.headers);
    if (options?.contentType) headers.set("content-type", options.contentType);
    const response = await this.fetchPath(path, { method: "PUT", headers, body: bodyInit(body) });
    await assertOk(response, path);
    this.fullObjectCache.delete(path);
    await discardResponse(response);
  }

  async delete(path: string): Promise<void> {
    const response = await this.fetchPath(path, { method: "DELETE" });
    await assertOk(response, path);
    this.fullObjectCache.delete(path);
    await discardResponse(response);
  }

  async *list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo> {
    if (!this.objects) {
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PUSHDOWN",
        "HTTP store list requires an object index",
        {
          prefix,
        },
      );
    }
    let emitted = 0;
    for (const object of this.objects
      .filter((candidate) => candidate.path.startsWith(prefix))
      .sort((a, b) => a.path.localeCompare(b.path))) {
      if (options?.limit !== undefined && emitted >= options.limit) return;
      yield object;
      emitted += 1;
    }
  }

  async head(path: string): Promise<ObjectHead | null> {
    // Probe with a tiny ranged GET rather than HEAD. Some static hosts report
    // compressed content-lengths from HEAD; the ranged response is normally the
    // authoritative byte length for follow-up ranges.
    const response = await this.fetchPath(path, {
      method: "GET",
      headers: { Range: "bytes=0-1" },
    });
    if (response.status === 404) {
      await discardResponse(response);
      return null;
    }
    await assertOk(response, path);
    const probe = await boundedResponseBody(response, this.maxResponseBytes);
    const total = parseContentRangeTotal(response.headers.get("content-range"));
    const contentLength = response.headers.get("content-length");
    let size = total ?? (contentLength ? Number(contentLength) : Number.NaN);
    const compressedProbe =
      response.status === 206 && isCompressedRangeProbe(probe, response.headers);
    if (compressedProbe) {
      size = (await this.fetchFullObject(path)).byteLength;
    } else if (response.status !== 206 && probe.byteLength > 0) {
      this.fullObjectCache.set(path, probe, probe.buffer.byteLength);
      size = probe.byteLength;
    }
    if (!Number.isFinite(size)) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Missing size for ${path}`, { path });
    }
    const head: ObjectHead = {
      size,
      rangeReadMode: response.status === 206 && !compressedProbe ? "native" : "emulated",
    };
    const etag = response.headers.get("etag");
    const lastModified = response.headers.get("last-modified");
    const contentType = response.headers.get("content-type");
    if (etag !== null) head.etag = etag;
    if (lastModified !== null) head.lastModified = new Date(lastModified);
    if (contentType !== null) head.contentType = contentType;
    return head;
  }

  async headOwned(path: string, scope: MemoryScope): Promise<OwnedMemory<ObjectHead> | null> {
    const scratch = scope.childScope();
    this.fullObjectCache.delete(path);
    try {
      let response = await this.fetchOwnedPath(
        path,
        { method: "GET", headers: { Range: "bytes=0-1" } },
        scratch,
      );
      if (response.status === 404) {
        await discardResponse(response);
        return null;
      }
      await assertOk(response, path);
      let probe = await readOwnedStream(response.body, scratch, {
        maxBytes: this.maxResponseBytes,
        purpose: "HTTP metadata probe",
      });
      const compressed =
        response.status === 206 && isCompressedRangeProbe(probe.value, response.headers);
      let size = parseContentRangeTotal(response.headers.get("content-range"));
      if (compressed) {
        probe.release();
        response = await this.fetchOwnedPath(path, { method: "GET" }, scratch);
        await assertOk(response, path);
        probe = await readOwnedStream(response.body, scratch, {
          maxBytes: this.maxResponseBytes,
          purpose: "HTTP full metadata probe",
        });
        size = probe.value.byteLength;
      } else if (response.status !== 206) {
        size = probe.value.byteLength;
      } else if (size === undefined) {
        const length = response.headers.get("content-length");
        size = length === null ? Number.NaN : Number(length);
      }
      if (!Number.isSafeInteger(size) || size < 0) {
        throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Missing size for ${path}`, { path });
      }
      if (response.status !== 206 || compressed) this.retainOwnedProbe(path, probe);
      const etag = response.headers.get("etag");
      const modified = response.headers.get("last-modified");
      const contentType = response.headers.get("content-type");
      const charge =
        c.object +
        5 * c.property +
        (etag === null ? 0 : c.string + etag.length * c.stringCodeUnit) +
        (modified === null ? 0 : c.timestamp) +
        (contentType === null ? 0 : c.string + contentType.length * c.stringCodeUnit);
      return scope
        .allocate<ObjectHead>(charge, "HTTP object metadata", () => ({
          size,
          rangeReadMode: response.status === 206 && !compressed ? "native" : "emulated",
          ...(etag === null ? {} : { etag }),
          ...(modified === null ? {} : { lastModified: new Date(modified) }),
          ...(contentType === null ? {} : { contentType }),
        }))
        .retain();
    } finally {
      scratch.dispose();
    }
  }

  /** Release retained full responses; independent active read owners remain valid. */
  clearFullObjectCache(): void {
    this.fullObjectCache.deleteWhere(() => true);
  }

  private retainOwnedProbe(path: string, owner: OwnedMemory<Uint8Array>): void {
    if (!this.fullObjectCache.supportsOwnedRetention(owner.budget)) return;
    const scope = new MemoryScope(owner.budget);
    try {
      scope.allocate(
        c.object + 6 * c.property + c.string + path.length * c.stringCodeUnit,
        "HTTP full response cache entry",
        () => undefined,
      );
      const retained = owner.retain();
      try {
        scope.adopt(retained);
      } catch (error) {
        retained.release();
        throw error;
      }
      const entry = scope.retainAllocations(owner.value);
      try {
        this.fullObjectCache.setOwned(path, entry);
      } finally {
        entry.release();
      }
    } catch (error) {
      if (!isLakeqlError(error) || error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
    } finally {
      scope.dispose();
    }
  }

  private async fetchOwnedPath(
    path: string,
    init: RequestInit,
    scope: MemoryScope,
  ): Promise<Response> {
    try {
      return await this.fetchPath(path, { ...init, signal: scope.signal });
    } catch (error) {
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "HTTP read scope disposed");
      throw error;
    }
  }

  private fetchPath(path: string, init: RequestInit): Promise<Response> {
    const headers = new Headers(this.headers);
    if (init.headers) {
      for (const [key, value] of new Headers(init.headers)) headers.set(key, value);
    }
    return this.fetchImpl(this.urlForPath(path), { ...init, headers });
  }

  private async fetchFullObject(path: string): Promise<Uint8Array> {
    const cached = this.fullObjectCache.get<Uint8Array>(path)?.value;
    if (cached !== undefined) return cached;
    const response = await this.fetchPath(path, { method: "GET" });
    await assertOk(response, path);
    const bytes = await boundedResponseBody(response, this.maxResponseBytes);
    this.fullObjectCache.set(path, bytes, bytes.buffer.byteLength);
    return bytes;
  }

  private urlForPath(path: string): URL {
    const base = new URL(this.baseUrl);
    const url = new URL(encodeObjectPath(path), base);
    if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname)) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        `Object path escapes HTTP base URL: ${path}`,
        {
          path,
          baseUrl: this.baseUrl,
        },
      );
    }
    return url;
  }
}

function isCompressedRangeProbe(bytes: Uint8Array, headers: Headers): boolean {
  const encoding = headers.get("content-encoding")?.toLowerCase();
  if (encoding && encoding !== "identity") return true;
  if (bytes[0] === 0x1f && bytes[1] === 0x8b) return true;
  return false;
}

function encodeObjectPath(path: string): string {
  if (/^(?:[a-z][a-z0-9+.-]*:)?\/\//iu.test(path) || path.startsWith("/")) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", `Object path must be relative: ${path}`, {
      path,
    });
  }
  if (path === "") return "";
  return path
    .split("/")
    .map((segment) => {
      let decoded: string;
      try {
        decoded = decodeURIComponent(segment);
      } catch {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          `Object path has invalid encoding: ${path}`,
          {
            path,
          },
        );
      }
      if (decoded === "." || decoded === "..") {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          `Object path contains traversal: ${path}`,
          {
            path,
          },
        );
      }
      return encodeURIComponent(segment);
    })
    .join("/");
}

// Parses the total length from a `content-range` header (e.g. "bytes 0-0/2820").
function parseContentRangeTotal(header: string | null): number | undefined {
  if (!header) return undefined;
  const match = /\/(\d+)\s*$/.exec(header);
  if (!match) return undefined;
  const total = Number(match[1]);
  return Number.isFinite(total) ? total : undefined;
}

async function discardResponse(response: Response): Promise<void> {
  // Cleanup must not replace the HTTP result if a transport rejects cancellation.
  try {
    await response.body?.cancel();
  } catch {}
}

async function assertOk(response: Response, path: string): Promise<void> {
  if (response.ok || response.status === 206) return;
  await discardResponse(response);
  if (response.status === 404) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
  }
  throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `HTTP object request failed for ${path}`, {
    path,
    status: response.status,
  });
}

function bodyInit(body: Uint8Array | ReadableStream<Uint8Array>): BodyInit {
  if (body instanceof Uint8Array) {
    const copy = new Uint8Array(body.byteLength);
    copy.set(body);
    return copy.buffer;
  }
  return body;
}
