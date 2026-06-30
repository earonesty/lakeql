import {
  LakeqlError,
  type ListOptions,
  type ObjectHead,
  type ObjectInfo,
  type ObjectStore,
  type PutOptions,
} from "lakeql-core";

export const PACKAGE = "lakeql-http" as const;

export interface HttpStoreOptions {
  baseUrl: string;
  fetch?: typeof fetch;
  headers?: HeadersInit;
  objects?: ObjectInfo[];
}

export function httpStore(options: HttpStoreOptions): ObjectStore {
  return new HttpObjectStore(options);
}

export class HttpObjectStore implements ObjectStore {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly headers: HeadersInit | undefined;
  private readonly objects: ObjectInfo[] | undefined;
  private readonly fullObjectCache = new Map<string, Uint8Array>();

  constructor(options: HttpStoreOptions) {
    this.baseUrl = options.baseUrl.endsWith("/") ? options.baseUrl : `${options.baseUrl}/`;
    // Bind the global fetch to globalThis: browsers throw "Illegal invocation"
    // if `fetch` is called as a method (`this.fetchImpl(...)`) with a non-global
    // `this`. A caller-supplied fetch is used as-is.
    this.fetchImpl = options.fetch ?? fetch.bind(globalThis);
    this.headers = options.headers;
    this.objects = options.objects;
  }

  async get(path: string): Promise<Uint8Array | null> {
    const response = await this.fetchPath(path, { method: "GET" });
    if (response.status === 404) return null;
    assertOk(response, path);
    const bytes = new Uint8Array(await response.arrayBuffer());
    this.fullObjectCache.set(path, bytes);
    return bytes;
  }

  async getRange(path: string, range: { offset: number; length: number }): Promise<Uint8Array> {
    if (range.offset < 0 || range.length < 0) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Invalid range for ${path}`, {
        path,
        range,
      });
    }
    const cached = this.fullObjectCache.get(path);
    if (cached !== undefined) return cached.subarray(range.offset, range.offset + range.length);
    const response = await this.fetchPath(path, {
      method: "GET",
      headers: { Range: `bytes=${range.offset}-${range.offset + range.length - 1}` },
    });
    if (response.status === 416) {
      const full = await this.fetchFullObject(path);
      return full.subarray(range.offset, range.offset + range.length);
    }
    assertOk(response, path);
    const bytes = new Uint8Array(await response.arrayBuffer());
    // `Range` is advisory: some servers (e.g. GitHub Pages on certain assets)
    // ignore it and return 200 with the full body. When that happens, slice the
    // requested window ourselves instead of handing back the whole object.
    if (response.status !== 206 && bytes.length > range.length) {
      this.fullObjectCache.set(path, bytes);
      return bytes.subarray(range.offset, range.offset + range.length);
    }
    return bytes;
  }

  async put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void> {
    const headers = new Headers(this.headers);
    if (options?.contentType) headers.set("content-type", options.contentType);
    const response = await this.fetchPath(path, { method: "PUT", headers, body: bodyInit(body) });
    assertOk(response, path);
  }

  async delete(path: string): Promise<void> {
    const response = await this.fetchPath(path, { method: "DELETE" });
    assertOk(response, path);
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
    if (response.status === 404) return null;
    assertOk(response, path);
    const probe = new Uint8Array(await response.arrayBuffer().catch(() => new ArrayBuffer(0)));
    const total = parseContentRangeTotal(response.headers.get("content-range"));
    const contentLength = response.headers.get("content-length");
    let size = total ?? (contentLength ? Number(contentLength) : Number.NaN);
    if (response.status === 206 && isCompressedRangeProbe(probe, response.headers)) {
      size = (await this.fetchFullObject(path)).byteLength;
    } else if (response.status !== 206 && probe.byteLength > 0) {
      this.fullObjectCache.set(path, probe);
      size = probe.byteLength;
    }
    if (!Number.isFinite(size)) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Missing size for ${path}`, { path });
    }
    const head: ObjectHead = { size };
    const etag = response.headers.get("etag");
    const lastModified = response.headers.get("last-modified");
    const contentType = response.headers.get("content-type");
    if (etag !== null) head.etag = etag;
    if (lastModified !== null) head.lastModified = new Date(lastModified);
    if (contentType !== null) head.contentType = contentType;
    return head;
  }

  private fetchPath(path: string, init: RequestInit): Promise<Response> {
    const headers = new Headers(this.headers);
    if (init.headers) {
      for (const [key, value] of new Headers(init.headers)) headers.set(key, value);
    }
    return this.fetchImpl(this.urlForPath(path), { ...init, headers });
  }

  private async fetchFullObject(path: string): Promise<Uint8Array> {
    const cached = this.fullObjectCache.get(path);
    if (cached !== undefined) return cached;
    const response = await this.fetchPath(path, { method: "GET" });
    assertOk(response, path);
    const bytes = new Uint8Array(await response.arrayBuffer());
    this.fullObjectCache.set(path, bytes);
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
  const vary = headers.get("vary")?.toLowerCase();
  if (vary?.split(",").some((value) => value.trim() === "accept-encoding")) return true;
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

function assertOk(response: Response, path: string): void {
  if (response.ok || response.status === 206) return;
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
