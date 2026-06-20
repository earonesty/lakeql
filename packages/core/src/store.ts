import { LakeqlError } from "./errors.js";

export interface PutOptions {
  contentType?: string;
  metadata?: Record<string, string>;
}

export interface ConditionalPutOptions extends PutOptions {
  /**
   * ETag that must still identify the existing object. Use null to require
   * the object to be absent.
   */
  expectedEtag: string | null;
}

export interface ListOptions {
  /** Stop listing after this many objects. */
  limit?: number;
  /** List "directories" by stopping at this delimiter. */
  delimiter?: string;
}

export interface ObjectInfo {
  path: string;
  size: number;
  etag?: string;
  lastModified?: Date;
}

export interface ObjectHead {
  size: number;
  etag?: string;
  lastModified?: Date;
  contentType?: string;
}

/**
 * The single environmental contract for storage. Every runtime driver
 * (R2, S3, HTTP, filesystem) supplies one of these; core never touches
 * a runtime API directly.
 */
export interface ObjectStore {
  get(path: string): Promise<Uint8Array | null>;

  getRange(path: string, range: { offset: number; length: number }): Promise<Uint8Array>;

  put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void>;

  delete(path: string): Promise<void>;

  list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo>;

  head(path: string): Promise<ObjectHead | null>;
}

export interface ConditionalObjectStore extends ObjectStore {
  conditionalPut(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options: ConditionalPutOptions,
  ): Promise<boolean>;
}

export interface ObjectStoreReadControls {
  maxConcurrentReads?: number;
  signal?: AbortSignal;
  maxElapsedMs?: number;
}

export interface ObjectStoreUriAuthority {
  scheme: string;
  authority: string;
  prefix?: string;
}

export function uriObjectStore(
  store: ConditionalObjectStore,
  authorities: ObjectStoreUriAuthority | readonly ObjectStoreUriAuthority[],
): ConditionalObjectStore;
export function uriObjectStore(
  store: ObjectStore,
  authorities: ObjectStoreUriAuthority | readonly ObjectStoreUriAuthority[],
): ObjectStore;
export function uriObjectStore(
  store: ObjectStore,
  authorities: ObjectStoreUriAuthority | readonly ObjectStoreUriAuthority[],
): ObjectStore {
  const mappings = (Array.isArray(authorities) ? authorities : [authorities]).map(
    normalizeUriAuthority,
  );
  const wrapped: ObjectStore = {
    async get(path) {
      return await store.get(normalizeObjectStorePath(path, mappings));
    },
    async getRange(path, range) {
      return await store.getRange(normalizeObjectStorePath(path, mappings), range);
    },
    async put(path, body, options) {
      return await store.put(normalizeObjectStorePath(path, mappings), body, options);
    },
    async delete(path) {
      return await store.delete(normalizeObjectStorePath(path, mappings));
    },
    async *list(prefix, options) {
      yield* store.list(normalizeObjectStorePath(prefix, mappings), options);
    },
    async head(path) {
      return await store.head(normalizeObjectStorePath(path, mappings));
    },
  };
  if (isConditionalObjectStore(store)) {
    const conditional: ConditionalObjectStore = {
      ...wrapped,
      async conditionalPut(path, body, options) {
        return await store.conditionalPut(normalizeObjectStorePath(path, mappings), body, options);
      },
    };
    return conditional;
  }
  return wrapped;
}

export function withObjectStoreReadControls(
  store: ObjectStore,
  controls: ObjectStoreReadControls,
): ObjectStore {
  if (
    controls.maxConcurrentReads === undefined &&
    controls.signal === undefined &&
    controls.maxElapsedMs === undefined
  ) {
    return store;
  }
  const signal = readControlSignal(controls);
  const normalized: ObjectStoreReadControls = {};
  if (controls.maxConcurrentReads !== undefined)
    normalized.maxConcurrentReads = controls.maxConcurrentReads;
  if (signal !== undefined) normalized.signal = signal;
  const semaphore =
    normalized.maxConcurrentReads === undefined
      ? undefined
      : new ReadSemaphore(normalized.maxConcurrentReads);
  return {
    get: (path) => controlledRead(normalized, semaphore, () => store.get(path)),
    getRange: (path, range) =>
      controlledRead(normalized, semaphore, () => store.getRange(path, range)),
    put: (path, body, options) => {
      throwIfAborted(normalized.signal);
      return store.put(path, body, options);
    },
    delete: (path) => {
      throwIfAborted(normalized.signal);
      return store.delete(path);
    },
    list: (prefix, options) => controlledList(store, prefix, options, normalized),
    head: async (path) => {
      throwIfAborted(normalized.signal);
      const result = await store.head(path);
      throwIfAborted(normalized.signal);
      return result;
    },
  };
}

export function readControlSignal(controls: ObjectStoreReadControls): AbortSignal | undefined {
  if (controls.maxElapsedMs === undefined) return controls.signal;
  const timeout = AbortSignal.timeout(controls.maxElapsedMs);
  if (controls.signal === undefined) return timeout;
  if (controls.signal.aborted) return controls.signal;
  const controller = new AbortController();
  const abortFrom = (signal: AbortSignal) => {
    if (!controller.signal.aborted) controller.abort(signal.reason);
  };
  controls.signal.addEventListener("abort", () => abortFrom(controls.signal as AbortSignal), {
    once: true,
  });
  timeout.addEventListener("abort", () => abortFrom(timeout), { once: true });
  return controller.signal;
}

export function throwIfAborted(signal: AbortSignal | undefined): void {
  if (!signal?.aborted) return;
  throw new LakeqlError("LAKEQL_ABORTED", "Query aborted", {
    reason: signal.reason instanceof Error ? signal.reason.message : signal.reason,
  });
}

async function controlledRead<T>(
  controls: ObjectStoreReadControls,
  semaphore: ReadSemaphore | undefined,
  read: () => Promise<T>,
): Promise<T> {
  throwIfAborted(controls.signal);
  const release = semaphore === undefined ? undefined : await semaphore.acquire(controls.signal);
  try {
    throwIfAborted(controls.signal);
    const result = await read();
    throwIfAborted(controls.signal);
    return result;
  } finally {
    release?.();
  }
}

function normalizeUriAuthority(
  mapping: ObjectStoreUriAuthority,
): Required<ObjectStoreUriAuthority> {
  const scheme = requiredUriPart(mapping.scheme, "scheme").toLowerCase();
  const authority = requiredUriPart(mapping.authority, "authority");
  const prefix = trimSlashes(mapping.prefix ?? "");
  return { scheme, authority, prefix };
}

function normalizeObjectStorePath(
  path: string,
  mappings: readonly Required<ObjectStoreUriAuthority>[],
): string {
  const maybeUri = absoluteObjectStoreUri(path);
  if (maybeUri === undefined) return path;
  const match = mappings.find(
    (mapping) => mapping.scheme === maybeUri.scheme && mapping.authority === maybeUri.authority,
  );
  if (match === undefined) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Object URI does not match configured store", {
      path,
      scheme: maybeUri.scheme,
      authority: maybeUri.authority,
      configuredAuthorities: mappings.map((mapping) => `${mapping.scheme}://${mapping.authority}`),
    });
  }
  const key = maybeUri.path;
  if (match.prefix === "") return key;
  if (key === match.prefix) return "";
  const prefix = `${match.prefix}/`;
  if (key.startsWith(prefix)) return key.slice(prefix.length);
  throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Object URI is outside configured prefix", {
    path,
    prefix: match.prefix,
  });
}

function absoluteObjectStoreUri(
  path: string,
): { scheme: string; authority: string; path: string } | undefined {
  if (!/^[A-Za-z][A-Za-z0-9+.-]*:\/\//u.test(path)) return undefined;
  rejectPathEscape(rawUriPath(path), path);
  let url: URL;
  try {
    url = new URL(path);
  } catch (cause) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid object URI", { path, cause });
  }
  const objectPath = trimLeadingSlash(url.pathname);
  rejectPathEscape(objectPath, path);
  return {
    scheme: url.protocol.slice(0, -1).toLowerCase(),
    authority: url.host,
    path: objectPath,
  };
}

function rejectPathEscape(objectPath: string, original: string): void {
  const decoded = objectPath
    .split("/")
    .map((segment) => {
      try {
        return decodeURIComponent(segment);
      } catch {
        return segment;
      }
    })
    .join("/");
  if (
    decoded === ".." ||
    decoded.startsWith("../") ||
    decoded.includes("/../") ||
    decoded.endsWith("/..")
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Object URI path may not escape its store", {
      path: original,
    });
  }
}

function rawUriPath(uri: string): string {
  const authorityStart = uri.indexOf("://") + 3;
  const pathStart = uri.indexOf("/", authorityStart);
  if (pathStart === -1) return "";
  const queryStart = uri.indexOf("?", pathStart);
  const fragmentStart = uri.indexOf("#", pathStart);
  const endCandidates = [queryStart, fragmentStart].filter((index) => index !== -1);
  const pathEnd = endCandidates.length === 0 ? uri.length : Math.min(...endCandidates);
  return trimLeadingSlash(uri.slice(pathStart, pathEnd));
}

function requiredUriPart(value: string, name: string): string {
  const trimmed = value.trim();
  if (trimmed === "") {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", `Object URI ${name} is required`);
  }
  return trimmed;
}

function trimSlashes(value: string): string {
  return value.replace(/^\/+/u, "").replace(/\/+$/u, "");
}

function trimLeadingSlash(value: string): string {
  return value.replace(/^\/+/u, "");
}

function isConditionalObjectStore(store: ObjectStore): store is ConditionalObjectStore {
  return typeof (store as Partial<ConditionalObjectStore>).conditionalPut === "function";
}

async function* controlledList(
  store: ObjectStore,
  prefix: string,
  options: ListOptions | undefined,
  controls: ObjectStoreReadControls,
): AsyncIterable<ObjectInfo> {
  throwIfAborted(controls.signal);
  for await (const object of store.list(prefix, options)) {
    throwIfAborted(controls.signal);
    yield object;
  }
}

class ReadSemaphore {
  private active = 0;
  private readonly queue: (() => void)[] = [];

  constructor(private readonly limit: number) {
    if (!Number.isInteger(limit) || limit < 1) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "maxConcurrentReads must be a positive integer",
        {
          maxConcurrentReads: limit,
        },
      );
    }
  }

  async acquire(signal: AbortSignal | undefined): Promise<() => void> {
    throwIfAborted(signal);
    if (this.active < this.limit) {
      this.active += 1;
      return () => this.release();
    }
    await new Promise<void>((resolve, reject) => {
      const resume = () => {
        cleanup();
        resolve();
      };
      const abort = () => {
        const index = this.queue.indexOf(resume);
        if (index >= 0) this.queue.splice(index, 1);
        cleanup();
        reject(
          new LakeqlError("LAKEQL_ABORTED", "Query aborted", {
            reason: signal?.reason instanceof Error ? signal.reason.message : signal?.reason,
          }),
        );
      };
      const cleanup = () => signal?.removeEventListener("abort", abort);
      this.queue.push(resume);
      signal?.addEventListener("abort", abort, { once: true });
    });
    throwIfAborted(signal);
    this.active += 1;
    return () => this.release();
  }

  private release(): void {
    this.active -= 1;
    this.queue.shift()?.();
  }
}
