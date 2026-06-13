export interface PutOptions {
  contentType?: string;
  metadata?: Record<string, string>;
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
