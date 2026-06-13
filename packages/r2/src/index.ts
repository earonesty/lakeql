import {
  LaQLError,
  type ListOptions,
  type ObjectHead,
  type ObjectInfo,
  type ObjectStore,
  type PutOptions,
} from "@laql/core";

export const PACKAGE = "@laql/r2" as const;

export interface R2ObjectBody {
  key: string;
  size: number;
  etag?: string;
  uploaded?: Date;
  httpMetadata?: { contentType?: string };
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
  list(options?: { prefix?: string; limit?: number }): Promise<{
    objects: Omit<R2ObjectBody, "arrayBuffer">[];
    truncated?: boolean;
    cursor?: string;
  }>;
}

export function r2Store(bucket: R2BucketLike): ObjectStore {
  return new R2ObjectStore(bucket);
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
    if (!object) throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
    return new Uint8Array(await object.arrayBuffer());
  }

  async put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void> {
    const putOptions: {
      httpMetadata?: { contentType?: string };
      customMetadata?: Record<string, string>;
    } = {};
    if (options?.contentType) putOptions.httpMetadata = { contentType: options.contentType };
    if (options?.metadata) putOptions.customMetadata = options.metadata;
    await this.bucket.put(path, body, putOptions);
  }

  async delete(path: string): Promise<void> {
    await this.bucket.delete(path);
  }

  async *list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo> {
    const listOptions: { prefix?: string; limit?: number } = { prefix };
    if (options?.limit !== undefined) listOptions.limit = options.limit;
    const result = await this.bucket.list(listOptions);
    for (const object of result.objects) yield r2Info(object);
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
