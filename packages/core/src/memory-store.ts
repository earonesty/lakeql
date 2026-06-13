import { LaQLError } from "./errors.js";
import type { ListOptions, ObjectHead, ObjectInfo, ObjectStore, PutOptions } from "./store.js";

interface StoredObject {
  bytes: Uint8Array;
  etag: string;
  lastModified: Date;
  contentType?: string;
}

async function collect(body: Uint8Array | ReadableStream<Uint8Array>): Promise<Uint8Array> {
  if (body instanceof Uint8Array) return body;
  const chunks: Uint8Array[] = [];
  let total = 0;
  for await (const chunk of body) {
    chunks.push(chunk);
    total += chunk.byteLength;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/**
 * In-memory ObjectStore. The reference implementation of the store
 * contract, used by tests and usable as a small read-through cache.
 */
export class MemoryObjectStore implements ObjectStore {
  private readonly objects = new Map<string, StoredObject>();
  private version = 0;

  async get(path: string): Promise<Uint8Array | null> {
    return this.objects.get(path)?.bytes ?? null;
  }

  async getRange(path: string, range: { offset: number; length: number }): Promise<Uint8Array> {
    const obj = this.objects.get(path);
    if (!obj) {
      throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
    }
    if (range.offset < 0 || range.length < 0 || range.offset + range.length > obj.bytes.length) {
      throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `Range out of bounds for ${path}`, {
        path,
        range,
        size: obj.bytes.length,
      });
    }
    return obj.bytes.slice(range.offset, range.offset + range.length);
  }

  async put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void> {
    const bytes = await collect(body);
    this.version += 1;
    const stored: StoredObject = {
      bytes,
      etag: `v${this.version}`,
      lastModified: new Date(),
    };
    if (options?.contentType !== undefined) stored.contentType = options.contentType;
    this.objects.set(path, stored);
  }

  async delete(path: string): Promise<void> {
    this.objects.delete(path);
  }

  async *list(prefix: string, options?: ListOptions): AsyncIterable<ObjectInfo> {
    let emitted = 0;
    const paths = [...this.objects.keys()].filter((p) => p.startsWith(prefix)).sort();
    for (const path of paths) {
      if (options?.limit !== undefined && emitted >= options.limit) return;
      // biome-ignore lint/style/noNonNullAssertion: key came from the map
      const obj = this.objects.get(path)!;
      yield { path, size: obj.bytes.length, etag: obj.etag, lastModified: obj.lastModified };
      emitted += 1;
    }
  }

  async head(path: string): Promise<ObjectHead | null> {
    const obj = this.objects.get(path);
    if (!obj) return null;
    const head: ObjectHead = {
      size: obj.bytes.length,
      etag: obj.etag,
      lastModified: obj.lastModified,
    };
    if (obj.contentType !== undefined) head.contentType = obj.contentType;
    return head;
  }
}

export function memoryStore(): MemoryObjectStore {
  return new MemoryObjectStore();
}
