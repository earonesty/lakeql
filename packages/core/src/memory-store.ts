import { LakeqlError } from "./errors.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import type {
  ConditionalPutOptions,
  ListOptions,
  ObjectHead,
  ObjectInfo,
  ObjectStore,
  PutOptions,
} from "./store.js";

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
    return new Uint8Array(this.rangeView(path, range));
  }

  getRangeOwnedAllocation(_path: string, range: { offset: number; length: number }) {
    return { bytes: range.length, output: "exact-buffer" as const };
  }

  async getRangeOwned(
    path: string,
    range: { offset: number; length: number },
    scope: MemoryScope,
  ): Promise<OwnedMemory<Uint8Array>> {
    const view = this.rangeView(path, range);
    return scope.allocate(view.byteLength, "memory store range", () => new Uint8Array(view));
  }

  private rangeView(path: string, range: { offset: number; length: number }): Uint8Array {
    const obj = this.objects.get(path);
    if (!obj) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
    }
    if (
      !Number.isSafeInteger(range.offset) ||
      !Number.isSafeInteger(range.length) ||
      !Number.isSafeInteger(range.offset + range.length) ||
      range.offset < 0 ||
      range.length < 0 ||
      range.offset + range.length > obj.bytes.length
    ) {
      throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Range out of bounds for ${path}`, {
        path,
        range,
        size: obj.bytes.length,
      });
    }
    return obj.bytes.subarray(range.offset, range.offset + range.length);
  }

  async put(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options?: PutOptions,
  ): Promise<void> {
    const bytes = await collect(body);
    this.writeObject(path, bytes, options);
  }

  async conditionalPut(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options: ConditionalPutOptions,
  ): Promise<boolean> {
    const bytes = await collect(body);
    const current = this.objects.get(path);
    if (options.expectedEtag === null) {
      if (current !== undefined) return false;
    } else if (current?.etag !== options.expectedEtag) {
      return false;
    }
    this.writeObject(path, bytes, options);
    return true;
  }

  private writeObject(path: string, bytes: Uint8Array, options?: PutOptions): void {
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
