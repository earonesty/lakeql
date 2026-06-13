import { describe, expect, it } from "vitest";
import { type R2BucketLike, type R2ObjectBody, r2Store } from "./index.js";

const enc = new TextEncoder();

class FakeR2Object implements R2ObjectBody {
  readonly size: number;
  readonly uploaded = new Date("2026-06-13T00:00:00Z");
  readonly httpMetadata = { contentType: "application/octet-stream" };

  constructor(
    readonly key: string,
    private readonly bytes: Uint8Array,
    readonly etag = "etag",
  ) {
    this.size = bytes.byteLength;
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    const out = new ArrayBuffer(this.bytes.byteLength);
    new Uint8Array(out).set(this.bytes);
    return out;
  }
}

class FakeBucket implements R2BucketLike {
  readonly objects = new Map<string, Uint8Array>();
  metadata = true;
  lastPutOptions: unknown;

  async get(key: string, options?: { range?: { offset: number; length: number } }) {
    const bytes = this.objects.get(key);
    if (!bytes) return null;
    const ranged = options?.range
      ? bytes.slice(options.range.offset, options.range.offset + options.range.length)
      : bytes;
    return new FakeR2Object(key, ranged);
  }

  async head(key: string) {
    const bytes = this.objects.get(key);
    if (!bytes) return null;
    if (this.metadata) return new FakeR2Object(key, bytes);
    return { key, size: bytes.byteLength };
  }

  async put(key: string, value: Uint8Array | ReadableStream<Uint8Array>, options?: unknown) {
    this.lastPutOptions = options;
    if (value instanceof Uint8Array) this.objects.set(key, value);
    else this.objects.set(key, enc.encode("stream"));
  }

  async delete(key: string) {
    this.objects.delete(key);
  }

  async list(options?: { prefix?: string; limit?: number }) {
    const objects = [...this.objects.entries()]
      .filter(([key]) => key.startsWith(options?.prefix ?? ""))
      .sort(([a], [b]) => a.localeCompare(b))
      .slice(0, options?.limit)
      .map(([key, bytes]) => new FakeR2Object(key, bytes));
    return { objects };
  }
}

describe("r2Store", () => {
  it("adapts R2 bucket operations to ObjectStore", async () => {
    const bucket = new FakeBucket();
    const store = r2Store(bucket);

    await store.put("b.txt", enc.encode("abcdef"));
    await store.put("a.txt", enc.encode("a"), {
      contentType: "text/plain",
      metadata: { owner: "test" },
    });
    expect(bucket.lastPutOptions).toEqual({
      httpMetadata: { contentType: "text/plain" },
      customMetadata: { owner: "test" },
    });

    await expect(store.get("b.txt")).resolves.toEqual(enc.encode("abcdef"));
    await expect(store.getRange("b.txt", { offset: 2, length: 3 })).resolves.toEqual(
      enc.encode("cde"),
    );
    await expect(store.head("b.txt")).resolves.toMatchObject({
      size: 6,
      etag: "etag",
      contentType: "application/octet-stream",
    });

    const listed = [];
    for await (const object of store.list("", { limit: 1 })) listed.push(object.path);
    expect(listed).toEqual(["a.txt"]);

    await store.delete("a.txt");
    await expect(store.get("a.txt")).resolves.toBeNull();
    await expect(store.getRange("missing.txt", { offset: 0, length: 1 })).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });
  });

  it("accepts stream writes and optional metadata-less objects", async () => {
    const bucket = new FakeBucket();
    bucket.metadata = false;
    const store = r2Store(bucket);
    await store.put(
      "stream.txt",
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(enc.encode("ignored"));
          controller.close();
        },
      }),
    );

    await expect(store.get("stream.txt")).resolves.toEqual(enc.encode("stream"));
    await expect(store.head("stream.txt")).resolves.toEqual({ size: 6 });

    const listed = [];
    for await (const object of store.list("")) listed.push(object);
    expect(listed[0]).toMatchObject({ path: "stream.txt", size: 6 });
  });
});
