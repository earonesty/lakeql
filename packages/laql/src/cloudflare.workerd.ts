import { describe, expect, it } from "vitest";
import type { Bookmark, RuntimeSubstrate } from "./cloudflare.js";

class FakeR2Object {
  readonly size: number;
  readonly uploaded = new Date("2026-06-14T00:00:00Z");
  readonly httpMetadata = { contentType: "application/vnd.apache.parquet" };

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

class FakeR2Bucket {
  readonly objects = new Map<string, Uint8Array>();

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
    return new FakeR2Object(key, bytes);
  }

  async put(key: string, value: Uint8Array | ReadableStream<Uint8Array>) {
    if (value instanceof Uint8Array) this.objects.set(key, value);
    else {
      const chunks: Uint8Array[] = [];
      let total = 0;
      for await (const chunk of value) {
        chunks.push(chunk);
        total += chunk.byteLength;
      }
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      this.objects.set(key, bytes);
    }
  }

  async delete(key: string) {
    this.objects.delete(key);
  }

  async list(options?: { prefix?: string; limit?: number; cursor?: string }) {
    const start = options?.cursor === undefined ? 0 : Number(options.cursor);
    const matching = [...this.objects.entries()]
      .filter(([key]) => key.startsWith(options?.prefix ?? ""))
      .sort(([a], [b]) => a.localeCompare(b));
    const page = matching.slice(
      start,
      options?.limit === undefined ? undefined : start + options.limit,
    );
    return {
      objects: page.map(([key, bytes]) => new FakeR2Object(key, bytes)),
      truncated: false,
    };
  }
}

describe("laql/cloudflare", () => {
  it("imports the Cloudflare entrypoint in the Workers runtime", async () => {
    const cloudflare = await import("./cloudflare.js");

    expect(cloudflare.createLake).toBeTypeOf("function");
    expect(cloudflare.r2Store).toBeTypeOf("function");
    expect(cloudflare.memoryStore).toBeTypeOf("function");
  });

  it("streams R2-backed Parquet NDJSON under a Worker budget", async () => {
    const cloudflare = await import("./cloudflare.js");
    const bucket = new FakeR2Bucket();
    const store = cloudflare.r2Store(bucket);
    const checkpoints = new Map<string, Bookmark>();
    const queued: Bookmark[] = [];
    const counts: string[] = [];
    const timings: string[] = [];
    const substrate: RuntimeSubstrate = {
      checkpointStore: {
        get: async (jobId) => checkpoints.get(jobId),
        put: async (jobId, bookmark) => {
          checkpoints.set(jobId, bookmark);
        },
        delete: async (jobId) => {
          checkpoints.delete(jobId);
        },
      },
      queue: {
        send: async (bookmark) => {
          queued.push(bookmark);
        },
      },
      metrics: {
        count: (name) => counts.push(name),
        timing: (name) => timings.push(name),
      },
    };
    await cloudflare.writeParquet(store, "events.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "region", data: ["west", "east", "west"], type: "STRING" },
      ],
    });

    const lake = cloudflare.createLake({
      store,
      budget: { maxOutputRows: 2, maxRangeRequests: 16 },
      queryId: () => "q_r2_workerd",
      substrate,
    });
    const result = lake
      .path("events.parquet")
      .select(["id"])
      .where(cloudflare.eq("region", "west"))
      .run();

    await expect(new Response(result.streamNdjson()).text()).resolves.toBe('{"id":1}\n{"id":3}\n');
    expect(result.stats).toMatchObject({
      queryId: "q_r2_workerd",
      filesRead: 1,
      rowsReturned: 2,
    });
    expect(result.stats.rangeRequests).toBeGreaterThan(0);
    expect(counts).toEqual(["laql.query.created"]);
    expect(timings).toEqual(["laql.query.elapsed"]);

    const slice = await lake
      .path("events.parquet")
      .select(["id"])
      .where(cloudflare.eq("region", "west"))
      .run()
      .slice({ maxRows: 1 });
    expect(slice.rows).toEqual([{ id: 1 }]);
    expect(slice.bookmark).toBeDefined();
    if (slice.bookmark === undefined) throw new Error("expected slice bookmark");
    await substrate.queue?.send(slice.bookmark);
    await substrate.checkpointStore?.put("worker-job", slice.bookmark);

    expect(queued).toHaveLength(1);
    await expect(substrate.checkpointStore?.get("worker-job")).resolves.toEqual(slice.bookmark);
  });
});
