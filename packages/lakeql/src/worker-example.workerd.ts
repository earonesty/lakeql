import { describe, expect, it } from "vitest";
import worker from "../../../examples/worker/src/index.js";
import { writeParquet } from "./cloudflare.js";

class WorkerR2Object {
  readonly size: number;
  readonly uploaded = new Date("2026-06-15T00:00:00Z");
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

class WorkerR2Bucket {
  private readonly objects = new Map<string, Uint8Array>();

  async get(key: string, options?: { range?: { offset: number; length: number } }) {
    const bytes = this.objects.get(key);
    if (bytes === undefined) return null;
    const ranged =
      options?.range === undefined
        ? bytes
        : bytes.slice(options.range.offset, options.range.offset + options.range.length);
    return new WorkerR2Object(key, ranged);
  }

  async head(key: string) {
    const bytes = this.objects.get(key);
    if (bytes === undefined) return null;
    return new WorkerR2Object(key, bytes);
  }

  async put(key: string, value: Uint8Array | ReadableStream<Uint8Array>) {
    if (value instanceof Uint8Array) {
      this.objects.set(key, value);
      return;
    }
    const chunks: Uint8Array[] = [];
    let length = 0;
    for await (const chunk of value) {
      chunks.push(chunk);
      length += chunk.byteLength;
    }
    const out = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    this.objects.set(key, out);
  }

  async delete(key: string) {
    this.objects.delete(key);
  }

  async list(options?: { prefix?: string; limit?: number; cursor?: string }) {
    const prefix = options?.prefix ?? "";
    const start = options?.cursor === undefined ? 0 : Number(options.cursor);
    const limit = options?.limit ?? Number.POSITIVE_INFINITY;
    const matching = [...this.objects.entries()]
      .filter(([key]) => key.startsWith(prefix))
      .sort(([left], [right]) => left.localeCompare(right));
    const page = matching.slice(start, start + limit);
    const next = start + page.length;
    return {
      objects: page.map(([key, bytes]) => new WorkerR2Object(key, bytes)),
      truncated: next < matching.length,
      ...(next < matching.length ? { cursor: String(next) } : {}),
    };
  }
}

describe("examples/worker", () => {
  it("serves Parquet rows and Iceberg plans from an R2-backed Worker", async () => {
    const bucket = new WorkerR2Bucket();
    await writeParquet(bucketStore(bucket), "sales.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "store_id", data: ["store-000", "store-001", "store-002"], type: "STRING" },
        { name: "region", data: ["west", "east", "west"], type: "STRING" },
        { name: "amount", data: [10, 20, 30], type: "INT32" },
      ],
    });
    await bucket.put(
      "iceberg/warehouse/places/metadata/v2.metadata.json",
      new TextEncoder().encode(`${JSON.stringify(inlineIcebergMetadata())}\n`),
    );

    const parquetResponse = await worker.fetch(new Request("https://example.test/parquet"), {
      DATA: bucket,
    });
    expect(parquetResponse.status).toBe(200);
    const rows = (await parquetResponse.json()) as Record<string, unknown>[];
    expect(rows).toHaveLength(3);
    expect(rows[0]).toMatchObject({ store_id: "store-000", region: "west" });

    const icebergResponse = await worker.fetch(new Request("https://example.test/iceberg"), {
      DATA: bucket,
    });
    expect(icebergResponse.status).toBe(200);
    await expect(icebergResponse.json()).resolves.toMatchObject({
      snapshotId: 2,
      filesPlanned: 2,
      filesSkipped: 1,
      files: [{ partition: { country: "US" } }, { partition: { country: "US" } }],
    });

    await expect(
      worker.fetch(new Request("https://example.test/missing"), { DATA: bucket }),
    ).resolves.toMatchObject({ status: 404 });
  });
});

function bucketStore(bucket: WorkerR2Bucket) {
  return {
    get: (path: string) =>
      bucket.get(path).then(async (object) => {
        if (object === null) return null;
        return new Uint8Array(await object.arrayBuffer());
      }),
    getRange: (path: string, range: { offset: number; length: number }) =>
      bucket.get(path, { range }).then(async (object) => {
        if (object === null) throw new Error(`No object at ${path}`);
        return new Uint8Array(await object.arrayBuffer());
      }),
    put: (path: string, body: Uint8Array | ReadableStream<Uint8Array>) => bucket.put(path, body),
    delete: (path: string) => bucket.delete(path),
    list: async function* (prefix: string) {
      const result = await bucket.list({ prefix });
      for (const object of result.objects) yield { path: object.key, size: object.size };
    },
    head: (path: string) =>
      bucket.head(path).then((object) => (object === null ? null : { size: object.size })),
  };
}

function inlineIcebergMetadata() {
  return {
    "format-version": 2,
    "table-uuid": "worker-example",
    location: "iceberg/warehouse/places",
    "current-snapshot-id": 2,
    refs: {
      main: { type: "branch", "snapshot-id": 2 },
    },
    schemas: [
      {
        "schema-id": 1,
        fields: [
          { id: 1, name: "id", type: "int", required: true },
          { id: 2, name: "country", type: "string", required: false },
        ],
      },
    ],
    snapshots: [
      {
        "snapshot-id": 2,
        "timestamp-ms": 1,
        "schema-id": 1,
        manifests: [
          {
            path: "iceberg/warehouse/places/metadata/manifest-inline.json",
            files: [
              {
                path: "iceberg/warehouse/places/data/country=US/part-000.parquet",
                sequenceNumber: 1,
                partition: { country: "US" },
                recordCount: 2,
              },
              {
                path: "iceberg/warehouse/places/data/country=US/part-001.parquet",
                sequenceNumber: 2,
                partition: { country: "US" },
                recordCount: 3,
              },
              {
                path: "iceberg/warehouse/places/data/country=CA/part-000.parquet",
                sequenceNumber: 3,
                partition: { country: "CA" },
                recordCount: 1,
              },
            ],
          },
        ],
      },
    ],
  };
}
