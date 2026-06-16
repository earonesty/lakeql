import { describe, expect, it } from "vitest";
import { s3Store, signS3Request } from "./index.js";

const enc = new TextEncoder();

const endpoint = process.env.LAQL_MINIO_ENDPOINT;
const accessKeyId = process.env.LAQL_MINIO_ACCESS_KEY ?? "minioadmin";
const secretAccessKey = process.env.LAQL_MINIO_SECRET_KEY ?? "minioadmin";
const region = process.env.LAQL_MINIO_REGION ?? "us-east-1";
const bucket = process.env.LAQL_MINIO_BUCKET ?? "laql-provider";
const requireProviders = process.env.LAQL_REQUIRE_PROVIDERS === "1";

const describeMinio = endpoint === undefined ? describe.skip : describe;
const describeMissingMinio = endpoint === undefined && requireProviders ? describe : describe.skip;

describeMissingMinio("MinIO S3-compatible provider conformance", () => {
  it("requires MinIO provider environment", () => {
    throw new Error("LAQL_MINIO_ENDPOINT is required when LAQL_REQUIRE_PROVIDERS=1");
  });
});

describeMinio("MinIO S3-compatible provider conformance", () => {
  it("round-trips object-store operations through real S3 APIs", async () => {
    await ensureBucket();
    const store = s3Store({
      endpoint: endpoint as string,
      bucket,
      region,
      accessKeyId,
      secretAccessKey,
    });
    const prefix = `provider-${Date.now()}-${Math.random().toString(36).slice(2)}/`;

    await store.put(`${prefix}hello.txt`, enc.encode("hello world"), { contentType: "text/plain" });
    await expect(store.head(`${prefix}hello.txt`)).resolves.toMatchObject({
      size: 11,
      contentType: "text/plain",
    });
    await expect(store.get(`${prefix}hello.txt`)).resolves.toEqual(enc.encode("hello world"));
    await expect(store.getRange(`${prefix}hello.txt`, { offset: 6, length: 5 })).resolves.toEqual(
      enc.encode("world"),
    );

    await expect(
      store.conditionalPut(`${prefix}created.txt`, enc.encode("first"), { expectedEtag: null }),
    ).resolves.toBe(true);
    const created = await store.head(`${prefix}created.txt`);
    expect(created?.etag).toBeTruthy();
    await expect(
      store.conditionalPut(`${prefix}created.txt`, enc.encode("second"), { expectedEtag: null }),
    ).resolves.toBe(false);
    await expect(
      store.conditionalPut(`${prefix}created.txt`, enc.encode("second"), {
        expectedEtag: created?.etag ?? "",
      }),
    ).resolves.toBe(true);
    await expect(store.get(`${prefix}created.txt`)).resolves.toEqual(enc.encode("second"));

    for (let index = 0; index < 1005; index++) {
      await store.put(`${prefix}many/${String(index).padStart(4, "0")}.txt`, enc.encode("x"));
    }
    const listed = [];
    for await (const object of store.list(`${prefix}many/`)) listed.push(object.path);
    expect(listed).toHaveLength(1005);
    expect(listed.at(0)).toBe(`${prefix}many/0000.txt`);
    expect(listed.at(-1)).toBe(`${prefix}many/1004.txt`);

    const limited = [];
    for await (const object of store.list(`${prefix}many/`, { limit: 3 }))
      limited.push(object.path);
    expect(limited).toEqual([
      `${prefix}many/0000.txt`,
      `${prefix}many/0001.txt`,
      `${prefix}many/0002.txt`,
    ]);

    await store.delete(`${prefix}hello.txt`);
    await expect(store.get(`${prefix}hello.txt`)).resolves.toBeNull();
  });
});

async function ensureBucket(): Promise<void> {
  const url = new URL(`${bucket}/`, ensureSlash(endpoint as string));
  const headers = await signS3Request({
    method: "PUT",
    url,
    headers: {},
    region,
    accessKeyId,
    secretAccessKey,
    now: new Date(),
  });
  const response = await fetch(url, { method: "PUT", headers });
  if (response.ok || response.status === 409) return;
  throw new Error(
    `failed to create MinIO bucket ${bucket}: ${response.status} ${await response.text()}`,
  );
}

function ensureSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}
