import { describe, expect, it } from "vitest";
import { s3Store, signS3Request } from "./index.js";

const enc = new TextEncoder();

function options(fetchImpl: typeof fetch, sessionToken?: string) {
  return {
    endpoint: "https://s3.example.test",
    bucket: "bucket",
    region: "us-east-1",
    accessKeyId: "AKID",
    secretAccessKey: "SECRET",
    sessionToken,
    now: () => new Date("2026-06-13T00:00:00Z"),
    fetch: fetchImpl,
  };
}

describe("signS3Request", () => {
  it("adds SigV4 authorization headers", async () => {
    const headers = await signS3Request({
      method: "GET",
      url: new URL("https://s3.example.test/bucket/key?prefix=a"),
      headers: {},
      region: "us-east-1",
      accessKeyId: "AKID",
      secretAccessKey: "SECRET",
      now: new Date("2026-06-13T00:00:00Z"),
    });

    expect(headers.get("x-amz-date")).toBe("20260613T000000Z");
    expect(headers.get("x-amz-content-sha256")).toBe("UNSIGNED-PAYLOAD");
    expect(headers.get("authorization")).toContain("AWS4-HMAC-SHA256 Credential=AKID/");
  });

  it("includes a session token when provided", async () => {
    const headers = await signS3Request({
      method: "GET",
      url: new URL("https://s3.example.test/bucket/key"),
      headers: {},
      region: "us-east-1",
      accessKeyId: "AKID",
      secretAccessKey: "SECRET",
      sessionToken: "TOKEN",
      now: new Date("2026-06-13T00:00:00Z"),
    });

    expect(headers.get("x-amz-security-token")).toBe("TOKEN");
    expect(headers.get("authorization")).toContain("x-amz-security-token");
  });
});

describe("s3Store", () => {
  it("performs signed head/get/range/list/put/delete requests", async () => {
    const seen: { url: string; method: string; range: string | null; auth: string | null }[] = [];
    const store = s3Store(
      options(async (input, init) => {
        const headers = new Headers(init?.headers);
        const url = String(input);
        const method = init?.method ?? "GET";
        seen.push({
          url,
          method,
          range: headers.get("range"),
          auth: headers.get("authorization"),
        });
        if (method === "HEAD") {
          return new Response(null, {
            headers: {
              "content-length": "6",
              etag: '"abc"',
              "content-type": "text/plain",
              "last-modified": "Sat, 13 Jun 2026 00:00:00 GMT",
            },
          });
        }
        if (url.includes("list-type=2")) {
          return new Response(
            "<ListBucketResult><Contents><Key>a&amp;b.txt</Key><Size>1</Size><ETag>&quot;e&quot;</ETag><LastModified>2026-06-13T00:00:00.000Z</LastModified></Contents><Contents><Key>skip</Key><Size>nan</Size></Contents></ListBucketResult>",
          );
        }
        return new Response(enc.encode(headers.get("range") ?? "body"), {
          status: headers.has("range") ? 206 : 200,
        });
      }) as typeof fetch,
    );

    await expect(store.head("key")).resolves.toMatchObject({ size: 6, etag: '"abc"' });
    await expect(store.get("key")).resolves.toEqual(enc.encode("body"));
    await expect(store.getRange("key", { offset: 1, length: 2 })).resolves.toEqual(
      enc.encode("bytes=1-2"),
    );
    await store.put("key", enc.encode("x"), { contentType: "text/plain" });
    await store.delete("key");

    const listed = [];
    for await (const object of store.list("a")) listed.push(object);
    expect(listed).toEqual([
      {
        path: "a&b.txt",
        size: 1,
        etag: '"e"',
        lastModified: new Date("2026-06-13T00:00:00.000Z"),
      },
    ]);
    expect(seen.every((entry) => entry.auth?.startsWith("AWS4-HMAC-SHA256"))).toBe(true);
    expect(seen.map((entry) => entry.range)).toContain("bytes=1-2");
  });

  it("returns null for missing get/head", async () => {
    const store = s3Store(options(async () => new Response(null, { status: 404 })) as typeof fetch);
    await expect(store.get("missing")).resolves.toBeNull();
    await expect(store.head("missing")).resolves.toBeNull();
    await expect(store.getRange("missing", { offset: 0, length: 1 })).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });
  });

  it("throws typed errors for malformed head and failed requests", async () => {
    const noLength = s3Store(
      options(async () => new Response(null, { status: 200 })) as typeof fetch,
    );
    await expect(noLength.head("key")).rejects.toMatchObject({ code: "LAQL_OBJECT_NOT_FOUND" });

    const failed = s3Store(
      options(async () => new Response(null, { status: 500 })) as typeof fetch,
    );
    await expect(failed.delete("key")).rejects.toMatchObject({ code: "LAQL_OBJECT_NOT_FOUND" });
  });

  it("passes session tokens and stream bodies through signed requests", async () => {
    const seen: { token: string | null; hasBody: boolean }[] = [];
    const store = s3Store(
      options(async (_input, init) => {
        const headers = new Headers(init?.headers);
        seen.push({
          token: headers.get("x-amz-security-token"),
          hasBody: init?.body !== undefined,
        });
        return new Response(null, { status: 200 });
      }, "TOKEN") as typeof fetch,
    );

    await store.put(
      "stream",
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(enc.encode("x"));
          controller.close();
        },
      }),
    );

    expect(seen).toEqual([{ token: "TOKEN", hasBody: true }]);
  });
});
