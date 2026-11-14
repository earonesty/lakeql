import { createServer } from "node:http";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { S3ObjectStore, s3JsonCache, s3Store, signS3Request } from "./index.js";

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

  it("matches AWS S3 SigV4 documentation vectors with explicit payload hashes", async () => {
    const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const vectors: {
      name: string;
      method: string;
      url: string;
      headers: Record<string, string>;
      body?: BodyInit;
      signedHeaders: string;
      signature: string;
    }[] = [
      {
        name: "PUT object",
        method: "PUT",
        url: "https://examplebucket.s3.amazonaws.com/test%24file.text",
        headers: {
          Date: "Fri, 24 May 2013 00:00:00 GMT",
          "x-amz-content-sha256":
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
          "x-amz-storage-class": "REDUCED_REDUNDANCY",
        },
        body: "Welcome to Amazon S3.",
        signedHeaders: "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
        signature: "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
      },
      {
        name: "GET bucket lifecycle",
        method: "GET",
        url: "https://examplebucket.s3.amazonaws.com/?lifecycle=",
        headers: { "x-amz-content-sha256": emptyHash },
        signedHeaders: "host;x-amz-content-sha256;x-amz-date",
        signature: "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
      },
      {
        name: "GET bucket list objects",
        method: "GET",
        url: "https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J",
        headers: { "x-amz-content-sha256": emptyHash },
        signedHeaders: "host;x-amz-content-sha256;x-amz-date",
        signature: "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
      },
    ];

    for (const vector of vectors) {
      const headers = await signS3Request(
        {
          method: vector.method,
          url: new URL(vector.url),
          headers: vector.headers,
          region: "us-east-1",
          accessKeyId: "AKIAIOSFODNN7EXAMPLE",
          secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          now: new Date("2013-05-24T00:00:00Z"),
        },
        undefined,
        vector.body,
      );
      const authorization = headers.get("authorization") ?? "";

      expect(headers.get("x-amz-date"), vector.name).toBe("20130524T000000Z");
      expect(headers.get("x-amz-content-sha256"), vector.name).toBe(
        vector.headers["x-amz-content-sha256"],
      );
      expect(authorization, vector.name).toContain(
        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
      );
      expect(authorization, vector.name).toContain(`SignedHeaders=${vector.signedHeaders}`);
      expect(authorization, vector.name).toContain(`Signature=${vector.signature}`);
    }
  });
});

describe("s3Store", () => {
  it("persists JSON cache entries through signed S3 requests", async () => {
    const objects = new Map<string, Uint8Array>();
    const cache = s3JsonCache<{ rows: number }>({
      ...options((async (input, init) => {
        const url = new URL(String(input));
        const key = url.pathname;
        const method = init?.method ?? "GET";
        if (method === "GET") {
          const bytes = objects.get(key);
          return bytes === undefined ? new Response(null, { status: 404 }) : new Response(bytes);
        }
        if (method === "PUT") {
          const body = init?.body;
          const bytes =
            body instanceof ArrayBuffer
              ? new Uint8Array(body)
              : body instanceof Uint8Array
                ? body
                : enc.encode(String(body ?? ""));
          objects.set(key, bytes);
          return new Response(null);
        }
        if (method === "DELETE") {
          objects.delete(key);
          return new Response(null);
        }
        return new Response(null, { status: 405 });
      }) as typeof fetch),
      prefix: "cache",
    });

    await cache.set("iceberg:manifest:path", { value: { rows: 20 } });

    expect(await cache.get("iceberg:manifest:path")).toEqual({ value: { rows: 20 } });
    expect(objects.size).toBe(1);
    await cache.delete("iceberg:manifest:path");
    expect(await cache.get("iceberg:manifest:path")).toBeUndefined();
  });

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
        if (url.includes("continuation-token=page-2")) {
          return new Response(
            "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>c.txt</Key><Size>2</Size></Contents></ListBucketResult>",
          );
        }
        if (url.includes("list-type=2")) {
          return new Response(
            "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken><Contents><Key>a&amp;b.txt</Key><Size>1</Size><ETag>&quot;e&quot;</ETag><LastModified>2026-06-13T00:00:00.000Z</LastModified></Contents><Contents><Key>skip</Key><Size>nan</Size></Contents></ListBucketResult>",
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
      {
        path: "c.txt",
        size: 2,
      },
    ]);
    expect(seen.every((entry) => entry.auth?.startsWith("AWS4-HMAC-SHA256"))).toBe(true);
    expect(seen.map((entry) => entry.range)).toContain("bytes=1-2");
    expect(seen.some((entry) => entry.url.includes("continuation-token=page-2"))).toBe(true);

    const limited = [];
    for await (const object of store.list("a", { limit: 1 })) limited.push(object);
    expect(limited.map((object) => object.path)).toEqual(["a&b.txt"]);
  });

  it("returns null for missing get/head", async () => {
    const store = s3Store(options(async () => new Response(null, { status: 404 })) as typeof fetch);
    await expect(store.get("missing")).resolves.toBeNull();
    await expect(store.head("missing")).resolves.toBeNull();
    await expect(store.getRange("missing", { offset: 0, length: 1 })).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });
  });

  it("throws typed errors for malformed head and failed requests", async () => {
    const noLength = s3Store(
      options(async () => new Response(null, { status: 200 })) as typeof fetch,
    );
    await expect(noLength.head("key")).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });

    const failed = s3Store(
      options(async () => new Response(null, { status: 500 })) as typeof fetch,
    );
    await expect(failed.delete("key")).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
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

  it("refreshes provider credentials when the active session reaches its refresh window", async () => {
    const seen: { credential: string; token: string | null }[] = [];
    let now = new Date("2026-06-13T00:00:00Z");
    let providerCalls = 0;
    const store = s3Store({
      endpoint: "https://s3.example.test",
      bucket: "bucket",
      region: "us-east-1",
      now: () => now,
      credentialRefreshWindowMs: 60_000,
      credentials: () => {
        providerCalls += 1;
        return {
          accessKeyId: `AKID${providerCalls}`,
          secretAccessKey: `SECRET${providerCalls}`,
          sessionToken: `TOKEN${providerCalls}`,
          expiresAt: new Date(now.getTime() + (providerCalls === 1 ? 120_000 : 600_000)),
        };
      },
      fetch: (async (_input, init) => {
        const headers = new Headers(init?.headers);
        const authorization = headers.get("authorization") ?? "";
        seen.push({
          credential: authorization.match(/Credential=([^/]+)/u)?.[1] ?? "",
          token: headers.get("x-amz-security-token"),
        });
        return new Response(enc.encode("ok"));
      }) as typeof fetch,
    });

    await store.get("first");
    now = new Date("2026-06-13T00:00:30Z");
    await store.get("still-valid");
    now = new Date("2026-06-13T00:01:01Z");
    await store.get("refresh");

    expect(providerCalls).toBe(2);
    expect(seen).toEqual([
      { credential: "AKID1", token: "TOKEN1" },
      { credential: "AKID1", token: "TOKEN1" },
      { credential: "AKID2", token: "TOKEN2" },
    ]);
  });

  it("retries credential providers after a refresh error", async () => {
    let providerCalls = 0;
    const store = s3Store({
      endpoint: "https://s3.example.test",
      bucket: "bucket",
      region: "us-east-1",
      credentials: () => {
        providerCalls += 1;
        if (providerCalls === 1) throw new Error("catalog unavailable");
        return {
          accessKeyId: "AKID",
          secretAccessKey: "SECRET",
        };
      },
      fetch: (async () => new Response(enc.encode("ok"))) as typeof fetch,
    });

    await expect(store.get("first")).rejects.toThrow("catalog unavailable");
    await expect(store.get("second")).resolves.toEqual(enc.encode("ok"));
    expect(providerCalls).toBe(2);
  });

  it("supports conditional puts with S3 precondition headers", async () => {
    const seen: {
      ifMatch: string | null;
      ifNoneMatch: string | null;
      contentType: string | null;
    }[] = [];
    const store = s3Store(
      options(async (_input, init) => {
        const headers = new Headers(init?.headers);
        seen.push({
          ifMatch: headers.get("if-match"),
          ifNoneMatch: headers.get("if-none-match"),
          contentType: headers.get("content-type"),
        });
        if (headers.get("if-match") === '"stale"') return new Response(null, { status: 412 });
        if (headers.get("if-none-match") === "*" && seen.length > 2) {
          return new Response(null, { status: 409 });
        }
        return new Response(null, { status: 200 });
      }) as typeof fetch,
    );

    await expect(
      store.conditionalPut("new", enc.encode("x"), {
        expectedEtag: null,
        contentType: "text/plain",
      }),
    ).resolves.toBe(true);
    await expect(
      store.conditionalPut("existing", enc.encode("x"), { expectedEtag: '"abc"' }),
    ).resolves.toBe(true);
    await expect(
      store.conditionalPut("stale", enc.encode("x"), { expectedEtag: '"stale"' }),
    ).resolves.toBe(false);
    await expect(
      store.conditionalPut("conflict", enc.encode("x"), { expectedEtag: null }),
    ).resolves.toBe(false);

    expect(seen).toEqual([
      { ifMatch: null, ifNoneMatch: "*", contentType: "text/plain" },
      { ifMatch: '"abc"', ifNoneMatch: null, contentType: null },
      { ifMatch: '"stale"', ifNoneMatch: null, contentType: null },
      { ifMatch: null, ifNoneMatch: "*", contentType: null },
    ]);
  });

  it("encodes keys and rejects path escapes before signing", async () => {
    const seen: string[] = [];
    const store = s3Store(
      options(async (input) => {
        seen.push(String(input));
        return new Response(enc.encode("ok"));
      }) as typeof fetch,
    );

    await store.get("dir/file name?#.parquet");
    expect(seen).toEqual(["https://s3.example.test/bucket/dir/file%20name%3F%23.parquet"]);
    await expect(store.get("../../other-bucket/key")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("%2e%2e/key")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("/bucket/key")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("https://evil.test/key")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    expect(seen).toHaveLength(1);
  });
});

describe("owned S3 ranges", () => {
  it("reads a signed range over real HTTP with exact allocation admission", async () => {
    const fixture = Uint8Array.from({ length: 4096 }, (_, i) => i % 251);
    const requests: string[] = [];
    const server = createServer((request, response) => {
      requests.push(request.headers.range ?? "");
      if (!request.headers.authorization?.startsWith("AWS4-HMAC-SHA256")) {
        response.writeHead(403).end();
        return;
      }
      const match = /^bytes=(\d+)-(\d+)$/u.exec(request.headers.range ?? "");
      if (!match) {
        response.writeHead(400).end();
        return;
      }
      const start = Number(match[1]);
      const end = Math.min(Number(match[2]), fixture.length - 1);
      response.writeHead(206, {
        "content-range": `bytes ${start}-${end}/${fixture.length}`,
        "content-length": end - start + 1,
      });
      response.end(fixture.subarray(start, end + 1));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const store = new S3ObjectStore({
      ...options(fetch),
      endpoint: `http://127.0.0.1:${address.port}`,
    });
    const scope = new MemoryScope(new MemoryBudget(128));
    try {
      const output = await store.getRangeOwned("fixture", { offset: 1000, length: 128 }, scope);
      expect([...output.value]).toEqual([...fixture.slice(1000, 1128)]);
      expect(scope.budget.usedBytes).toBe(128);
      output.release();
      const tail = await store.getRangeOwned("fixture", { offset: 4090, length: 128 }, scope);
      expect([...tail.value]).toEqual([...fixture.slice(4090)]);
      expect(tail.value.byteOffset).toBe(0);
      expect(tail.value.buffer.byteLength).toBe(6);
      expect(store.getRangeOwnedAllocation("fixture", { offset: 4090, length: 128 })).toEqual({
        bytes: 128,
        output: "exact-buffer",
      });
      expect(scope.budget.usedBytes).toBe(6);
      tail.release();
      const tight = new MemoryScope(new MemoryBudget(127));
      await expect(
        store.getRangeOwned("fixture", { offset: 1000, length: 128 }, tight),
      ).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
      expect(tight.budget.peakBytes).toBe(0);
      tight.dispose();
      expect(requests).toEqual(["bytes=1000-1127", "bytes=4090-4217"]);
    } finally {
      scope.dispose();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
    expect(scope.budget.usedBytes).toBe(0);
  });

  it.each([
    { status: 200, contentRange: "bytes 0-3/4", bytes: 4 },
    { status: 206, contentRange: "bytes 1-4/5", bytes: 4 },
    { status: 206, contentRange: "bytes 0-3/4", bytes: 3 },
    { status: 206, contentRange: "bytes 0-3/4", bytes: 5 },
  ])("rejects incorrect or oversized responses: %j", async ({ status, contentRange, bytes }) => {
    const store = new S3ObjectStore(
      options(
        async () =>
          new Response(new Uint8Array(bytes), {
            status,
            headers: { "content-range": contentRange },
          }),
      ),
    );
    const scope = new MemoryScope(new MemoryBudget(4));
    await expect(
      store.getRangeOwned("fixture", { offset: 0, length: 4 }, scope),
    ).rejects.toBeDefined();
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  });

  it("propagates scope cancellation into the signed fetch", async () => {
    const scope = new MemoryScope(new MemoryBudget(4));
    const store = new S3ObjectStore(
      options(async (_url, init) => {
        expect(init?.signal).toBe(scope.signal);
        scope.dispose();
        throw new Error("transport aborted");
      }),
    );
    await expect(
      store.getRangeOwned("fixture", { offset: 0, length: 4 }, scope),
    ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(scope.budget.usedBytes).toBe(0);
  });
});
