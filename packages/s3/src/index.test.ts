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
      code: "LAQL_VALIDATION_ERROR",
    });
    await expect(store.get("%2e%2e/key")).rejects.toMatchObject({
      code: "LAQL_VALIDATION_ERROR",
    });
    await expect(store.get("/bucket/key")).rejects.toMatchObject({
      code: "LAQL_VALIDATION_ERROR",
    });
    await expect(store.get("https://evil.test/key")).rejects.toMatchObject({
      code: "LAQL_VALIDATION_ERROR",
    });
    expect(seen).toHaveLength(1);
  });
});
