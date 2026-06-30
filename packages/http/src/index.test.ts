import { LakeqlError } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { httpStore } from "./index.js";

const enc = new TextEncoder();

describe("httpStore", () => {
  it("constructs with default fetch and normalizes base URLs", () => {
    expect(httpStore({ baseUrl: "https://example.test/data" })).toBeTruthy();
  });

  it("reads head, full body, ranges, and indexed lists", async () => {
    const seen: { url: string; method: string; range: string | null }[] = [];
    const store = httpStore({
      baseUrl: "https://example.test/data/",
      objects: [
        { path: "a.parquet", size: 10, etag: "a" },
        { path: "nested/b.parquet", size: 20, etag: "b" },
      ],
      fetch: async (input, init) => {
        const url = String(input);
        const method = init?.method ?? "GET";
        const headers = new Headers(init?.headers);
        seen.push({ url, method, range: headers.get("range") });
        if (url.endsWith("missing.parquet")) return new Response(null, { status: 404 });
        const meta = {
          etag: "v1",
          "last-modified": "Sat, 13 Jun 2026 00:00:00 GMT",
          "content-type": "application/octet-stream",
        };
        const range = headers.get("range");
        if (range) {
          // Real servers answer a ranged GET with 206 + content-range total.
          return new Response(enc.encode(range), {
            status: 206,
            headers: { ...meta, "content-range": "bytes 0-0/5", "content-length": "5" },
          });
        }
        return new Response(enc.encode("whole"), { status: 200, headers: meta });
      },
    });

    await expect(store.head("file.parquet")).resolves.toMatchObject({
      size: 5,
      etag: "v1",
      contentType: "application/octet-stream",
    });
    await expect(store.getRange("file.parquet", { offset: 2, length: 4 })).resolves.toEqual(
      enc.encode("bytes=2-5"),
    );
    await expect(store.get("file.parquet")).resolves.toEqual(enc.encode("whole"));
    await expect(store.getRange("file.parquet", { offset: 2, length: 4 })).resolves.toEqual(
      enc.encode("ole"),
    );
    const listed = [];
    for await (const object of store.list("nested/")) listed.push(object.path);
    expect(listed).toEqual(["nested/b.parquet"]);
    const limited = [];
    for await (const object of store.list("", { limit: 1 })) limited.push(object.path);
    expect(limited).toEqual(["a.parquet"]);
    expect(seen.map((entry) => entry.range)).toContain("bytes=2-5");
    await store.put("new.parquet", enc.encode("x"), { contentType: "application/octet-stream" });
    await store.put(
      "stream.parquet",
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(enc.encode("x"));
          controller.close();
        },
      }),
    );
    await store.delete("new.parquet");
    expect(seen.map((entry) => entry.method)).toEqual(
      expect.arrayContaining(["GET", "PUT", "DELETE"]),
    );
    // head() probes with a tiny ranged GET, not HEAD.
    expect(seen.some((entry) => entry.range === "bytes=0-1")).toBe(true);
  });

  it("returns null for 404 reads and throws when list has no object index", async () => {
    const store = httpStore({
      baseUrl: "https://example.test/",
      fetch: async () => new Response(null, { status: 404 }),
    });

    await expect(store.get("missing.parquet")).resolves.toBeNull();
    await expect(store.head("missing.parquet")).resolves.toBeNull();
    await expect(
      store.getRange("missing.parquet", { offset: 0, length: 1 }),
    ).rejects.toBeInstanceOf(LakeqlError);
    await expect(
      store.getRange("missing.parquet", { offset: -1, length: 1 }),
    ).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
    await expect(async () => {
      for await (const _object of store.list("")) {
        // unreachable
      }
    }).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_PUSHDOWN" });
  });

  it("throws typed errors for bad head metadata and failed responses", async () => {
    const noLength = httpStore({
      baseUrl: "https://example.test/",
      fetch: async () => new Response(null, { status: 200 }),
    });
    await expect(noLength.head("x")).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });

    const failed = httpStore({
      baseUrl: "https://example.test/",
      fetch: async () => new Response(null, { status: 500 }),
    });
    await expect(failed.get("x")).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
  });

  it("keeps object paths inside the configured base URL", async () => {
    const seen: string[] = [];
    const store = httpStore({
      baseUrl: "https://example.test/data/prefix/",
      headers: { authorization: "Bearer secret" },
      fetch: async (input) => {
        seen.push(String(input));
        return new Response(enc.encode("ok"));
      },
    });

    await store.get("dir/file name?#.parquet");
    expect(seen).toEqual(["https://example.test/data/prefix/dir/file%20name%3F%23.parquet"]);
    await expect(store.get("https://evil.test/file")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("//evil.test/file")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("../secret")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("%2e%2e/secret")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    expect(seen).toHaveLength(1);
  });

  it("slices client-side when a server ignores Range and returns 200 with the full body", async () => {
    const whole = enc.encode("0123456789");
    const store = httpStore({
      baseUrl: "https://example.test/data/",
      // Simulates GitHub Pages and similar hosts that treat Range as advisory.
      fetch: async () => new Response(whole, { status: 200 }),
    });
    await expect(store.getRange("file.parquet", { offset: 2, length: 4 })).resolves.toEqual(
      enc.encode("2345"),
    );
  });

  it("falls back to full-object slicing when static hosts range over compressed bytes", async () => {
    const whole = enc.encode("PAR1decoded-parquet-body");
    const seen: { range: string | null }[] = [];
    const store = httpStore({
      baseUrl: "https://example.test/data/",
      fetch: async (_input, init) => {
        const headers = new Headers(init?.headers);
        const range = headers.get("range");
        seen.push({ range });
        if (range === "bytes=0-1") {
          return new Response(new Uint8Array([0x1f, 0x8b]), {
            status: 206,
            headers: {
              "content-range": "bytes 0-1/11",
              "content-length": "2",
              vary: "Accept-Encoding",
            },
          });
        }
        if (range) {
          return new Response(null, {
            status: 416,
            headers: { "content-range": "bytes */11" },
          });
        }
        return new Response(whole, {
          status: 200,
          headers: { "content-length": String(whole.byteLength) },
        });
      },
    });

    await expect(store.head("file.parquet")).resolves.toMatchObject({ size: whole.byteLength });
    await expect(store.getRange("file.parquet", { offset: 4, length: 7 })).resolves.toEqual(
      enc.encode("decoded"),
    );
    expect(seen.map((entry) => entry.range)).toEqual(["bytes=0-1", null]);
  });

  it("falls back when compressed ranged probes expose no decoded bytes", async () => {
    const whole = enc.encode("PAR1decoded-parquet-body");
    const store = httpStore({
      baseUrl: "https://example.test/data/",
      fetch: async (_input, init) => {
        const range = new Headers(init?.headers).get("range");
        if (range === "bytes=0-1") {
          return new Response(new Uint8Array(), {
            status: 206,
            headers: {
              "content-range": "bytes 0-1/11",
              vary: "Accept-Encoding",
            },
          });
        }
        return new Response(whole);
      },
    });

    await expect(store.head("file.parquet")).resolves.toMatchObject({ size: whole.byteLength });
    await expect(store.getRange("file.parquet", { offset: 0, length: 4 })).resolves.toEqual(
      enc.encode("PAR1"),
    );
  });

  it("falls back when ranged probes carry content-encoding", async () => {
    const whole = enc.encode("PAR1decoded-parquet-body");
    const store = httpStore({
      baseUrl: "https://example.test/data/",
      fetch: async (_input, init) => {
        const range = new Headers(init?.headers).get("range");
        if (range === "bytes=0-1") {
          return new Response(new Uint8Array([0x1f, 0x8b]), {
            status: 206,
            headers: {
              "content-encoding": "gzip",
              "content-range": "bytes 0-1/11",
            },
          });
        }
        return new Response(whole);
      },
    });

    await expect(store.head("file.parquet")).resolves.toMatchObject({ size: whole.byteLength });
  });
});
