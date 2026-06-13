import { LaQLError } from "@laql/core";
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
        if (method === "HEAD") {
          return new Response(null, {
            headers: {
              "content-length": "5",
              etag: "v1",
              "last-modified": "Sat, 13 Jun 2026 00:00:00 GMT",
              "content-type": "application/octet-stream",
            },
          });
        }
        return new Response(enc.encode(headers.get("range") ?? "whole"), {
          status: headers.has("range") ? 206 : 200,
        });
      },
    });

    await expect(store.head("file.parquet")).resolves.toMatchObject({
      size: 5,
      etag: "v1",
      contentType: "application/octet-stream",
    });
    await expect(store.get("file.parquet")).resolves.toEqual(enc.encode("whole"));
    await expect(store.getRange("file.parquet", { offset: 2, length: 4 })).resolves.toEqual(
      enc.encode("bytes=2-5"),
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
      expect.arrayContaining(["HEAD", "GET", "PUT", "DELETE"]),
    );
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
    ).rejects.toBeInstanceOf(LaQLError);
    await expect(
      store.getRange("missing.parquet", { offset: -1, length: 1 }),
    ).rejects.toMatchObject({ code: "LAQL_OBJECT_NOT_FOUND" });
    await expect(async () => {
      for await (const _object of store.list("")) {
        // unreachable
      }
    }).rejects.toMatchObject({ code: "LAQL_UNSUPPORTED_PUSHDOWN" });
  });

  it("throws typed errors for bad head metadata and failed responses", async () => {
    const noLength = httpStore({
      baseUrl: "https://example.test/",
      fetch: async () => new Response(null, { status: 200 }),
    });
    await expect(noLength.head("x")).rejects.toMatchObject({ code: "LAQL_OBJECT_NOT_FOUND" });

    const failed = httpStore({
      baseUrl: "https://example.test/",
      fetch: async () => new Response(null, { status: 500 }),
    });
    await expect(failed.get("x")).rejects.toMatchObject({ code: "LAQL_OBJECT_NOT_FOUND" });
  });
});
