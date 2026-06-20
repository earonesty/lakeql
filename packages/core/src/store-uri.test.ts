import { describe, expect, it } from "vitest";
import { memoryStore } from "./memory-store.js";
import { uriObjectStore } from "./store.js";

const enc = new TextEncoder();

describe("uriObjectStore", () => {
  it("maps object-store URI authorities onto configured stores", async () => {
    const backing = memoryStore();
    const store = uriObjectStore(backing, [
      { scheme: "s3", authority: "bucket" },
      { scheme: "r2", authority: "bucket" },
    ]);

    await store.put("s3://bucket/a/b.txt", enc.encode("one"));
    await store.put("r2://bucket/c.txt", enc.encode("two"));

    expect(await backing.get("a/b.txt")).toEqual(enc.encode("one"));
    expect(await store.get("a/b.txt")).toEqual(enc.encode("one"));
    expect(await store.get("s3://bucket/a/b.txt")).toEqual(enc.encode("one"));
    expect(await store.getRange("r2://bucket/c.txt", { offset: 0, length: 3 })).toEqual(
      enc.encode("two"),
    );
    expect(await store.head("s3://bucket/a/b.txt")).toMatchObject({ size: 3 });
  });

  it("maps URI prefixes to store-relative paths", async () => {
    const backing = memoryStore();
    const store = uriObjectStore(backing, {
      scheme: "gs",
      authority: "bucket",
      prefix: "warehouse/table",
    });

    await store.put("gs://bucket/warehouse/table/data/file.parquet", enc.encode("data"));
    const listed = [];
    for await (const object of store.list("gs://bucket/warehouse/table/data")) listed.push(object);

    expect(await backing.get("data/file.parquet")).toEqual(enc.encode("data"));
    expect(listed.map((object) => object.path)).toEqual(["data/file.parquet"]);
  });

  it("preserves conditional writes on capable stores", async () => {
    const backing = memoryStore();
    const store = uriObjectStore(backing, { scheme: "s3", authority: "bucket" });

    if (!("conditionalPut" in store)) throw new Error("conditionalPut was not preserved");

    await expect(
      store.conditionalPut("s3://bucket/cas", enc.encode("first"), { expectedEtag: null }),
    ).resolves.toBe(true);
    await expect(
      store.conditionalPut("s3://bucket/cas", enc.encode("second"), { expectedEtag: null }),
    ).resolves.toBe(false);
  });

  it("rejects absolute object URIs outside the configured store", async () => {
    const store = uriObjectStore(memoryStore(), { scheme: "s3", authority: "bucket" });

    await expect(store.get("s3://other/key")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("https://bucket/key")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
    await expect(store.get("s3://bucket/../other")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
  });

  it("rejects URIs outside a configured prefix", async () => {
    const store = uriObjectStore(memoryStore(), {
      scheme: "s3",
      authority: "bucket",
      prefix: "warehouse/table",
    });

    await expect(store.get("s3://bucket/warehouse/other/file.parquet")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
  });
});
