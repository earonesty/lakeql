import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LakeqlError } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { fsJsonCache } from "./index.js";

describe("fsJsonCache", () => {
  it("persists JSON cache entries under a filesystem prefix", async () => {
    const root = await mkdtemp(join(tmpdir(), "lakeql-fs-cache-"));
    try {
      const cache = fsJsonCache<{ files: number }>({ root, prefix: "catalog/cache" });

      await cache.set("iceberg:manifest:path", { value: { files: 3 } });

      expect(await cache.get("iceberg:manifest:path")).toEqual({ value: { files: 3 } });
      await expect(
        readFile(join(root, "catalog/cache/iceberg%3Amanifest%3Apath.json"), "utf8"),
      ).resolves.toContain('"files":3');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("expires entries using cache ttl", async () => {
    const root = await mkdtemp(join(tmpdir(), "lakeql-fs-cache-"));
    let now = 100;
    try {
      const cache = fsJsonCache<string>({
        root,
        ttlMs: 50,
        now: () => now,
      });

      await cache.set("key", { value: "value" });
      expect(await cache.get("key")).toEqual({ value: "value", expiresAt: 150 });

      now = 151;
      expect(await cache.get("key")).toBeUndefined();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("honors per-entry expiration and deletes entries", async () => {
    const root = await mkdtemp(join(tmpdir(), "lakeql-fs-cache-"));
    let now = 100;
    try {
      const cache = fsJsonCache<string>({
        root,
        ttlMs: 50,
        now: () => now,
      });

      await cache.set("key", { value: "value", expiresAt: 300 });
      now = 151;
      expect(await cache.get("key")).toEqual({ value: "value", expiresAt: 300 });

      await cache.delete("key");
      expect(await cache.get("key")).toBeUndefined();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("formats JSON when spacing is configured", async () => {
    const root = await mkdtemp(join(tmpdir(), "lakeql-fs-cache-"));
    try {
      const cache = fsJsonCache<{ files: number }>({ root, space: 2 });

      await cache.set("key", { value: { files: 3 } });

      await expect(readFile(join(root, "lakeql/key.json"), "utf8")).resolves.toContain(
        '\n  "value"',
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("rejects invalid prefixes and keys", async () => {
    expect(() => fsJsonCache({ root: "/tmp", prefix: "/" })).toThrow(LakeqlError);
    expect(() => fsJsonCache({ root: "/tmp", prefix: "../cache" })).toThrow(LakeqlError);

    const root = await mkdtemp(join(tmpdir(), "lakeql-fs-cache-"));
    try {
      await expect(fsJsonCache({ root }).set("", { value: "value" })).rejects.toThrow(LakeqlError);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("rejects invalid stored entries and unserializable values", async () => {
    const root = await mkdtemp(join(tmpdir(), "lakeql-fs-cache-"));
    try {
      const cache = fsJsonCache<unknown>({ root });
      await mkdir(join(root, "lakeql"), { recursive: true });
      await writeFile(join(root, "lakeql/not-entry.json"), "[]");

      await expect(cache.get("not-entry")).rejects.toThrow(LakeqlError);
      await expect(cache.set("undefined", { value: undefined })).rejects.toThrow(LakeqlError);

      const circular: Record<string, unknown> = {};
      circular.self = circular;
      await expect(cache.set("circular", { value: circular })).rejects.toThrow(LakeqlError);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
