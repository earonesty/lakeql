import { MemoryBudget } from "lakeql";
import { expect, it } from "vitest";
import { PipelineCache } from "./pipeline-cache.js";

it("evicts by recency with a fixed entry bound", () => {
  const cache = new PipelineCache<number>(2, 65536);
  const first = Promise.resolve(1);
  cache.set("a", "source", first);
  cache.set("b", "source", Promise.resolve(2));
  expect(cache.get("a")).toBe(first);
  cache.set("c", "source", Promise.resolve(3));
  expect(cache.has("a")).toBe(true);
  expect(cache.has("b")).toBe(false);
  expect(cache.has("c")).toBe(true);
  for (let index = 0; index < 1000; index++)
    cache.set(String(index), "source", Promise.resolve(index));
  expect(cache.has("997")).toBe(false);
  expect(cache.has("998")).toBe(true);
  expect(cache.has("999")).toBe(true);
});

it("enforces modeled byte boundaries and skips oversized entries", () => {
  // 384 descriptor bytes plus two bytes per source/key code unit.
  const cache = new PipelineCache<number>(10, 388);
  cache.set("a", "b", Promise.resolve(1));
  expect(cache.has("a")).toBe(true);
  cache.set("c", "d", Promise.resolve(2));
  expect(cache.has("a")).toBe(false);
  cache.set("long", "source", Promise.resolve(3));
  expect(cache.has("long")).toBe(false);
  expect(cache.has("c")).toBe(true);
  cache.clear();
  cache.set("a", "b", Promise.resolve(4));
  expect(cache.has("a")).toBe(true);
  const short = new PipelineCache<number>(10, 387);
  short.set("a", "b", Promise.resolve(1));
  expect(short.has("a")).toBe(false);
});

it("does not let an evicted compilation failure delete its replacement", async () => {
  const cache = new PipelineCache<number>(1, 65536);
  let reject!: (error: Error) => void;
  const failing = new Promise<number>((_resolve, fail) => {
    reject = fail;
  });
  cache.set("a", "source", failing);
  cache.clear();
  const replacement = Promise.resolve(2);
  cache.set("a", "source", replacement);
  reject(new Error("old compilation"));
  await expect(failing).rejects.toThrow("old compilation");
  expect(cache.get("a")).toBe(replacement);
  cache.set("b", "source", Promise.reject(new Error("current compilation")));
  await Promise.resolve();
  expect(cache.has("b")).toBe(false);
});

it.each([
  [0, 1024],
  [10, 0],
])("can disable retention (%s, %s)", (entries, bytes) => {
  const cache = new PipelineCache<number>(entries, bytes);
  cache.set("a", "b", Promise.resolve(1));
  expect(cache.get("a")).toBeUndefined();
});

it.each([
  -1,
  NaN,
  Infinity,
  1.5,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid limits (%s)", (value) => {
  expect(() => new PipelineCache(value, 1024)).toThrow();
  expect(() => new PipelineCache(1, value)).toThrow();
});

it("charges the shared pool, reclaims entries for sibling jobs and detaches on clear", () => {
  const pool = new MemoryBudget(1024);
  const job = pool.child(512);
  const cache = new PipelineCache<number>(2, 1024);
  const value = Promise.resolve(1);
  cache.set("a", "b", value, job);
  expect(job.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(388);
  expect(cache.get("a", job)).toBe(value);
  expect(cache.get("a", new MemoryBudget(1024))).toBeUndefined();
  const sibling = pool.child(1024).reserve(1024, "sibling work");
  expect(cache.has("a")).toBe(false);
  expect(pool.usedBytes).toBe(1024);
  sibling.release();
  cache.set("a", "b", value, job);
  cache.clear();
  expect(pool.usedBytes).toBe(0);
  const all = pool.reserve(1024, "after cache clear");
  all.release();
});

it("declines optional retention when pinned work consumes the pool", () => {
  const pool = new MemoryBudget(388);
  const pinned = pool.reserve(1, "work");
  const cache = new PipelineCache<number>(2, 1024);
  cache.set("a", "b", Promise.resolve(1), pool);
  expect(cache.has("a")).toBe(false);
  expect(pool.usedBytes).toBe(1);
  pinned.release();
});

it("releases shared charges on replacement and compilation failure", async () => {
  const pool = new MemoryBudget(1024);
  const cache = new PipelineCache<number>(2, 1024);
  cache.set("a", "b", Promise.resolve(1), pool);
  cache.set("a", "b", Promise.reject(new Error("failed")), pool);
  expect(pool.usedBytes).toBe(388);
  await Promise.resolve();
  expect(pool.usedBytes).toBe(0);
});

it("keeps evicted in-flight metadata charged until its pin releases", () => {
  const pool = new MemoryBudget(1024);
  const job = pool.child(1024);
  const cache = new PipelineCache<number>(2, 1024);
  cache.set("a", "b", Promise.resolve(1), job);
  const pin = cache.pin("a", job);
  expect(job.usedBytes).toBe(388);
  cache.clear();
  expect(pool.usedBytes).toBe(388);
  expect(cache.has("a")).toBe(false);
  pin?.release();
  expect(pool.usedBytes).toBe(0);
  expect(job.usedBytes).toBe(0);
});

it("reclaims the least recently used entry in the requesting pool", () => {
  const pool = new MemoryBudget(1024);
  const cache = new PipelineCache<number>(3, 2048);
  cache.set("a", "b", Promise.resolve(1), pool);
  cache.set("c", "d", Promise.resolve(2), pool);
  cache.get("a", pool);
  const work = pool.reserve(300, "pressure");
  expect(cache.has("a")).toBe(true);
  expect(cache.has("c")).toBe(false);
  work.release();
  cache.clear();
  expect(pool.usedBytes).toBe(0);
});

it("retains active metadata in cache without reserving backing twice", () => {
  const pool = new MemoryBudget(388);
  const job = pool.child(388);
  const active = job.reserve(388, "active pipeline");
  const cache = new PipelineCache<number>(2, 1024);
  cache.set("a", "b", Promise.resolve(1), job, active);
  expect(pool.usedBytes).toBe(388);
  active.release();
  expect(job.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(388);
  cache.clear();
  expect(pool.usedBytes).toBe(0);
});
