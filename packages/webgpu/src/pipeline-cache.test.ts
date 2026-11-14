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
