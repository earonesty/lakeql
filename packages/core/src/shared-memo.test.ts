import { expect, it, vi } from "vitest";
import { SharedMemoryCache } from "./object-store-cache.js";

it("shares pending and completed computation with caller-selected capacity", async () => {
  const cache = new SharedMemoryCache({ maxBytes: 100 });
  let settle: (value: string) => void = () => {
    throw Error("not started");
  };
  const compute = vi.fn(
    () =>
      new Promise<string>((resolve) => {
        settle = resolve;
      }),
  );
  const first = cache.getOrCompute("key", compute, (text) => text.length);
  const second = cache.getOrCompute("key", compute, (text) => text.length);
  await Promise.resolve();
  expect(compute).toHaveBeenCalledTimes(1);
  settle("value");
  expect(await Promise.all([first, second])).toEqual(["value", "value"]);
  expect(await cache.getOrCompute("key", compute, (text) => text.length)).toBe("value");
  expect(compute).toHaveBeenCalledTimes(1);
  expect(cache.get("key")?.bytes).toBe(75);
});

it("does not retain oversized results and releases failed computations for retry", async () => {
  const cache = new SharedMemoryCache({ maxBytes: 100 });
  const compute = vi.fn(() => "x".repeat(101));
  for (let i = 0; i < 2; i++)
    expect(await cache.getOrCompute("k", compute, (text) => text.length)).toHaveLength(101);
  expect(compute).toHaveBeenCalledTimes(2);
  expect(cache.get("k")).toBeUndefined();
  const fail = vi.fn(() => {
    throw Error("failed");
  });
  await expect(cache.getOrCompute("k", fail, () => 0)).rejects.toThrow("failed");
  expect(
    await cache.getOrCompute(
      "k",
      () => 42,
      () => 8,
    ),
  ).toBe(42);
});

it("does not resurrect cleared or evicted pending entries", async () => {
  const cache = new SharedMemoryCache({ maxBytes: 100 });
  let settle: (value: string) => void = () => {};
  const pending = cache.getOrCompute(
    "k",
    () =>
      new Promise<string>((resolve) => {
        settle = resolve;
      }),
    () => 1,
  );
  await Promise.resolve();
  cache.deleteWhere(() => true);
  expect(
    await cache.getOrCompute(
      "k",
      () => "new",
      () => 3,
    ),
  ).toBe("new");
  settle("old");
  expect(await pending).toBe("old");
  expect(
    await cache.getOrCompute(
      "k",
      () => "wrong",
      () => 5,
    ),
  ).toBe("new");
});

it("expires by caller TTL and retains nothing when disabled", async () => {
  const now = vi.spyOn(Date, "now");
  try {
    now.mockReturnValue(100);
    const cache = new SharedMemoryCache({ maxBytes: 100, ttlMs: 5 });
    const compute = vi.fn(() => "value");
    await cache.getOrCompute("k", compute, () => 5);
    now.mockReturnValue(105);
    await cache.getOrCompute("k", compute, () => 5);
    expect(compute).toHaveBeenCalledTimes(2);
    const disabled = new SharedMemoryCache({ maxBytes: 0 });
    await disabled.getOrCompute("k", compute, () => 5);
    expect(disabled.get("k")).toBeUndefined();
  } finally {
    now.mockRestore();
  }
});

it("preserves cache priority and rejects invalid sizes", async () => {
  const cache = new SharedMemoryCache({ maxBytes: 100 });
  cache.set("pinned", "important", 100, { priority: 2 });
  expect(
    await cache.getOrCompute(
      "k",
      () => 42,
      () => 8,
    ),
  ).toBe(42);
  expect(cache.get("pinned")?.value).toBe("important");
  expect(cache.get("k")).toBeUndefined();
  expect(() => cache.set("invalid", 1, NaN)).toThrow(/byte size/);
  const empty = new SharedMemoryCache();
  await expect(
    empty.getOrCompute(
      "k",
      () => 1,
      () => -1,
    ),
  ).rejects.toThrow(/result size/);
  expect(empty.get("k")).toBeUndefined();
});
