import { describe, expect, it } from "vitest";
import { memoryCache, type RuntimeSubstrate } from "./runtime.js";

describe("runtime substrate helpers", () => {
  it("stores, expires, and deletes memory cache entries", async () => {
    const cache = memoryCache<string>();
    await cache.set("fresh", { value: "ok", expiresAt: Date.now() + 10_000 });
    await cache.set("expired", { value: "old", expiresAt: Date.now() - 1 });

    await expect(cache.get("fresh")).resolves.toEqual({
      value: "ok",
      expiresAt: expect.any(Number),
    });
    await expect(cache.get("expired")).resolves.toBeUndefined();
    await cache.delete("fresh");
    await expect(cache.get("fresh")).resolves.toBeUndefined();
  });

  it("accepts caller-supplied substrate interfaces", async () => {
    const substrate: RuntimeSubstrate = {
      clock: { now: () => 1 },
      ids: { id: (prefix = "id") => `${prefix}-1` },
      lock: { withLock: async (_key, fn) => fn() },
      metrics: { count() {}, timing() {} },
      log: { debug() {}, info() {}, warn() {}, error() {} },
    };

    await expect(substrate.lock?.withLock("x", async () => substrate.ids?.id("job"))).resolves.toBe(
      "job-1",
    );
  });
});
