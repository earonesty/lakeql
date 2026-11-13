import { describe, expect, it, vi } from "vitest";
import { mapConcurrent } from "./concurrent-map.js";

describe("bounded concurrent mapping", () => {
  it("starts only the configured window and preserves result order", async () => {
    const first = Promise.withResolvers<number>();
    const second = Promise.withResolvers<number>();
    const map = vi.fn(async (value: number) => {
      if (value === 0) return await first.promise;
      if (value === 1) return await second.promise;
      return value * 2;
    });
    const input = Array.from({ length: 10000 }, (_, index) => index);
    const result = mapConcurrent(input, 2, map);
    expect(map).toHaveBeenCalledTimes(2);
    second.resolve(2);
    await expect.poll(() => map.mock.calls.length).toBe(10000);
    first.resolve(0);
    expect(await result).toEqual(input.map((value) => value * 2));
  });

  it("stops admission on failure but waits for pending siblings", async () => {
    const pending = Promise.withResolvers<number>();
    const failure = Promise.withResolvers<number>();
    const map = vi.fn(
      async (value: number) => await (value === 0 ? failure.promise : pending.promise),
    );
    const result = mapConcurrent([0, 1, 2, 3], 2, map);
    let settled = false;
    const observed = result.catch((error: unknown) => {
      settled = true;
      return error;
    });
    const error = new Error("failed");
    failure.reject(error);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(settled).toBe(false);
    expect(map).toHaveBeenCalledTimes(2);
    pending.resolve(1);
    expect(await observed).toBe(error);
    expect(map).toHaveBeenCalledTimes(2);
  });

  it("handles empty input and synchronous undefined rejection", async () => {
    const map = vi.fn(async () => 1);
    expect(await mapConcurrent([], 4, map)).toEqual([]);
    expect(map).not.toHaveBeenCalled();
    const failure = mapConcurrent([1, 2], 1, () => {
      throw undefined;
    });
    await expect(failure).rejects.toBeUndefined();
  });

  it.each([0, -1, 0.5, NaN, Infinity])("rejects invalid concurrency %s", async (limit) => {
    await expect(mapConcurrent([1], limit, async (value) => value)).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
  });
});
