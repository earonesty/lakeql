import { describe, expect, it } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { MemorySpillAdapter } from "./runtime.js";

describe.each(["sort", "window"] as const)("%s checkpoint batches", (operator) => {
  it.each([
    "success",
    "cancel",
    "commit-cancel",
    "conflict",
    "write-failure",
  ] as const)("publishes runs and checkpoint together: %s", async (outcome) => {
    const pool = new MemoryBudget(128 * 1024);
    const controller = new AbortController();
    const store = memoryStore();
    await store.put("table", new Uint8Array(1));
    const lake = new Lake({
      store,
      memoryPool: pool,
      budget: { signal: controller.signal },
      scanner: {
        async *scan() {
          yield [{ id: 3 }, { id: 1 }, { id: 2 }];
        },
      },
    });
    const spill = new MemorySpillAdapter({ maxBytes: 128 * 1024 });
    const prior = Uint8Array.of(7, 8, 9);
    await spill.write("state", prior);
    const begin = spill.beginWriteBatch.bind(spill);
    const ready = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    spill.beginWriteBatch = () => {
      const batch = begin();
      return {
        ...batch,
        async commit() {
          if (outcome === "commit-cancel") {
            ready.resolve();
            await gate.promise;
          }
          await batch.commit();
        },
        async write(id, data) {
          const ref = await batch.write(id, data);
          if (id === "state" && outcome !== "commit-cancel") {
            ready.resolve();
            await gate.promise;
            if (outcome === "write-failure") throw new Error("checkpoint storage failure");
          }
          return ref;
        },
      };
    };
    const query = lake.path("table");
    const pending =
      operator === "sort"
        ? query
            .orderBy([{ column: "id" }])
            .limit(2)
            .sortWithState({ spill, spillId: "state" })
        : query
            .window("rn", {
              fn: "row_number",
              args: [],
              over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
            })
            .windowWithState({ spill, spillId: "state" });
    const settled = pending.then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    );
    await ready.promise;
    expect(await spill.read("state")).toEqual(prior);
    await expect(spill.read("state-run-000000")).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });
    if (outcome === "cancel" || outcome === "commit-cancel") controller.abort();
    if (outcome === "conflict") await spill.write("state", Uint8Array.of(4));
    gate.resolve();
    const result = await settled;
    if (outcome === "success") {
      expect("value" in result).toBe(true);
      if ("value" in result)
        expect(result.value.rows).toEqual(
          operator === "sort"
            ? [{ id: 1 }, { id: 2 }]
            : [
                { id: 3, rn: 3 },
                { id: 1, rn: 1 },
                { id: 2, rn: 2 },
              ],
        );
      expect((await spill.read("state-run-000000")).byteLength).toBeGreaterThan(0);
      expect((await spill.usage()).entries).toBe(2);
    } else if (outcome === "commit-cancel") {
      expect(result).toMatchObject({ error: { code: "LAKEQL_ABORTED" } });
      // Commit may already be in flight when cancellation arrives. Rollback
      // must never delete its successfully published checkpoint.
      expect(await spill.read("state")).not.toEqual(prior);
      expect((await spill.read("state-run-000000")).byteLength).toBeGreaterThan(0);
      expect((await spill.usage()).entries).toBe(2);
    } else {
      expect("error" in result).toBe(true);
      if ("error" in result && outcome !== "write-failure")
        expect(result.error).toMatchObject({
          code: outcome === "cancel" ? "LAKEQL_ABORTED" : "LAKEQL_VALIDATION_ERROR",
        });
      expect(await spill.read("state")).toEqual(outcome === "conflict" ? Uint8Array.of(4) : prior);
      await expect(spill.read("state-run-000000")).rejects.toMatchObject({
        code: "LAKEQL_OBJECT_NOT_FOUND",
      });
      expect((await spill.usage()).entries).toBe(1);
    }
    expect(pool.usedBytes).toBe(0);
  });
});
