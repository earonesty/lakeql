import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { MemorySpillAdapter } from "./runtime.js";

it.each([
  "topKWithState",
  "sortWithState",
  "aggregateWithState",
] as const)("uses staged writes for %s without deleting a prior checkpoint", async (method) => {
  for (const outcome of ["success", "cancel", "conflict"] as const) {
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
          yield [{ id: 2 }, { id: 1 }];
        },
      },
    });
    const spill = new MemorySpillAdapter({ maxBytes: 128 * 1024 });
    const prior = Uint8Array.of(7, 8, 9);
    await spill.write("state", prior);
    const stage = spill.stageWrite.bind(spill);
    const ready = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    spill.stageWrite = async (id, bytes) => {
      const staged = await stage(id, bytes);
      if (id === "state") {
        ready.resolve();
        await gate.promise;
      }
      return staged;
    };
    const pending =
      method === "aggregateWithState"
        ? lake
            .path("table")
            .run()
            .aggregateWithState(
              [],
              { total: { op: "sum", column: "id" } },
              { spill, spillId: "state" },
            )
        : lake
            .path("table")
            .orderBy([{ column: "id" }])
            .limit(1)
            [method]({ spill, spillId: "state" });
    const settled = pending.then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    );
    await ready.promise;
    expect(await spill.read("state")).toEqual(prior);
    expect((await spill.usage()).entries).toBe(method === "sortWithState" ? 3 : 2);
    if (outcome === "cancel") controller.abort();
    if (outcome === "conflict") await spill.write("state", Uint8Array.of(4));
    gate.resolve();
    const result = await settled;
    if (outcome === "success") {
      expect("value" in result).toBe(true);
      if ("value" in result)
        expect(result.value.rows).toEqual(
          method === "aggregateWithState" ? [{ total: 3 }] : [{ id: 1 }],
        );
      expect(await spill.read("state")).not.toEqual(prior);
    } else {
      expect("error" in result).toBe(true);
      if ("error" in result)
        expect(result.error).toMatchObject({
          code: outcome === "cancel" ? "LAKEQL_ABORTED" : "LAKEQL_VALIDATION_ERROR",
        });
      expect(await spill.read("state")).toEqual(outcome === "cancel" ? prior : Uint8Array.of(4));
    }
    // Sort run files have a separate multi-file lifetime from the final checkpoint.
    expect((await spill.usage()).entries).toBe(method === "sortWithState" ? 2 : 1);
    expect(pool.usedBytes).toBe(0);
  }
});
