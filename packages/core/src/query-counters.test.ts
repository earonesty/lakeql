import { expect, it, vi } from "vitest";
import { memoryStore } from "./memory-store.js";
import { Lake, type QueryBudget } from "./query.js";
import type { Row } from "./types.js";

async function lakeFor(rows: Row[], budget: QueryBudget): Promise<Lake> {
  const store = memoryStore();
  await store.put("input", new Uint8Array(1));
  return new Lake({
    store,
    budget,
    scanner: {
      async *scan() {
        yield rows;
      },
    },
  });
}

it("measures an ordered buffer's retained row payload once per append", async () => {
  let reads = 0;
  const rows = Array.from({ length: 256 }, (_, id) => ({
    id,
    score: 256 - id,
    get payload() {
      reads++;
      return "x".repeat(64);
    },
  }));
  const lake = await lakeFor(rows, { maxMemoryBytes: 1024 * 1024 });
  const result = await lake
    .path("input")
    .orderBy([{ column: "score" }])
    .toArray();
  expect(reads).toBe(rows.length);
  expect(result.map((row) => row.id)).toEqual(rows.map((row) => row.id).reverse());
});

it.each([
  "grouped",
  "median",
])("keeps %s accounting traversal linear as input doubles", async (kind) => {
  const volumes = [];
  for (const count of [256, 512]) {
    const rows = Array.from({ length: count }, (_, id) => ({
      id,
      group: id,
      value: 1000000000 + id,
    }));
    const lake = await lakeFor(rows, { maxMemoryBytes: 1024 * 1024 * 16 });
    let characters = 0;
    const encode = TextEncoder.prototype.encode;
    const spy = vi.spyOn(TextEncoder.prototype, "encode").mockImplementation(function (
      input?: string,
    ) {
      characters += input?.length ?? 0;
      return encode.call(this, input);
    });
    try {
      const result = await lake
        .path("input")
        .run()
        .aggregateWithState(kind === "grouped" ? ["group"] : [], {
          total: { op: kind === "grouped" ? "sum" : "median", column: "value" },
        });
      expect(result.rows).toHaveLength(kind === "grouped" ? count : 1);
      volumes.push(characters);
    } finally {
      spy.mockRestore();
    }
  }
  expect((volumes[1] ?? Infinity) / (volumes[0] ?? 1)).toBeLessThan(2.8);
});

it("preserves exact serialized DISTINCT limits and rejects one byte below", async () => {
  const rows = [
    { id: 1, text: '雪"' },
    { id: 2, text: "🙂" },
  ];
  const keys = rows.map((row) => JSON.stringify(row));
  const cap = new TextEncoder().encode(JSON.stringify(keys)).length;
  for (const limit of [cap, cap - 1]) {
    const lake = await lakeFor(rows, { maxMemoryBytes: limit });
    if (limit === cap) expect(await lake.path("input").distinct().toArray()).toEqual(rows);
    else
      await expect(lake.path("input").distinct().toArray()).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
  }
});

it("matches serialized aggregate state at exact byte and restored-row boundaries", async () => {
  const rows = Array.from({ length: 32 }, (_, id) => ({
    group: "雪",
    value: id + 1,
    text: `🙂"${id}`,
  }));
  const spec = {
    median: { op: "median", column: "value" },
    quantile: { op: "quantile", column: "value", quantile: 0.75 },
    distinct: { op: "count_distinct", column: "text" },
    approximate: { op: "approx_count_distinct", column: "text" },
    mode: { op: "mode", column: "text" },
    total: { op: "sum", column: "value" },
  } as const;
  const baseline = await (await lakeFor(rows, {}))
    .path("input")
    .run()
    .aggregateWithState(["group"], spec);
  const cap = baseline.operatorState.byteLength;
  for (const limit of [cap, cap - 1]) {
    const lake = await lakeFor(rows, { maxMemoryBytes: limit });
    const result = lake.path("input").run().aggregateWithState(["group"], spec);
    if (limit === cap) expect((await result).rows).toEqual(baseline.rows);
    else await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  }
  for (const limit of [160, 159]) {
    const lake = await lakeFor([], { maxMemoryBytes: cap, maxBufferedRows: limit });
    const result = lake
      .path("input")
      .run()
      .aggregateWithState(["group"], spec, { operatorState: baseline.operatorState });
    if (limit === 160) expect((await result).rows).toEqual(baseline.rows);
    else await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  }
});
