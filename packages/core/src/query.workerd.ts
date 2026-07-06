import { describe, expect, it } from "vitest";
import { col, eq } from "./expr.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type ScanAdapter, type ScanOptions } from "./query.js";
import type { Row } from "./types.js";

class InlineScanner implements ScanAdapter {
  async *scan(_path: string, options: ScanOptions): AsyncIterable<Row[]> {
    options.stats.rangeRequests += 1;
    yield [
      { id: 1, region: "west" },
      { id: 2, region: "east" },
      { id: 3, region: "west" },
    ];
  }
}

describe("workerd runtime", () => {
  it("runs core query and bookmark resume APIs in the Workers runtime", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");

    const store = memoryStore();
    await store.put("table", new Uint8Array([1, 2, 3]));
    const lake = new Lake({
      store,
      scanner: new InlineScanner(),
      queryId: () => "q_workerd",
    });

    await expect(lake.path("table").where(eq("region", "west")).toArray()).resolves.toEqual([
      { id: 1, region: "west" },
      { id: 3, region: "west" },
    ]);

    const first = await lake
      .path("table")
      .where(eq("region", "west"))
      .run({
        slice: { maxRows: 1 },
      });
    expect(first.rows).toEqual([{ id: 1, region: "west" }]);
    if (!first.bookmark) throw new Error("expected workerd bookmark");

    await expect(lake.resume(first.bookmark).run({ slice: { maxRows: 2 } })).resolves.toEqual({
      rows: [{ id: 3, region: "west" }],
    });
  });

  it("runs window functions through the lazy backend in the Workers runtime", async () => {
    const store = memoryStore();
    await store.put("table", new Uint8Array([1, 2, 3]));
    const lake = new Lake({
      store,
      scanner: new InlineScanner(),
      queryId: () => "q_window_workerd",
    });

    await expect(
      lake
        .path("table")
        .window("rn", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        })
        .select(["id", "region", "rn"])
        .orderBy([{ column: "id" }])
        .toArray(),
    ).resolves.toEqual([
      { id: 1, region: "west", rn: 1 },
      { id: 2, region: "east", rn: 1 },
      { id: 3, region: "west", rn: 2 },
    ]);
  });
});
