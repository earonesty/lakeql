import { expect, it } from "vitest";
import { col, eq } from "./expr.js";
import { createOwnedTaskManifest, createTaskManifest } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it("retains input ownership and preserves normalized task identifiers at exact capacity", async () => {
  const run = async (capacity: number) => {
    const store = memoryStore();
    await store.put("lake/__proto__=safe/one", new Uint8Array(1));
    const pool = new MemoryBudget(capacity);
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        scan() {
          throw Error("unexpected scan");
        },
        async planTask() {
          return {
            rowGroupRanges: [
              { start: 5, end: 7 },
              { start: 1, end: 2 },
            ],
          };
        },
      },
    });
    const tasks = await lake
      .hive("lake/**/*")
      .select(["z", "a"])
      .where(eq(col("payload"), new Uint8Array([1, 2])))
      .planOwnedTasks();
    const inputBytes = pool.usedBytes;
    try {
      const expected = createTaskManifest({ jobId: "job-猫", tasks: tasks.value });
      const manifest = createOwnedTaskManifest({ jobId: "job-猫", tasks });
      try {
        expect(manifest.value).toEqual(expected);
        expect(manifest.value.tasks[0]?.input.rowGroupRanges).toEqual([
          { start: 1, end: 2 },
          { start: 5, end: 7 },
        ]);
        expect(
          Object.hasOwn(manifest.value.tasks[0]?.input.partitionValues ?? {}, "__proto__"),
        ).toBe(true);
        tasks.release();
        expect(pool.usedBytes).toBeGreaterThan(inputBytes);
        expect(manifest.value.tasks[0]?.input.residualPredicate).toMatchObject({
          right: { value: new Uint8Array([1, 2]) },
        });
        return pool.peakBytes;
      } finally {
        manifest.release();
      }
    } catch (error) {
      expect(pool.usedBytes).toBe(inputBytes);
      throw error;
    } finally {
      tasks.release();
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = await run(1 << 20);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("exposes owned query manifests and transfers plain results out of accounting", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(1 << 20);
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw Error("unexpected scan");
      },
    },
  });
  const query = lake.path("table").select(["id"]);
  const owned = await query.ownedTaskManifest("job");
  expect(pool.usedBytes).toBeGreaterThan(0);
  owned.release();
  expect(pool.usedBytes).toBe(0);
  const plain = await query.taskManifest("job");
  expect(plain.tasks).toHaveLength(1);
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "success",
  "failure",
  "cancel",
] as const)("retains the slice manifest while a scan %s is pending", async (outcome) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(1 << 20);
  const controller = new AbortController();
  let enter!: () => void, finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        try {
          enter();
          await pending;
          expect(pool.usedBytes).toBeGreaterThan(0);
          if (outcome === "failure") throw Error("scan failed");
          yield [{ id: 1 }, { id: 2 }];
        } finally {
          closed = true;
        }
      },
    },
  });
  const work = lake.path("table").run().slice({ maxRows: 1 });
  const checked =
    outcome === "success"
      ? expect(work).resolves.toMatchObject({
          rows: [{ id: 1 }],
          bookmark: { position: { rowOffset: 1 } },
        })
      : expect(work).rejects.toMatchObject(
          outcome === "failure" ? { message: "scan failed" } : { code: "LAKEQL_ABORTED" },
        );
  await entered;
  if (outcome === "cancel") controller.abort();
  expect(pool.usedBytes).toBeGreaterThan(0);
  finish();
  await checked;
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});

it("releases slice manifests on terminal output and stale bookmark rejection", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(1 << 20);
  let scans = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        scans++;
        yield [{ id: 1 }, { id: 2 }];
      },
    },
  });
  const query = lake.path("table");
  const first = await query.run().slice({ maxRows: 1 });
  expect(first.bookmark).toBeDefined();
  expect(pool.usedBytes).toBe(0);
  const bookmark = first.bookmark;
  if (bookmark === undefined) throw Error("missing bookmark");
  const terminal = await query.run().slice({ maxRows: 2, bookmark });
  expect(terminal).toEqual({ rows: [{ id: 2 }] });
  expect(pool.usedBytes).toBe(0);
  await expect(
    query.run().slice({ maxRows: 1, bookmark: { ...bookmark, planFingerprint: "stale" } }),
  ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
  expect(scans).toBe(2);
  expect(pool.usedBytes).toBe(0);
});
