import { expect, it } from "vitest";
import { type Batch, batchFromColumns, materializeBatchRows } from "./batch.js";
import { DecimalValue } from "./decimal-value.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { detachOwnedBatch, detachOwnedRow } from "./owned-row-detachment.js";
import { Lake, type ScanOptions } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

it("copies shared vector dictionaries once and detaches numeric storage", () => {
  const dictionary = { type: "utf8" as const, values: ["猫", "second"] };
  const indices = Uint32Array.of(1, 0);
  const batch: Batch = {
    rowCount: 2,
    columns: {
      a: { type: "dict", indices, dictionary },
      b: { type: "dict", indices, dictionary },
      id: { type: "i32", values: Int32Array.of(7, 8) },
    },
  };
  const pool = new MemoryBudget(65536);
  const owner = detachOwnedBatch(batch, pool);
  const a = owner.value.columns.a;
  const b = owner.value.columns.b;
  if (a?.type !== "dict" || b?.type !== "dict") throw Error("Missing dictionaries");
  expect(a.dictionary).toBe(b.dictionary);
  expect(a.dictionary).not.toBe(dictionary);
  expect(a.indices).toBe(b.indices);
  indices.fill(0);
  dictionary.values.fill("changed");
  const id = batch.columns.id;
  if (id?.type === "i32") id.values.fill(0);
  expect(materializeBatchRows(owner.value)).toEqual([
    { a: "second", b: "second", id: 7 },
    { a: "猫", b: "猫", id: 8 },
  ]);
  owner.release();
  expect(pool.usedBytes).toBe(0);
  // Vector support does not silently broaden the borrowed row data contract.
  expect(() => detachOwnedRow({ id: Int32Array.of(1) }, pool)).toThrow();
  expect(pool.usedBytes).toBe(0);
});

it("admits detached vector storage at exact peak and rejects one byte less", () => {
  const batch: Batch = {
    rowCount: 64,
    columns: { id: { type: "i64", values: new BigInt64Array(64) } },
  };
  const measured = new MemoryBudget(65536);
  detachOwnedBatch(batch, measured).release();
  const exact = new MemoryBudget(measured.peakBytes);
  detachOwnedBatch(batch, exact).release();
  expect(exact.usedBytes).toBe(0);
  const small = new MemoryBudget(measured.peakBytes - 1);
  expect(() => detachOwnedBatch(batch, small)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(small.usedBytes).toBe(0);
});

it.each([false, true])("preserves reusable vector scanner input (spill=%s)", async (spill) => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const ids = Int32Array.of(2, 1);
  const batch: Batch = { rowCount: 2, columns: { id: { type: "i32", values: ids } } };
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw Error("Unexpected row fallback");
      },
      async *scanVectorBatches() {
        yield { batch, rowOffset: 0 };
        ids.set([4, 3]);
        yield { batch, rowOffset: 2 };
        ids.fill(99);
      },
    },
  });
  const query = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
    })
    .orderBy([{ column: "id" }]);
  const rows = spill
    ? (await query.windowWithState({ spill: memorySpillAdapter() })).rows
    : await query.toArray();
  expect(rows).toEqual([1, 2, 3, 4].map((id) => ({ id, rn: id })));
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("prefers owned vector input and retains buffers (ownedOnly=%s)", async (ownedOnly) => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let producerClosed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw Error("Unexpected row fallback");
      },
      ...(ownedOnly
        ? {}
        : {
            scanVectorBatches() {
              throw Error("Unexpected borrowed vector scan");
            },
          }),
      async *scanOwnedVectorBatches(
        _path: string,
        options: ScanOptions & { memoryBudget: MemoryBudget },
      ) {
        const stage = new MemoryScope(options.memoryBudget);
        const values = stage.allocate(8, "source values", () => Int32Array.of(1, 2)).value;
        const data = stage.allocate(8192, "source backing", () => new Uint8Array(8192)).value;
        data.set([1, 2]);
        const owner = stage.retainAllocations({
          rowOffset: 0,
          batch: {
            rowCount: 2,
            columns: {
              id: { type: "i32" as const, values },
              bytes: { type: "binary-view" as const, data, fixedLength: 1, length: 2 },
            },
          },
        });
        stage.dispose();
        try {
          yield owner;
        } finally {
          owner.release();
          producerClosed = true;
        }
      },
    },
  });
  const stream = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
    })
    .batchSize(1)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  expect((await stream.next()).value).toEqual([{ id: 1, rn: 1, bytes: Uint8Array.of(1) }]);
  expect(producerClosed).toBe(true);
  expect(pool.usedBytes).toBeGreaterThanOrEqual(8192 + 8);
  await stream.return?.();
  expect(pool.usedBytes).toBe(0);
});

it("preserves nested, union, decimal, timestamp and packed physical storage", () => {
  const batch = batchFromColumns({
    nested: [[{ value: "猫" }], []],
    decimal: [new DecimalValue(123n, 2), new DecimalValue(7n, 0)],
  });
  const data = Uint8Array.of(1, 2);
  batch.columns.bytes = { type: "binary-view", data, fixedLength: 1, length: 2 };
  batch.columns.map = {
    type: "map",
    offsets: Int32Array.of(0, 1, 2),
    keys: { type: "utf8", values: ["a", "b"] },
    values: { type: "i32", values: Int32Array.of(4, 5) },
  };
  batch.columns.union = {
    type: "union",
    tags: Uint8Array.of(0, 1),
    offsets: Uint32Array.of(0, 0),
    children: [
      { type: "i32", values: Int32Array.of(8) },
      { type: "utf8", values: ["text"] },
    ],
  };
  batch.columns.time = {
    type: "timestamp",
    values: BigInt64Array.of(0n, 1n),
    unit: "micros",
    isAdjustedToUTC: true,
  };
  const pool = new MemoryBudget(65536);
  const owner = detachOwnedBatch(batch, pool);
  expect(owner.value).toEqual(batch);
  const expected = materializeBatchRows(owner.value);
  data.fill(99);
  expect(materializeBatchRows(owner.value)).toEqual(expected);
  expect(materializeBatchRows(owner.value)[0]?.bytes).toEqual(Uint8Array.of(1));
  expect(materializeBatchRows(owner.value)[1]?.bytes).toEqual(Uint8Array.of(2));
  expect(owner.value.columns.bytes).not.toBe(batch.columns.bytes);
  owner.release();
  expect(pool.usedBytes).toBe(0);
});
