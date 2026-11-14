import { parquetWriteBuffer } from "hyparquet-writer";
import { col, eq, gt, Lake, MemoryBudget, memoryStore, mod } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { createParquetLake } from "./index.js";
import { ParquetScanAdapter } from "./scan-adapter.js";

async function fixture() {
  const store = memoryStore();
  const rows = Array.from({ length: 128 }, (_, id) => ({ id, text: `${id}:`.padEnd(256, "x") }));
  await store.put(
    "rows.parquet",
    new Uint8Array(
      parquetWriteBuffer({
        rowGroupSize: Array(16).fill(8),
        columnData: [
          { name: "id", type: "INT32", data: rows.map((row) => row.id) },
          { name: "text", type: "STRING", data: rows.map((row) => row.text) },
        ],
      }),
    ),
  );
  return { store, rows };
}

it("holds query output reservations across completion and independent owners", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const result = lake.path("rows.parquet").batchSize(7).run();
  const output = await result.toOwnedArray();
  expect(output.value).toEqual(rows);
  expect(result.stats.rowsReturned).toBe(128);
  expect(pool.usedBytes).toBeGreaterThan(0);
  const copy = output.retain();
  output.release();
  expect(copy.value[127]).toEqual(rows[127]);
  expect(pool.usedBytes).toBeGreaterThan(0);
  copy.release();
  expect(pool.usedBytes).toBe(0);
  expect(await lake.path("rows.parquet").offset(3).limit(2).run().toArray()).toEqual(
    rows.slice(3, 5),
  );
  expect(pool.usedBytes).toBe(0);
});

it("bounds default collected output before exhausting the file and unwinds reservations", async () => {
  const { store } = await fixture();
  const pool = new MemoryBudget(48 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const result = lake.path("rows.parquet").batchSize(4).run();
  await expect(result.toArray()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(result.stats.rowsReturned).toBeLessThan(128);
  expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  expect(pool.usedBytes).toBe(0);
});

it("competes with retained prior query output in the same pool", async () => {
  const { store } = await fixture();
  const pool = new MemoryBudget(180 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const first = await lake.path("rows.parquet").batchSize(8).run().toOwnedArray();
  const used = pool.usedBytes;
  expect(used).toBeGreaterThan(0);
  await expect(lake.path("rows.parquet").batchSize(8).run().toOwnedArray()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(pool.usedBytes).toBe(used);
  first.release();
  expect(pool.usedBytes).toBe(0);
  const next = await lake.path("rows.parquet").batchSize(8).run().toOwnedArray();
  next.release();
  expect(pool.usedBytes).toBe(0);
});

it("owns deferred filtered output across predicate batches, offsets and wildcard projections", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const output = await lake
    .path("rows.parquet")
    .select(["text", "id"])
    .where(gt("id", 90))
    .offset(5)
    .limit(13)
    .batchSize(3)
    .run()
    .toOwnedArray();
  expect(output.value).toEqual(rows.slice(96, 109));
  expect(pool.usedBytes).toBeGreaterThan(0);
  output.release();
  expect(pool.usedBytes).toBe(0);
  const wildcard = await lake
    .path("rows.parquet")
    .select(["*", "text"])
    .where(gt("id", 126))
    .limit(1)
    .run()
    .toOwnedArray();
  expect(wildcard.value).toEqual(rows.slice(127));
  wildcard.release();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  [0, 0],
  [0, 1],
  [2, 7],
  [40, 9],
  [1, 200],
])("matches eager output at deferred offset=%s limit=%s", async (offset, limit) => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const predicate = eq(mod(col("id"), 5), 2);
  const expected = rows.filter((row) => row.id % 5 === 2).slice(offset, offset + limit);
  const result = lake
    .path("rows.parquet")
    .select(["id", "text"])
    .where(predicate)
    .offset(offset)
    .limit(limit)
    .batchSize(7)
    .run();
  const owned = await result.toOwnedArray();
  expect(owned.value).toEqual(expected);
  expect(result.stats.rowsReturned).toBe(expected.length);
  owned.release();
  expect(pool.usedBytes).toBe(0);
});

it("admits final vector top-k rows before materialization and transfers the result without recollecting", async () => {
  const { store, rows } = await fixture();
  for (const capacity of [512 * 1024, 180 * 1024]) {
    const pool = new MemoryBudget(capacity);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    // No explicit projection: the existing vector top-k path keeps all columns.
    const result = lake
      .path("rows.parquet")
      .orderBy([{ column: "id", direction: "desc" }])
      .limit(128)
      .batchSize(8)
      .run();
    if (capacity === 512 * 1024) expect(await result.toArray()).toEqual([...rows].reverse());
    else
      await expect(result.toArray()).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { purpose: "materialized output" },
      });
    expect(pool.peakBytes).toBeLessThanOrEqual(capacity);
    expect(pool.usedBytes).toBe(0);
  }
});

it("retains bounded ordered output across completion, offset and independent handles", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const output = await lake
    .path("rows.parquet")
    .orderBy([{ column: "id", direction: "desc" }])
    .offset(3)
    .limit(5)
    .batchSize(7)
    .run()
    .toOwnedArray();
  expect(output.value).toEqual(rows.slice(-8, -3).reverse());
  const copy = output.retain();
  output.release();
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(copy.value).toEqual(rows.slice(-8, -3).reverse());
  copy.release();
  expect(pool.usedBytes).toBe(0);
  const projected = await lake
    .path("rows.parquet")
    .select(["id"])
    .orderBy([{ column: "id", direction: "desc" }])
    .limit(2)
    .run()
    .toOwnedArray();
  expect(projected.value).toEqual([{ id: 127 }, { id: 126 }]);
  projected.release();
  expect(pool.usedBytes).toBe(0);
});

it("cleans up empty and rejected owned top-k queries", async () => {
  const { store } = await fixture();
  for (const cap of [16, 1024 * 1024]) {
    const pool = new MemoryBudget(cap);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    const query = lake
      .path("rows.parquet")
      .where(gt("id", 1000))
      .orderBy([{ column: "id", direction: "asc" }])
      .limit(5)
      .run();
    if (cap === 16)
      await expect(query.toOwnedArray()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else {
      const output = await query.toOwnedArray();
      expect(output.value).toEqual([]);
      output.release();
    }
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(cap);
  }
});

it.each([
  1, 7, 64,
])("preserves owned top-k ties across real row groups at batch size %s", async (batchSize) => {
  const rows = Array.from({ length: 64 }, (_, id) => ({
    id,
    key: id % 5 === 0 ? null : id % 3,
    secondary: id % 2,
  }));
  const store = memoryStore();
  await store.put(
    "ties.parquet",
    new Uint8Array(
      parquetWriteBuffer({
        rowGroupSize: Array(8).fill(8),
        columnData: [
          { name: "id", type: "INT32", data: rows.map((row) => row.id) },
          { name: "key", type: "INT32", data: rows.map((row) => row.key) },
          { name: "secondary", type: "INT32", data: rows.map((row) => row.secondary) },
        ],
      }),
    ),
  );
  for (const direction of ["asc", "desc"] as const)
    for (const nulls of ["first", "last"] as const) {
      const expected = rows
        .filter((row) => row.id > 3)
        .sort((left, right) => {
          if (left.key === null && right.key !== null) return nulls === "first" ? -1 : 1;
          if (left.key !== null && right.key === null) return nulls === "first" ? 1 : -1;
          const order = (left.key ?? 0) - (right.key ?? 0);
          return (
            (direction === "asc" ? order : -order) ||
            right.secondary - left.secondary ||
            left.id - right.id
          );
        });
      for (const [offset, limit] of [
        [5, 17],
        [100, 5],
        [0, 0],
      ]) {
        const pool = new MemoryBudget(1024 * 1024);
        const lake = createParquetLake({
          store,
          memoryPool: pool,
          scanRangeCache: { maxBytes: 0 },
        });
        const output = await lake
          .path("ties.parquet")
          .select(["id"])
          .where(gt("id", 3))
          .orderBy([
            { column: "key", direction, nulls },
            { column: "secondary", direction: "desc" },
          ])
          .offset(offset)
          .limit(limit)
          .batchSize(batchSize)
          .run()
          .toOwnedArray();
        expect(output.value).toEqual(
          expected.slice(offset, offset + limit).map((row) => ({ id: row.id })),
        );
        output.release();
        expect(pool.usedBytes).toBe(0);
        expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
      }
    }
});

it.each([
  false,
  true,
])("releases cancelled top-k candidates while preserving sibling output (deferred=%s)", async (deferred) => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const sibling = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const pinned = await sibling.path("rows.parquet").limit(2).run().toOwnedArray();
  const baseline = pool.usedBytes;
  const controller = new AbortController();
  const adapter = new ParquetScanAdapter(store, { scanRangeCache: { maxBytes: 0 } });
  const original = adapter.scanOwnedVectorBatches;
  if (!original) throw Error("Missing owned scanner");
  let closed = false;
  let observedCandidateMemory = false;
  const scan = vi
    .spyOn(adapter, "scanOwnedVectorBatches")
    .mockImplementation(async function* (path, options) {
      try {
        for await (const input of original(path, options)) {
          yield input;
          observedCandidateMemory = pool.usedBytes > baseline;
          controller.abort();
        }
      } finally {
        closed = true;
      }
    });
  try {
    const lake = new Lake({
      store,
      scanner: adapter,
      memoryPool: pool,
      budget: { signal: controller.signal },
    });
    const query = lake.path("rows.parquet");
    const result = (deferred ? query.select(["text"]) : query)
      .orderBy([{ column: "id", direction: "desc" }])
      .limit(10)
      .batchSize(4)
      .run();
    await expect(deferred ? result.toArray() : result.toOwnedArray()).rejects.toMatchObject({
      code: "LAKEQL_ABORTED",
    });
    expect(closed).toBe(true);
    expect(observedCandidateMemory).toBe(true);
    expect(pool.usedBytes).toBe(baseline);
    expect(pinned.value).toEqual(rows.slice(0, 2));
  } finally {
    scan.mockRestore();
    pinned.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("ranks owned Parquet keys before reading deferred payload columns (owned=%s)", async (owned) => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const adapter = new ParquetScanAdapter(store, { scanRangeCache: { maxBytes: 0 } });
  const scans = vi.spyOn(adapter, "scanOwnedVectorBatches");
  const lake = new Lake({ store, scanner: adapter, memoryPool: pool });
  try {
    const result = lake
      .path("rows.parquet")
      .select(["text"])
      .orderBy([{ column: "id", direction: "desc" }])
      .offset(2)
      .limit(5)
      .batchSize(7)
      .run();
    const outputOwner = owned ? await result.toOwnedArray() : undefined;
    const output = outputOwner ? outputOwner.value : await result.toArray();
    expect(output).toEqual(
      rows
        .slice(121, 126)
        .reverse()
        .map(({ text }) => ({ text })),
    );
    expect(scans.mock.calls.some(([, options]) => options.columns?.join() === "id")).toBe(true);
    expect(scans.mock.calls.every(([, options]) => options.columns?.join() !== "text,id")).toBe(
      true,
    );
    if (outputOwner) {
      expect(pool.usedBytes).toBeGreaterThan(0);
      const retained = outputOwner.retain();
      outputOwner.release();
      expect(retained.value).toEqual(output);
      retained.release();
    }
    expect(pool.usedBytes).toBe(0);
    const constrained = new Lake({
      store,
      scanner: adapter,
      memoryPool: pool,
      budget: { maxMemoryBytes: 1 },
    });
    await expect(
      constrained
        .path("rows.parquet")
        .select(["text"])
        .orderBy([{ column: "id", direction: "desc" }])
        .limit(5)
        .batchSize(7)
        .run()
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(pool.usedBytes).toBe(0);
  } finally {
    scans.mockRestore();
  }
});

it.each([
  "missing",
  "duplicate",
  "offset",
  "column",
])("rejects malformed deferred %s scans and releases candidates", async (mode) => {
  const { store } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const adapter = new ParquetScanAdapter(store, { scanRangeCache: { maxBytes: 0 } });
  const original = adapter.scanOwnedVectorBatches;
  let lateClosed = false;
  const spy = vi
    .spyOn(adapter, "scanOwnedVectorBatches")
    .mockImplementation(async function* (path, options) {
      if (options.rowStart === undefined) {
        yield* original(path, options);
        return;
      }
      try {
        if (mode === "missing") return;
        for await (const input of original(path, options)) {
          const value = input.value;
          if (mode === "offset") value.rowOffset = -1;
          if (mode === "column") delete value.batch.columns.text;
          const duplicate = mode === "duplicate" ? input.retain() : undefined;
          try {
            yield input;
            if (duplicate) yield duplicate;
          } finally {
            duplicate?.release();
            input.release();
          }
        }
      } finally {
        lateClosed = true;
      }
    });
  try {
    const lake = new Lake({ store, scanner: adapter, memoryPool: pool });
    await expect(
      lake
        .path("rows.parquet")
        .select(["text"])
        .orderBy([{ column: "id", direction: "desc" }])
        .limit(5)
        .batchSize(7)
        .run()
        .toOwnedArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    expect(lateClosed).toBe(true);
    expect(pool.usedBytes).toBe(0);
  } finally {
    spy.mockRestore();
  }
});

it("collects and resumes bounded slices across real Parquet row groups", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const query = lake.path("rows.parquet").batchSize(7).run();
  const first = await query.slice({ maxRows: 11 });
  expect(first.rows).toEqual(rows.slice(0, 11));
  expect(first.bookmark?.position.rowOffset).toBe(11);
  expect(pool.usedBytes).toBe(0);
  if (!first.bookmark) throw new Error("Expected continuation bookmark");
  const second = await query.slice({ maxRows: 117, bookmark: first.bookmark });
  expect(second.rows).toEqual(rows.slice(11));
  expect(second.bookmark).toBeUndefined();
  expect(first.rows).toEqual(rows.slice(0, 11));
  expect(pool.usedBytes).toBe(0);
});

it("bounds retained slice output and recovers with a smaller slice", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(48 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  await expect(
    lake.path("rows.parquet").batchSize(4).run().slice({ maxRows: 128 }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(pool.usedBytes).toBe(0);
  const small = await lake.path("rows.parquet").batchSize(4).run().slice({ maxRows: 2 });
  expect(small.rows).toEqual(rows.slice(0, 2));
  expect(pool.usedBytes).toBe(0);
});

it("owns slice rows and bookmarks across query completion, resumption and independent handles", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const query = lake.path("rows.parquet").batchSize(7).run();
  const first = await query.ownedSlice({ maxRows: 11 });
  const retained = first.retain();
  first.release();
  try {
    expect(retained.value.rows).toEqual(rows.slice(0, 11));
    expect(pool.usedBytes).toBeGreaterThan(0);
    const bookmark = retained.value.bookmark;
    if (!bookmark) throw new Error("Expected continuation");
    const second = await query.ownedSlice({ maxRows: 117, bookmark });
    try {
      expect(second.value.rows).toEqual(rows.slice(11));
      expect(second.value.bookmark).toBeUndefined();
      expect(retained.value.rows).toEqual(rows.slice(0, 11));
      const occupied = pool.usedBytes;
      const pressure = pool.reserve(pool.maxBytes - occupied, "other jobs");
      try {
        await expect(query.ownedSlice({ maxRows: 1 })).rejects.toMatchObject({
          code: "LAKEQL_BUDGET_EXCEEDED",
        });
        expect(second.value.rows).toEqual(rows.slice(11));
      } finally {
        pressure.release();
      }
      expect(pool.usedBytes).toBe(occupied);
    } finally {
      second.release();
    }
    expect(pool.usedBytes).toBeGreaterThan(0);
  } finally {
    retained.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps columnar top-k charged while its borrowed batch iterator is suspended", async () => {
  const { store, rows } = await fixture();
  const pool = new MemoryBudget(1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const iterator = lake
    .path("rows.parquet")
    .orderBy([{ column: "id", direction: "desc" }])
    .limit(17)
    .batchSize(7)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  try {
    const first = await iterator.next();
    expect(first.value).toEqual([...rows].reverse().slice(0, 7));
    expect(pool.usedBytes).toBeGreaterThan(0);
    const pressure = pool.reserve(pool.maxBytes - pool.usedBytes, "other jobs");
    try {
      await expect(lake.path("rows.parquet").limit(1).run().toOwnedArray()).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
      expect((await iterator.next()).value).toEqual([...rows].reverse().slice(7, 14));
      expect(pool.usedBytes).toBe(pool.maxBytes);
    } finally {
      pressure.release();
    }
  } finally {
    await iterator.return?.();
  }
  expect(pool.usedBytes).toBe(0);
});
