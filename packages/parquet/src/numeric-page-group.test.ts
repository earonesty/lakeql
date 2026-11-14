import { zstdCompressSync } from "node:zlib";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  batchFromVectors,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it } from "vitest";
import { MemoryStageScheduler } from "../../core/dist/memory-stage-scheduler.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import {
  prepareNumericColumnPages,
  readAlignedNumericPageGroups,
} from "./numeric-page-alignment.js";
import { type NumericPageGroupEntry, planNumericPageGroup } from "./numeric-page-group.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { cachedRangeBuffer } from "./range-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { preparePageWindow, readParquetVectorBatchesFromFile } from "./vector-batches.js";

async function fixture(
  rows = 64,
  pageSize = 1 << 20,
  codec: "UNCOMPRESSED" | "SNAPPY" | "ZSTD" = "SNAPPY",
) {
  const values = Array.from({ length: rows }, (_, i) => i);
  const bytes = parquetWriteBuffer({
    codec,
    ...(codec === "ZSTD" ? { compressors: { ZSTD: zstdCompressSync } } : {}),
    pageSize,
    columnData: [
      { name: "a", type: "INT32", encoding: "PLAIN", data: values },
      {
        name: "b",
        type: "DOUBLE",
        encoding: "PLAIN",
        data: values.map((i) => (i % 3 ? i + 0.5 : null)),
      },
    ],
  });
  const store = memoryStore();
  await store.put("group.parquet", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "group.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  const entries: NumericPageGroupEntry[] = [];
  for (const chunk of metadata.row_groups[0]?.columns ?? []) {
    const column = chunk.meta_data;
    if (!column) throw new Error("Missing column");
    const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
    const element = schemaPath.at(-1)?.element;
    if (!element) throw new Error("Missing leaf");
    const start = Number(column.data_page_offset);
    const page = await preparePageWindow(
      file,
      start,
      start + Number(column.total_compressed_size),
      0,
    );
    if (!page) throw new Error("Missing page");
    entries.push({
      page,
      decoder: {
        ...column,
        pathInSchema: column.path_in_schema,
        schemaPath,
        element,
        parsers: DEFAULT_PARSERS,
      },
    });
  }
  const plan = planNumericPageGroup(entries, file);
  if (!plan) throw new Error("Missing group capability");
  return { file, plan, values, entries, metadata };
}

it("admits the complete real-column group before bounded parallel transport", async () => {
  const { file, plan, values } = await fixture();
  for (const concurrency of [1, 2]) {
    let active = 0,
      peak = 0,
      reads = 0;
    const source = {
      ...file,
      async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
        reads++;
        active++;
        peak = Math.max(peak, active);
        try {
          if (!file.sliceOwned) throw new Error("Missing owned read");
          return await file.sliceOwned(...args);
        } finally {
          active--;
        }
      },
    };
    const rejected = new MemoryScope(new MemoryBudget(plan.bytes - 1));
    await expect(plan.execute(source, rejected, concurrency)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    expect(reads).toBe(0);
    rejected.dispose();
    const budget = new MemoryBudget(plan.bytes);
    const scope = new MemoryScope(budget);
    const vectors = await plan.execute(source, scope, concurrency);
    const owner = scope.retainAllocations(vectors);
    scope.dispose();
    expect(peak).toBe(concurrency);
    expect(reads).toBe(2);
    if (!vectors[0] || !vectors[1]) throw new Error("Missing vectors");
    expect(materializeBatchRows(batchFromVectors({ a: vectors[0], b: vectors[1] }))).toEqual(
      values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })),
    );
    expect(budget.peakBytes).toBe(plan.bytes);
    owner.release();
    expect(budget.usedBytes).toBe(0);
  }
});

it("waits for pending owned transport to settle after a peer fails", async () => {
  const { file, plan } = await fixture();
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reads = 0;
  const source = {
    ...file,
    async sliceOwned(start: number, end: number, scope: MemoryScope) {
      if (++reads === 2) throw new Error("Injected peer failure");
      return scope.allocateAsync(end - start, "pending group body", async () => {
        await gate;
        return file.slice(start, end);
      });
    },
  };
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  const pending = plan.execute(source, scope, 2);
  const rejected = expect(pending).rejects.toThrow("Injected peer failure");
  await Promise.resolve();
  await Promise.resolve();
  expect(reads).toBe(2);
  expect(budget.usedBytes).toBeGreaterThan(0);
  release?.();
  await rejected;
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("retires one decoded peer independently while another survives replacement", async () => {
  const { file, plan, values, entries } = await fixture();
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  const owners = await plan.executeOwned(file, scope, 2);
  scope.dispose();
  const first = owners[0],
    second = owners[1];
  if (!first || !second || !entries[0]) throw new Error("Missing peers");
  expect(first.bytes).toBe(64 * 4);
  expect(second.bytes).toBe(64 * 8 + 64);
  expect(budget.usedBytes).toBe(first.bytes + second.bytes);
  first.release();
  expect(budget.usedBytes).toBe(second.bytes);
  const replacement = planNumericPageGroup([entries[0]], file);
  if (!replacement) throw new Error("Missing replacement plan");
  const nextScope = new MemoryScope(budget);
  const next = await replacement.executeOwned(file, nextScope, 1);
  nextScope.dispose();
  if (!next[0]) throw new Error("Missing replacement");
  expect(materializeBatchRows(batchFromVectors({ a: next[0].value, b: second.value }))).toEqual(
    values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })),
  );
  expect(budget.usedBytes).toBe(next[0].bytes + second.bytes);
  next[0].release();
  expect(budget.usedBytes).toBe(second.bytes);
  second.release();
  expect(budget.usedBytes).toBe(0);
});

it("queues real page groups across jobs while allowing retained outputs within the pool", async () => {
  const { file, plan } = await fixture();
  const probeScope = new MemoryScope(new MemoryBudget(plan.bytes));
  const probeOwners = await plan.executeOwned(file, probeScope);
  probeScope.dispose();
  const retained = probeOwners.reduce((total, owner) => total + owner.bytes, 0);
  for (const owner of probeOwners) owner.release();
  expect(retained).toBeLessThan(plan.bytes);
  const root = new MemoryBudget(plan.bytes + retained);
  const scheduler = new MemoryStageScheduler(root);
  let open: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    open = resolve;
  });
  let secondStarted = false;
  const first = scheduler.run({
    budget: root.child(),
    bytes: plan.bytes,
    purpose: "first numeric group",
    async execute(scope) {
      await gate;
      return plan.execute(file, scope);
    },
  });
  const second = scheduler.run({
    budget: root.child(),
    bytes: plan.bytes,
    purpose: "second numeric group",
    async execute(scope) {
      secondStarted = true;
      return plan.execute(file, scope);
    },
  });
  expect(scheduler.queuedStages).toBe(1);
  expect(secondStarted).toBe(false);
  open?.();
  const a = await first;
  const b = await second;
  expect(secondStarted).toBe(true);
  expect(a.value.map((vector) => batchFromVectors({ x: vector }).rowCount)).toEqual([64, 64]);
  expect(b.value.map((vector) => batchFromVectors({ x: vector }).rowCount)).toEqual([64, 64]);
  expect(root.peakBytes).toBeLessThanOrEqual(root.maxBytes);
  expect(root.usedBytes).toBe(retained * 2);
  a.release();
  expect(root.usedBytes).toBe(retained);
  b.release();
  expect(root.usedBytes).toBe(0);
});

it("aligns unequal real page boundaries and preserves an output across early closure", async () => {
  const { file, entries, values, metadata } = await fixture(512, 128);
  const boundaries: number[][] = [[], []];
  const makeSources = (scope: MemoryScope) =>
    entries.map((entry, i) => ({
      column: i === 0 ? "a" : "b",
      pages: (async function* () {
        const column = metadata.row_groups[0]?.columns[i]?.meta_data;
        if (!column) throw new Error("Missing column");
        const start = Number(column.data_page_offset);
        const end = start + Number(column.total_compressed_size);
        let offset = 0,
          rowOffset = 0;
        while (start + offset < end) {
          const page = await preparePageWindow(file, start, end, offset, scope);
          if (!page) return;
          const rowCount = page.header.data_page_header_v2?.num_rows;
          if (rowCount === undefined) throw new Error("Missing row count");
          boundaries[i]?.push(rowOffset + rowCount);
          yield { page, decoder: entry.decoder, rowOffset, rowCount };
          rowOffset += rowCount;
          offset = page.bodyEnd - start;
        }
      })(),
    }));
  let peak = 0;
  for (const batchSize of [1, 13]) {
    const budget = new MemoryBudget(peak || 1024 * 1024);
    const scope = new MemoryScope(budget);
    const rows = [];
    for await (const owner of readAlignedNumericPageGroups(file, makeSources(scope), scope, {
      batchSize,
      maxConcurrentReads: 2,
    })) {
      expect(owner.value.rowOffset).toBe(rows.length);
      expect(owner.value.batch.rowCount).toBeLessThanOrEqual(batchSize);
      rows.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(rows).toEqual(values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })));
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
    if (peak === 0) peak = budget.peakBytes;
    else expect(budget.peakBytes).toBeLessThanOrEqual(peak);
  }
  expect(boundaries[0]).not.toEqual(boundaries[1]);
  const budget = new MemoryBudget(peak);
  const scope = new MemoryScope(budget);
  const iterator = readAlignedNumericPageGroups(file, makeSources(scope), scope, { batchSize: 1 })[
    Symbol.asyncIterator
  ]();
  const first = await iterator.next();
  if (first.done) throw new Error("Missing first batch");
  await iterator.return?.();
  scope.dispose();
  expect(budget.usedBytes).toBeGreaterThan(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual([{ a: 0, b: null }]);
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it("rejects a gap before replacement body reads and closes every prepared source", async () => {
  const { file, entries, plan } = await fixture();
  let reads = 0,
    closed = 0;
  const source = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      reads++;
      if (!file.sliceOwned) throw new Error("Missing owned read");
      return file.sliceOwned(...args);
    },
  };
  const sources = entries.map((entry, i) => ({
    column: i === 0 ? "a" : "b",
    pages: (async function* () {
      try {
        yield { ...entry, rowOffset: 0, rowCount: 64 };
        yield { ...entry, rowOffset: i === 0 ? 65 : 64, rowCount: 64 };
      } finally {
        closed++;
      }
    })(),
  }));
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  const run = async () => {
    for await (const owner of readAlignedNumericPageGroups(source, sources, scope)) owner.release();
  };
  await expect(run()).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
  expect(reads).toBe(2);
  expect(closed).toBe(2);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("prepares and aligns row windows inside unequal pages without loading excluded bodies", async () => {
  const { file, entries, values, metadata } = await fixture(512, 128);
  for (const [start, end] of [
    [123, 187],
    [0, 1],
    [511, 512],
    [100, 100],
  ]) {
    if (start === undefined || end === undefined) throw new Error("Missing row extent");
    const budget = new MemoryBudget(8192);
    const scope = new MemoryScope(budget);
    const reads: [number, number][] = [];
    const source = {
      ...file,
      async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
        reads.push([args[0], args[1]]);
        if (!file.sliceOwned) throw new Error("Missing owned read");
        return file.sliceOwned(...args);
      },
    };
    const excluded: [number, number][] = [];
    const sources = [];
    for (const [i, entry] of entries.entries()) {
      const column = metadata.row_groups[0]?.columns[i]?.meta_data;
      if (!column) throw new Error("Missing column");
      const chunkStart = Number(column.data_page_offset);
      const extent = { start: chunkStart, end: chunkStart + Number(column.total_compressed_size) };
      const inspection = new MemoryScope(new MemoryBudget(8192));
      for await (const page of prepareNumericColumnPages(file, entry.decoder, extent, inspection, {
        start: 0,
        end: 512,
      })) {
        if (page.rowOffset + page.rowCount <= start || page.rowOffset >= end)
          excluded.push([page.page.bodyOffset, page.page.bodyEnd]);
      }
      inspection.dispose();
      sources.push({
        column: i === 0 ? "a" : "b",
        pages: prepareNumericColumnPages(source, entry.decoder, extent, scope, { start, end }),
      });
    }
    const rows = [];
    for await (const owner of readAlignedNumericPageGroups(source, sources, scope, {
      rowStart: start,
      rowEnd: end,
      batchSize: 7,
    })) {
      expect(owner.value.rowOffset).toBe(start + rows.length);
      rows.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(rows).toEqual(
      values.slice(start, end).map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })),
    );
    for (const range of excluded) expect(reads).not.toContainEqual(range);
    if (start === end) expect(reads).toHaveLength(0);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it("includes cold cache workspace and reuses shared backing across group wrappers", async () => {
  const { file, entries, values } = await fixture();
  const root = new MemoryBudget(16384);
  const cache = new SharedMemoryCache({ memoryBudget: root, maxBytes: 4096 });
  let reads = 0;
  const source = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      reads++;
      if (!file.sliceOwned) throw new Error("Missing owned read");
      return file.sliceOwned(...args);
    },
  };
  let coldReads = 0;
  for (let run = 0; run < 2; run++) {
    const cached = cachedRangeBuffer(
      source,
      { maxBytes: 4096, coalesceBytes: 256, sharedCache: cache },
      "group-cache",
    );
    const plan = planNumericPageGroup(entries, cached);
    if (!plan) throw new Error("Missing cache group capability");
    const scope = new MemoryScope(root.child(8192));
    const owners = await plan.executeOwned(cached, scope);
    scope.dispose();
    if (!owners[0] || !owners[1]) throw new Error("Missing cached vectors");
    expect(
      materializeBatchRows(batchFromVectors({ a: owners[0].value, b: owners[1].value })),
    ).toEqual(values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })));
    for (const owner of owners) owner.release();
    expect(scope.budget.usedBytes).toBe(0);
    if (run === 0) {
      coldReads = reads;
      expect(coldReads).toBeGreaterThan(0);
    } else expect(reads).toBe(coldReads);
  }
  expect(root.usedBytes).toBeGreaterThan(0);
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
  const { ownedReadAllocationBytes: _capability, ...unknown } = file;
  expect(planNumericPageGroup(entries, unknown)).toBeUndefined();
});

it("rejects equal premature exhaustion while preserving retained output ownership", async () => {
  const { file, entries, plan } = await fixture();
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  let closed = 0;
  const sources = entries.map((entry, i) => ({
    column: i === 0 ? "a" : "b",
    pages: (async function* () {
      try {
        yield { ...entry, rowOffset: 0, rowCount: 64 };
      } finally {
        closed++;
      }
    })(),
  }));
  const iterator = readAlignedNumericPageGroups(file, sources, scope, {
    rowEnd: 65,
    batchSize: 64,
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("Missing retained batch");
  await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
  expect(closed).toBe(2);
  scope.dispose();
  expect(materializeBatchRows(first.value.value.batch)).toHaveLength(64);
  expect(budget.usedBytes).toBeGreaterThan(0);
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it("rejects a truncated real column source before treating its prefix as complete", async () => {
  const { file, entries } = await fixture(512, 128);
  const entry = entries[0];
  if (!entry) throw new Error("Missing column");
  const budget = new MemoryBudget(8192);
  const scope = new MemoryScope(budget);
  let prepared = 0;
  const run = async () => {
    for await (const page of prepareNumericColumnPages(
      file,
      entry.decoder,
      { start: entry.page.bodyOffset - entry.page.headerBytes, end: entry.page.bodyEnd },
      scope,
      { start: 0, end: 512 },
    )) {
      expect(page.rowCount).toBeLessThan(512);
      prepared++;
    }
  };
  await expect(run()).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
  expect(prepared).toBe(1);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("reuses decoded groups under an output-only job cap and survives cache eviction", async () => {
  const { file, entries, plan, values } = await fixture();
  const root = new MemoryBudget(plan.bytes + 8192);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8192 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 8192, policy: "latency" });
  let reads = 0;
  const source = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      reads++;
      if (!file.sliceOwned) throw new Error("Missing owned read");
      return file.sliceOwned(...args);
    },
  };
  const sources = () =>
    entries.map((entry, i) => ({
      column: i === 0 ? "a" : "b",
      pages: (async function* () {
        yield { ...entry, rowOffset: 0, rowCount: 64 };
      })(),
    }));
  const options = {
    rowEnd: 64,
    batchSize: 64,
    decodedCache: { cache, path: "group.parquet", rowGroupStart: 0 },
  };
  const cold = new MemoryScope(root.child(plan.bytes));
  for await (const owner of readAlignedNumericPageGroups(source, sources(), cold, options))
    owner.release();
  cold.dispose();
  expect(reads).toBe(2);
  expect(cold.budget.usedBytes).toBe(0);
  const retained = root.usedBytes;
  expect(retained).toBe(832);
  const warm = new MemoryScope(root.child(retained));
  const iterator = readAlignedNumericPageGroups(source, sources(), warm, options)[
    Symbol.asyncIterator
  ]();
  const next = await iterator.next();
  if (next.done) throw new Error("Missing cached output");
  expect(reads).toBe(2);
  expect(warm.budget.usedBytes).toBe(retained);
  shared.deleteWhere(() => true);
  await iterator.return?.();
  warm.dispose();
  expect(materializeBatchRows(next.value.value.batch)).toEqual(
    values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })),
  );
  expect(root.usedBytes).toBe(retained);
  next.value.release();
  expect(root.usedBytes).toBe(0);
});

it("releases cached peers when an uncached grouped read fails", async () => {
  const { file, entries, plan } = await fixture();
  const root = new MemoryBudget(plan.bytes + 8192);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8192 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 8192, policy: "latency" });
  const options = { rowEnd: 64, decodedCache: { cache, path: "group.parquet", rowGroupStart: 0 } };
  const sources = (selected: NumericPageGroupEntry[]) =>
    selected.map((entry, i) => ({
      column: i === 0 ? "a" : "b",
      pages: (async function* () {
        yield { ...entry, rowOffset: 0, rowCount: 64 };
      })(),
    }));
  const seed = new MemoryScope(root.child(plan.bytes));
  for await (const owner of readAlignedNumericPageGroups(
    file,
    sources(entries.slice(0, 1)),
    seed,
    options,
  ))
    owner.release();
  seed.dispose();
  expect(root.usedBytes).toBe(256);
  let reads = 0;
  const failure = new Error("Uncached peer read failed");
  const source = {
    ...file,
    async sliceOwned() {
      reads++;
      throw failure;
    },
  };
  const scope = new MemoryScope(root.child(plan.bytes));
  const run = async () => {
    for await (const owner of readAlignedNumericPageGroups(
      source,
      sources(entries),
      scope,
      options,
    ))
      owner.release();
  };
  await expect(run()).rejects.toBe(failure);
  expect(reads).toBe(1);
  expect(scope.budget.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(256);
  scope.dispose();
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("does not publish decoded cache entries with inconsistent prepared row extents", async () => {
  const { file, entries, plan } = await fixture();
  const root = new MemoryBudget(plan.bytes + 8192);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8192 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 8192, policy: "latency" });
  const scope = new MemoryScope(root.child(plan.bytes));
  const sources = entries.map((entry, i) => ({
    column: i === 0 ? "a" : "b",
    pages: (async function* () {
      yield { ...entry, rowOffset: 0, rowCount: 65 };
    })(),
  }));
  const run = async () => {
    for await (const owner of readAlignedNumericPageGroups(file, sources, scope, {
      rowEnd: 65,
      decodedCache: { cache, path: "group.parquet", rowGroupStart: 0 },
    }))
      owner.release();
  };
  await expect(run()).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
  expect(scope.budget.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(0);
  scope.dispose();
});

it.each([
  "before-first",
  "during-preparation",
  "between-batches",
] as const)("cancels cached grouped reads %s without losing retained output ownership", async (when) => {
  const { file, entries, plan } = await fixture();
  const root = new MemoryBudget(plan.bytes + 8192);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 8192 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 8192, policy: "latency" });
  const options = {
    rowEnd: 64,
    batchSize: 1,
    decodedCache: { cache, path: "group.parquet", rowGroupStart: 0 },
  };
  let closed = 0;
  const sources = (scope?: MemoryScope) =>
    entries.map((entry, i) => ({
      column: i === 0 ? "a" : "b",
      pages: (async function* () {
        try {
          if (when === "during-preparation" && i === 1) scope?.dispose();
          yield { ...entry, rowOffset: 0, rowCount: 64 };
        } finally {
          closed++;
        }
      })(),
    }));
  const seed = new MemoryScope(root.child(plan.bytes));
  for await (const owner of readAlignedNumericPageGroups(file, sources(), seed, options))
    owner.release();
  seed.dispose();
  closed = 0;
  expect(root.usedBytes).toBe(832);
  let reads = 0;
  const source = {
    ...file,
    async sliceOwned() {
      reads++;
      throw new Error("Unexpected warm read");
    },
  };
  const scope = new MemoryScope(root.child(832));
  const iterator = readAlignedNumericPageGroups(source, sources(scope), scope, options)[
    Symbol.asyncIterator
  ]();
  if (when === "before-first") scope.dispose();
  if (when === "between-batches") {
    const first = await iterator.next();
    if (first.done) throw new Error("Missing retained output");
    scope.dispose();
    shared.deleteWhere(() => true);
    await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(materializeBatchRows(first.value.value.batch)).toEqual([{ a: 0, b: null }]);
    expect(root.usedBytes).toBe(832);
    first.value.release();
  } else {
    await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    shared.deleteWhere(() => true);
  }
  expect(closed).toBe(when === "before-first" ? 0 : 2);
  expect(reads).toBe(0);
  expect(scope.budget.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("schedules aligned replacements across jobs (cancel queued=%s)", async (cancelQueued) => {
  const { file, entries, plan } = await fixture();
  const root = new MemoryBudget(plan.bytes + 832);
  const scheduler = new MemoryStageScheduler(root);
  const firstScope = new MemoryScope(root.child(plan.bytes));
  const secondScope = new MemoryScope(root.child(plan.bytes));
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let firstReads = 0;
  let secondReads = 0;
  const firstFile = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      firstReads++;
      entered.resolve();
      await release.promise;
      if (!file.sliceOwned) throw new Error("Missing owned read");
      return file.sliceOwned(...args);
    },
  };
  const secondFile = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      secondReads++;
      if (!file.sliceOwned) throw new Error("Missing owned read");
      return file.sliceOwned(...args);
    },
  };
  const sources = () =>
    entries.map((entry, i) => ({
      column: i === 0 ? "a" : "b",
      pages: (async function* () {
        yield { ...entry, rowOffset: 0, rowCount: 64 };
      })(),
    }));
  const options = { rowEnd: 64, batchSize: 64, scheduler };
  const first = readAlignedNumericPageGroups(firstFile, sources(), firstScope, options)[
    Symbol.asyncIterator
  ]();
  const second = readAlignedNumericPageGroups(secondFile, sources(), secondScope, options)[
    Symbol.asyncIterator
  ]();
  const aPending = first.next();
  await entered.promise;
  const bPending = second.next();
  const bOutcome = bPending.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(scheduler.queuedStages).toBe(1);
  expect(secondReads).toBe(0);
  if (cancelQueued) secondScope.dispose();
  release.resolve();
  const a = await aPending;
  if (a.done) throw new Error("Missing first output");
  const b = await bOutcome;
  if (cancelQueued) {
    expect(b).toMatchObject({ error: { code: "LAKEQL_ABORTED" } });
    expect(secondReads).toBe(0);
  } else {
    if (!("value" in b) || b.value.done) throw new Error("Missing second output");
    expect(materializeBatchRows(b.value.value.value.batch)).toHaveLength(64);
    b.value.value.release();
    expect(secondReads).toBe(2);
  }
  expect(firstReads).toBe(2);
  expect(scheduler.queuedStages).toBe(0);
  expect(scheduler.activeStages).toBe(0);
  await first.return?.();
  await second.return?.();
  firstScope.dispose();
  secondScope.dispose();
  expect(root.usedBytes).toBe(832);
  a.value.release();
  expect(root.usedBytes).toBe(0);
  expect(root.peakBytes).toBeLessThanOrEqual(root.maxBytes);
});

it("uses existing admission when alignment runs inside a scheduled stage", async () => {
  const { file, entries, plan } = await fixture();
  const root = new MemoryBudget(plan.bytes);
  const scheduler = new MemoryStageScheduler(root);
  const owner = await scheduler.run({
    budget: root.child(plan.bytes),
    bytes: plan.bytes,
    purpose: "outer numeric scan",
    async execute(scope) {
      const sources = entries.map((entry, i) => ({
        column: i === 0 ? "a" : "b",
        pages: (async function* () {
          yield { ...entry, rowOffset: 0, rowCount: 64 };
        })(),
      }));
      let count = 0;
      for await (const output of readAlignedNumericPageGroups(file, sources, scope, {
        scheduler,
        rowEnd: 64,
      })) {
        count += output.value.batch.rowCount;
        output.release();
      }
      return count;
    },
  });
  expect(owner.value).toBe(64);
  owner.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("measures ZSTD inputs when header bounds exceed the job cap (scheduled=%s)", async (scheduled) => {
  const { file, entries, plan, values } = await fixture(1, 1 << 20, "ZSTD");
  expect(plan.bytes).toBeGreaterThan(60000);
  const root = new MemoryBudget(8192);
  const scope = new MemoryScope(root.child(8192));
  const scheduler = scheduled ? new MemoryStageScheduler(root) : undefined;
  const sources = entries.map((entry, i) => ({
    column: i === 0 ? "a" : "b",
    pages: (async function* () {
      yield { ...entry, rowOffset: 0, rowCount: 1 };
    })(),
  }));
  const rows = [];
  let decodes = 0;
  for await (const owner of readAlignedNumericPageGroups(file, sources, scope, {
    rowEnd: 1,
    decode(work) {
      decodes++;
      return work();
    },
    ...(scheduler === undefined ? {} : { scheduler }),
  })) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })));
  expect(decodes).toBe(entries.length);
  expect(root.peakBytes).toBeLessThanOrEqual(8192);
  expect(root.usedBytes).toBe(0);
  scope.dispose();
});

it("releases measured inputs when actual ZSTD workspace still exceeds the cap", async () => {
  const { file, entries } = await fixture(64, 1 << 20, "ZSTD");
  const root = new MemoryBudget(8192);
  const scope = new MemoryScope(root);
  const sources = entries.map((entry, i) => ({
    column: i === 0 ? "a" : "b",
    pages: (async function* () {
      yield { ...entry, rowOffset: 0, rowCount: 64 };
    })(),
  }));
  const run = async () => {
    for await (const owner of readAlignedNumericPageGroups(file, sources, scope, { rowEnd: 64 }))
      owner.release();
  };
  await expect(run()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(root.usedBytes).toBe(0);
  expect(root.peakBytes).toBeLessThanOrEqual(8192);
  scope.dispose();
});

it("cleans measured inputs after a later transport failure", async () => {
  const { file, plan } = await fixture();
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  let reads = 0;
  const failure = new Error("Measured second input failed");
  const source = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      if (++reads === 2) throw failure;
      if (!file.sliceOwned) throw new Error("Missing owned read");
      return file.sliceOwned(...args);
    },
  };
  await expect(plan.measureOwned(source, scope)).rejects.toBe(failure);
  expect(reads).toBe(2);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("replays measured transport and decode at the exact observed allocation boundary", async () => {
  const { file, plan } = await fixture(1, 1 << 20, "ZSTD");
  const run = async (limit: number) => {
    const budget = new MemoryBudget(limit);
    const scope = new MemoryScope(budget);
    let measured: Awaited<ReturnType<typeof plan.measureOwned>> | undefined;
    try {
      measured = await plan.measureOwned(file, scope);
      const owners = await measured.executeOwned(file, scope);
      measured.dispose();
      scope.dispose();
      expect(owners.map((owner) => batchFromVectors({ x: owner.value }).rowCount)).toEqual([1, 1]);
      for (const owner of owners) owner.release();
      return budget.peakBytes;
    } finally {
      measured?.dispose();
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = await run(8192);
  expect(peak).toBeLessThan(8192);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("routes ordinary owned scans through parallel groups and preserves retained output", async () => {
  const { file, metadata } = await fixture(512, 128);
  const budget = new MemoryBudget(8192);
  const scope = new MemoryScope(budget);
  let active = 0,
    peak = 0;
  const source = {
    ...file,
    async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
      active++;
      peak = Math.max(peak, active);
      try {
        await new Promise((resolve) => setTimeout(resolve, 0));
        if (!file.sliceOwned) throw new Error("Missing owned read");
        return await file.sliceOwned(...args);
      } finally {
        active--;
      }
    },
  };
  const iterator = readParquetVectorBatchesFromFile(source, metadata, {
    columns: ["a", "b"],
    rowStart: 123,
    rowEnd: 187,
    batchSize: 7,
    allocationScope: scope,
    pageAllocationLifetime: "iteration",
    canStopEarly: true,
    maxConcurrentReads: 2,
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("Missing ordinary grouped output");
  const retained = scope.retainAllocations(first.value.batch);
  expect(first.value.rowOffset).toBe(123);
  expect(peak).toBe(2);
  await iterator.return?.();
  scope.dispose();
  expect(materializeBatchRows(retained.value)).toEqual(
    Array.from({ length: first.value.batch.rowCount }, (_, n) => {
      const i = 123 + n;
      return { a: i, b: i % 3 ? i + 0.5 : null };
    }),
  );
  expect(budget.usedBytes).toBeGreaterThan(0);
  retained.release();
  expect(budget.usedBytes).toBe(0);
  expect(active).toBe(0);
});

it("rejects invalid alignment concurrency even for empty sources", async () => {
  const { file } = await fixture();
  const scope = new MemoryScope(new MemoryBudget(1024));
  for (const maxConcurrentReads of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    const iterator = readAlignedNumericPageGroups(file, [], scope, { maxConcurrentReads })[
      Symbol.asyncIterator
    ]();
    await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  }
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("selects chunk reads with headroom and grouped pages under memory pressure", async () => {
  const { readOwnedParquetVectorBatches } = await import("./owned-vector-batches.js");
  const { file, metadata, values } = await fixture(512, 128);
  async function run(limit: number) {
    const budget = new MemoryBudget(limit);
    let requests = 0;
    const source = {
      ...file,
      async sliceOwned(...args: Parameters<NonNullable<typeof file.sliceOwned>>) {
        requests++;
        if (!file.sliceOwned) throw new Error("Missing owned read");
        return file.sliceOwned(...args);
      },
    };
    const rows = [];
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["a", "b"],
      batchSize: 31,
    })) {
      rows.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(rows).toEqual(values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })));
    expect(budget.usedBytes).toBe(0);
    return { requests, peak: budget.peakBytes };
  }
  const ample = await run(65536);
  const tight = await run(8192);
  expect(tight.requests).toBeGreaterThan(ample.requests);
  expect(tight.peak).toBeLessThan(ample.peak);
});

it.each([
  "UNCOMPRESSED",
  "SNAPPY",
] as const)("preserves measured %s output after input-plan disposal", async (codec) => {
  const { file, plan, values } = await fixture(64, 1 << 20, codec);
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  const measured = await plan.measureOwned(file, scope);
  const owners = await measured.executeOwned(file, scope);
  measured.dispose();
  scope.dispose();
  const [a, b] = owners;
  if (a === undefined || b === undefined) throw new Error("Missing measured vectors");
  expect(materializeBatchRows(batchFromVectors({ a: a.value, b: b.value }))).toEqual(
    values.map((i) => ({ a: i, b: i % 3 ? i + 0.5 : null })),
  );
  expect(budget.usedBytes).toBeGreaterThan(0);
  for (const owner of owners) owner.release();
  expect(budget.usedBytes).toBe(0);
});

it("rejects measured execution with a different job or disposed input plan", async () => {
  const { file, plan } = await fixture();
  const root = new MemoryBudget(plan.bytes * 2);
  const scope = new MemoryScope(root.child(plan.bytes));
  const other = new MemoryScope(root.child(plan.bytes));
  const measured = await plan.measureOwned(file, scope);
  const inputBytes = root.usedBytes;
  await expect(measured.executeOwned(file, other)).rejects.toMatchObject({
    code: "LAKEQL_VALIDATION_ERROR",
  });
  expect(other.budget.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(inputBytes);
  measured.dispose();
  await expect(measured.executeOwned(file, scope)).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  expect(root.usedBytes).toBe(0);
  scope.dispose();
  other.dispose();
});

it("rejects measured execution after its source scope closes and releases retained inputs", async () => {
  const { file, plan } = await fixture();
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  const measured = await plan.measureOwned(file, scope);
  scope.dispose();
  expect(budget.usedBytes).toBeGreaterThan(0);
  await expect(measured.executeOwned(file, scope)).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  measured.dispose();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "UNCOMPRESSED",
  "SNAPPY",
  "ZSTD",
] as const)("groups real boolean and numeric columns under %s", async (codec) => {
  const values = Array.from({ length: 64 }, (_, i) => i);
  const flags = values.map((i) => (i % 3 === 0 ? null : i % 2 === 0));
  const bytes = parquetWriteBuffer({
    codec,
    compressors: { ZSTD: zstdCompressSync },
    pageSize: 128,
    columnData: [
      { name: "id", type: "INT32", encoding: "PLAIN", data: values },
      { name: "flag", type: "BOOLEAN", encoding: "PLAIN", data: flags },
    ],
  });
  const store = memoryStore();
  await store.put("boolean-group.parquet", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "boolean-group.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  const budget = new MemoryBudget(131072);
  const scope = new MemoryScope(budget);
  const rows = [];
  for await (const output of readParquetVectorBatchesFromFile(file, metadata, {
    columns: ["id", "flag"],
    rowStart: 13,
    rowEnd: 61,
    batchSize: 7,
    allocationScope: scope,
    pageAllocationLifetime: "iteration",
    canStopEarly: true,
  })) {
    expect(output.rowOffset).toBe(13 + rows.length);
    rows.push(...materializeBatchRows(output.batch));
  }
  expect(rows).toEqual(values.slice(13, 61).map((id) => ({ id, flag: flags[id] })));
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBeLessThanOrEqual(budget.maxBytes);
  scope.dispose();
});

it.each([
  ["a"],
  ["a", "b"],
])("reuses real window headers and decoded bodies for columns %j", async (...columns) => {
  const { file, metadata, values } = await fixture();
  const root = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 1024 * 1024 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 1024 * 1024 });
  let reads = 0;
  if (!file.sliceOwned) throw Error("Missing owned reads");
  const readOwned = file.sliceOwned.bind(file);
  const source = {
    ...file,
    sliceOwned: (...args: Parameters<typeof readOwned>) => {
      reads++;
      return readOwned(...args);
    },
  };
  const options = {
    columns,
    rowStart: 1,
    rowEnd: 4,
    batchSize: 2,
    decodedColumnCache: cache,
    decodedColumnCacheKey: "group.parquet",
  };
  for await (const output of readOwnedParquetVectorBatches(source, metadata, root, options))
    output.release();
  expect(reads).toBeGreaterThan(0);
  reads = 0;
  const retained = [];
  for await (const output of readOwnedParquetVectorBatches(source, metadata, root, options))
    retained.push(output);
  expect(reads).toBe(0);
  shared.deleteWhere(() => true);
  const rows = retained.flatMap((owner) => materializeBatchRows(owner.value.batch));
  expect(rows).toEqual(
    values
      .slice(1, 4)
      .map((i) => (columns.length === 1 ? { a: i } : { a: i, b: i % 3 ? i + 0.5 : null })),
  );
  for (const owner of retained) owner.release();
  expect(root.usedBytes).toBe(0);
});

it("avoids dictionary header and body reads on a warm deferred string window", async () => {
  const values = Array.from({ length: 64 }, (_, i) => ["west", "east", "north", "south"][i % 4]);
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    columnData: [{ name: "region", type: "STRING", encoding: "RLE_DICTIONARY", data: values }],
  });
  const store = memoryStore();
  await store.put("dictionary.parquet", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "dictionary.parquet");
  const metadata = await readParquetMetadataFromFile(file);
  if (!file.sliceOwned) throw Error("Missing owned transport");
  const read = file.sliceOwned.bind(file);
  let reads = 0;
  const source = {
    ...file,
    sliceOwned: (...args: Parameters<typeof read>) => {
      reads++;
      return read(...args);
    },
  };
  const root = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ maxBytes: root.maxBytes, memoryBudget: root });
  const cache = new DecodedColumnCache(shared, { maxBytes: root.maxBytes });
  const options = {
    columns: ["region"],
    rowStart: 1,
    rowEnd: 4,
    decodedColumnCache: cache,
    decodedColumnCacheKey: "dictionary.parquet",
  };
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, root, options))
    owner.release();
  expect(reads).toBeGreaterThan(0);
  reads = 0;
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, root, options)) {
    shared.deleteWhere(() => true);
    expect(materializeBatchRows(owner.value.batch)).toEqual(
      values.slice(1, 4).map((region) => ({ region })),
    );
    owner.release();
  }
  expect(reads).toBe(0);
  expect(root.usedBytes).toBe(0);
});
