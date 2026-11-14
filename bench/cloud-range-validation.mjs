// Read-only validation against the project's public R2 fixture. Run after pnpm build.
import assert from "node:assert/strict";
import {
  col,
  gt,
  MemoryBudget,
  MemoryScope,
  mul,
  SharedMemoryCache,
  timestampFromEpoch,
  withObjectStoreReadControls,
} from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { asyncBufferFromStore, createParquetLake } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { readParquetObjectBatchesFromFile } from "../packages/parquet/dist/object-batches.js";
import { cachedRangeBuffer } from "../packages/parquet/dist/range-cache.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const path = "window-events.parquet";
const reports = [];
const queryHeadroom = Number(process.env.LAKEQL_CLOUD_QUERY_HEADROOM ?? 128 * 1024);
assert.ok(
  Number.isSafeInteger(queryHeadroom) && queryHeadroom > 0 && queryHeadroom < 64 * 1024 * 1024,
);

function measuredStore() {
  const metrics = {
    requests: 0,
    bytes: 0,
    active: 0,
    peak: 0,
    heads: 0,
    httpRangeRequests: 0,
    httpResponseBytes: 0,
  };
  const adapter = httpStore({
    baseUrl,
    fetch: async (input, init) => {
      const response = await fetch(input, {
        ...init,
        signal: AbortSignal.any([
          AbortSignal.timeout(30_000),
          ...(init?.signal ? [init.signal] : []),
        ]),
      });
      if (init?.method !== "HEAD") {
        assert.ok(new Headers(init?.headers).has("Range"), "Unexpected full-object request");
        assert.equal(response.status, 206, "Cloud endpoint must honor byte ranges");
        metrics.httpRangeRequests += 1;
        const length = Number(response.headers.get("Content-Length"));
        assert.ok(Number.isSafeInteger(length) && length >= 0);
        assert.equal(response.headers.get("Content-Encoding"), null);
        metrics.httpResponseBytes += length;
      }
      return response;
    },
  });
  return {
    metrics,
    store: {
      get() {
        throw new Error("Full-object reads are forbidden in this validation");
      },
      async getRange(key, range) {
        metrics.requests += 1;
        metrics.active += 1;
        metrics.peak = Math.max(metrics.peak, metrics.active);
        try {
          const bytes = await adapter.getRange(key, range);
          assert.equal(bytes.byteLength, range.length);
          metrics.bytes += bytes.byteLength;
          return bytes;
        } finally {
          metrics.active -= 1;
        }
      },
      async getRangeOwned(key, range, scope) {
        metrics.requests += 1;
        metrics.active += 1;
        metrics.peak = Math.max(metrics.peak, metrics.active);
        try {
          const owner = await adapter.getRangeOwned(key, range, scope);
          assert.equal(owner.value.byteLength, range.length);
          metrics.bytes += owner.value.byteLength;
          return owner;
        } finally {
          metrics.active -= 1;
        }
      },
      async head(key) {
        metrics.heads += 1;
        return adapter.head(key);
      },
      list: adapter.list.bind(adapter),
      put() {
        throw new Error("Read-only validation");
      },
      delete() {
        throw new Error("Read-only validation");
      },
    },
  };
}

// Independent expected rows from the public fixture's documented generator contract.
const expected = [];
const expectedFullRows = [];
for (const region of ["north", "south", "west"]) {
  for (let accountIndex = 0; accountIndex < 4; accountIndex += 1) {
    for (let index = 0; index < 12; index += 1) {
      const peer = 100 - (1 + Math.floor(index / 2)) * 3 + (accountIndex % 2);
      expected.push({
        event_id: expected.length + 1,
        region,
        account: `${region}-${accountIndex + 1}`,
        amount: index % 5 === 4 ? null : 10 + accountIndex * 4 + index * 1.5,
        score: index % 3 === 0 ? peer : peer - (index % 3),
      });
      expectedFullRows.push({
        ...expected.at(-1),
        event_ts: timestampFromEpoch(
          BigInt(Date.UTC(2026, 1, 1 + Math.floor(index / 2), (index % 2) * 6 + accountIndex)),
          "millis",
        ),
        category: ["purchase", "refund", "fee", "purchase"][(index + accountIndex) % 4],
      });
    }
  }
}

for (const maxConcurrentReads of [1, 4]) {
  const { store, metrics } = measuredStore();
  const file = await asyncBufferFromStore(
    withObjectStoreReadControls(store, { maxConcurrentReads }),
    path,
  );
  assert.ok(file.byteLength >= 4096);
  const cached = cachedRangeBuffer(
    file,
    { maxBytes: 4096, coalesceBytes: 1024, sharedCache: new SharedMemoryCache({ maxBytes: 4096 }) },
    `${baseUrl}${path}`,
  );
  await Promise.all([
    cached.prefetch([
      { start: 0, end: 8 },
      { start: 1024, end: 1032 },
      { start: 2048, end: 2056 },
      { start: 3072, end: 3080 },
    ]),
    cached.slice(2, 4),
  ]);
  assert.equal(metrics.requests, 4);
  assert.equal(metrics.bytes, 4096);
  // HttpObjectStore.head probes size with a two-byte ranged GET, not HTTP HEAD.
  assert.equal(metrics.httpRangeRequests, 5);
  assert.equal(metrics.httpResponseBytes, 4098);
  assert.equal(metrics.peak, maxConcurrentReads);
  await cached.prefetch([{ start: 0, end: 8 }]);
  assert.equal(metrics.requests, 4);
  reports.push({ operation: "coalesced-prefetch-with-overlap", maxConcurrentReads, ...metrics });
}

for (const memoryMiB of [64, 128]) {
  const { store, metrics } = measuredStore();
  const lake = createParquetLake({
    store,
    budget: { maxMemoryBytes: memoryMiB * 1024 * 1024, maxConcurrentReads: 4 },
    scanRangeCache: { maxBytes: 4096, coalesceBytes: 1024 },
  });
  for (const temperature of ["cold", "warm"]) {
    const before = { requests: metrics.requests, bytes: metrics.bytes };
    const start = performance.now();
    const rows = await lake
      .path(path)
      .select(["event_id", "region", "account", "amount", "score"])
      .toArray();
    assert.deepEqual(rows, expected);
    assert.ok(metrics.peak <= 4);
    reports.push({
      operation: "decode-fixture",
      serializedBudgetMiB: memoryMiB,
      temperature,
      rows: rows.length,
      requests: metrics.requests - before.requests,
      bytes: metrics.bytes - before.bytes,
      peak: metrics.peak,
      elapsedMs: Number((performance.now() - start).toFixed(3)),
    });
  }
}

for (const memoryMiB of [64, 128]) {
  const { store, metrics } = measuredStore();
  const budget = new MemoryBudget(memoryMiB * 1024 * 1024);
  const cache = new SharedMemoryCache({ maxBytes: 4096, memoryBudget: budget });
  const file = await asyncBufferFromStore(
    withObjectStoreReadControls(store, { maxConcurrentReads: 4 }),
    path,
  );
  const cached = cachedRangeBuffer(
    file,
    { maxBytes: 4096, coalesceBytes: 1024, sharedCache: cache },
    `${baseUrl}${path}`,
  );
  const prefetchScope = new MemoryScope(budget.child(budget.maxBytes / 2));
  const demandScope = new MemoryScope(budget.child(budget.maxBytes / 2));
  try {
    const [, output] = await Promise.all([
      cached.prefetchOwned(
        [0, 1024, 2048, 3072].map((start) => ({ start, end: start + 8 })),
        prefetchScope,
      ),
      cached.sliceOwned(2, 4, demandScope),
    ]);
    assert.equal(output.value.byteLength, 2);
    assert.equal(metrics.requests, 4);
    assert.equal(metrics.bytes, 4096);
    assert.equal(metrics.peak, 4);
    assert.equal(budget.usedBytes, 4098);
    output.release();
    prefetchScope.dispose();
    demandScope.dispose();
    assert.equal(prefetchScope.budget.usedBytes, 0);
    assert.equal(demandScope.budget.usedBytes, 0);
    const nextJob = new MemoryScope(budget.child(budget.maxBytes / 2));
    try {
      const reused = await cached.sliceOwned(2, 4, nextJob);
      assert.equal(reused.value.byteLength, 2);
      assert.equal(nextJob.budget.usedBytes, 2);
      assert.equal(metrics.requests, 4);
    } finally {
      nextJob.dispose();
    }
    assert.equal(nextJob.budget.usedBytes, 0);
    reports.push({
      operation: "owned-coalesced-prefetch",
      allocationPoolMiB: memoryMiB,
      chargedBytes: budget.usedBytes,
      peakChargedBytes: budget.peakBytes,
      ...metrics,
    });
  } finally {
    prefetchScope.dispose();
    demandScope.dispose();
    cache.deleteWhere(() => true);
  }
  assert.equal(budget.usedBytes, 0);
  for (const temperature of ["cold", "warm"]) {
    const scope = new MemoryScope(budget.child());
    const before = { requests: metrics.requests, bytes: metrics.bytes };
    const started = performance.now();
    try {
      // Keep source backing alive for the complete decode stage. JS decoded
      // objects remain outside this read-allocation validation.
      const ownedFile = {
        byteLength: file.byteLength,
        async slice(start, end = file.byteLength) {
          return (await cached.sliceOwned(start, end, scope)).value;
        },
      };
      const metadata = await readParquetMetadataFromFile(ownedFile);
      const rows = [];
      for await (const batch of readParquetObjectBatchesFromFile(ownedFile, metadata, {
        columns: ["event_id", "region", "account", "amount", "score"],
        batchSize: 144,
      }))
        rows.push(...batch.rows);
      assert.deepEqual(rows, expected);
      assert.ok(budget.peakBytes <= budget.maxBytes);
      reports.push({
        operation: "owned-decode-fixture",
        allocationPoolMiB: memoryMiB,
        temperature,
        rows: rows.length,
        requests: metrics.requests - before.requests,
        bytes: metrics.bytes - before.bytes,
        chargedBytes: budget.usedBytes,
        peakChargedBytes: budget.peakBytes,
        elapsedMs: Number((performance.now() - started).toFixed(3)),
      });
    } finally {
      scope.dispose();
    }
  }
  cache.deleteWhere(() => true);
  assert.equal(budget.usedBytes, 0);
}

for (const scenario of ["aggregate", "top-k", "projection", "one-row-batches", "mixed"]) {
  for (const memoryMiB of [64, 128]) {
    for (const concurrency of [1, 4]) {
      const { store: rawStore, metrics } = measuredStore();
      const store = withObjectStoreReadControls(rawStore, { maxConcurrentReads: 4 });
      const root = new MemoryBudget(memoryMiB * 1024 * 1024);
      const headroom = queryHeadroom;
      const cache = new SharedMemoryCache({ maxBytes: root.maxBytes, memoryBudget: root });
      const seed = new MemoryScope(root);
      const occupied = root.maxBytes - headroom;
      const backing = seed.allocate(occupied, "existing reusable cache", () =>
        new Uint8Array(occupied).fill(1),
      );
      assert.equal(cache.setOwned("existing-cache", backing, { priority: 2 }), true);
      const pin = cache.acquireOwned("existing-cache");
      assert.ok(pin);
      seed.dispose();
      const lake = createParquetLake({
        store,
        memoryPool: root,
        budget: { maxAllocatedMemoryBytes: headroom, maxConcurrentReads: 4 },
        scanRangeCache: { maxBytes: 65536, coalesceBytes: 1024, sharedCache: cache },
      });
      const total = expected.reduce((sum, row) => sum + (row.amount ?? 0), 0);
      try {
        for (const temperature of ["cold", "warm"]) {
          const before = { ...metrics };
          const started = performance.now();
          let oneRowBatchesVerified = 0;
          const query = async (kind) => {
            const base = lake.path(path).batchSize(kind === "one-row-batches" ? 1 : 32);
            if (kind === "one-row-batches") {
              const rows = [];
              for await (const batch of base
                .project({ doubled: mul(col("amount"), 2) })
                .batches()) {
                assert.equal(batch.length, 1);
                oneRowBatchesVerified++;
                rows.push(batch[0]);
              }
              return rows;
            }
            if (kind === "aggregate")
              return base.groupBy([]).aggregate({ total: { op: "sum", column: "amount" } });
            if (kind === "top-k")
              return base
                .where(gt("amount", 20))
                .orderBy([{ column: "score", direction: "desc" }, { column: "event_id" }])
                .offset(3)
                .limit(7)
                .toArray();
            return base
              .where(gt("amount", 20))
              .project({ doubled: mul(col("amount"), 2) })
              .limit(11)
              .toArray();
          };
          const expectedRows = (kind) =>
            kind === "one-row-batches"
              ? expectedFullRows.map((row) => ({
                  ...row,
                  doubled: row.amount === null ? null : row.amount * 2,
                }))
              : kind === "aggregate"
                ? [{ total }]
                : kind === "top-k"
                  ? expectedFullRows
                      .filter((row) => row.amount !== null && row.amount > 20)
                      .sort((a, b) => b.score - a.score || a.event_id - b.event_id)
                      .slice(3, 10)
                  : expectedFullRows
                      .filter((row) => row.amount !== null && row.amount > 20)
                      .slice(0, 11)
                      .map((row) => ({ ...row, doubled: row.amount * 2 }));
          const kinds = Array.from({ length: concurrency }, (_, index) =>
            scenario === "mixed"
              ? ["top-k", "aggregate", "projection", "one-row-batches"][index % 4]
              : scenario,
          );
          const results = await Promise.all(
            kinds.map(async (kind) => {
              try {
                return await query(kind);
              } catch (error) {
                console.error(
                  JSON.stringify({
                    scenario,
                    kind,
                    memoryMiB,
                    concurrency,
                    temperature,
                    queryHeadroomBytes: headroom,
                    poolUsedBytes: root.usedBytes,
                    poolPeakBytes: root.peakBytes,
                    code: error?.code,
                    details: error?.details,
                  }),
                );
                throw error;
              }
            }),
          );
          for (const [index, rows] of results.entries()) {
            assert.deepEqual(rows, expectedRows(kinds[index]));
          }
          assert.equal(
            oneRowBatchesVerified,
            kinds.filter((kind) => kind === "one-row-batches").length * expectedFullRows.length,
          );
          assert.ok(root.peakBytes <= root.maxBytes);
          assert.ok(metrics.peak <= 4);
          assert.ok(root.usedBytes >= occupied);
          reports.push({
            operation: `normal-owned-${scenario}-with-pinned-cache`,
            queryKinds: kinds,
            oneRowBatchesVerified,
            allocationPoolMiB: memoryMiB,
            concurrency,
            temperature,
            cachePinnedBytes: occupied,
            queryHeadroomBytes: headroom,
            peakIncrementalChargedBytes: root.peakBytes - occupied,
            retainedIncrementalChargedBytes: root.usedBytes - occupied,
            requests: metrics.requests - before.requests,
            bytes: metrics.bytes - before.bytes,
            wireRangeRequests: metrics.httpRangeRequests - before.httpRangeRequests,
            wireBytes: metrics.httpResponseBytes - before.httpResponseBytes,
            peakReads: metrics.peak,
            elapsedMs: Number((performance.now() - started).toFixed(3)),
          });
        }
      } finally {
        pin.release();
        cache.deleteWhere(() => true);
      }
      assert.equal(root.usedBytes, 0);
    }
  }
}
console.log(JSON.stringify({ source: `${baseUrl}${path}`, reports }, null, 2));
