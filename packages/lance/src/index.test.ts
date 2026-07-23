import { readdir, readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve } from "node:path";
import { memoryCache, memoryStore, type ObjectStore } from "lakeql-core";
import { httpStore } from "lakeql-http";
import { afterEach, describe, expect, it } from "vitest";
import { openLanceDataset } from "./index.js";

const FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/take-v2.0.lance");
const DATASET_PATH = "fixtures/take-v2.0.lance";
const TYPE_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/types-v2.0.lance");
const TYPE_DATASET_PATH = "fixtures/types-v2.0.lance";
const DELETION_FIXTURE_ROOT = resolve(import.meta.dirname, "../fixtures/deletions-v2.0.lance");
const DELETION_DATASET_PATH = "fixtures/deletions-v2.0.lance";
const servers: { close(): Promise<void> }[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.close()));
});

describe("lakeql-lance takeRows", () => {
  it("materializes one projected row without a full data-file read", async () => {
    const fixture = await fixtureStore();
    const observed = recordingStore(fixture.store);
    const dataset = await openLanceDataset({
      store: observed.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [17n],
      select: ["serial", "mark_text", "active"],
    });

    expect(result.rows).toEqual([{ serial: 10000017, mark_text: "MARK 017", active: false }]);
    expect(result.stats).toMatchObject({
      snapshotVersion: "1",
      rowsRequested: 1,
      rowsMaterialized: 1,
      fragmentsTouched: 1,
      pagesTouched: 3,
      selectedColumns: ["serial", "mark_text", "active"],
    });
    expect(result.stats.physicalBytesRequested).toBeLessThan(4_000);
    expect(observed.fullDataGets).toBe(0);
    expect(observed.dataRanges.every((range) => range.length < range.fileSize)).toBe(true);
  });

  it("preserves caller order, duplicates, projections, and nulls", async () => {
    const fixture = await fixtureStore();
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const expected = JSON.parse(await readFile(resolve(FIXTURE_ROOT, "expected.json"), "utf8"))
      .sampleProjection as {
      rowIds: string[];
      select: string[];
      rows: Record<string, unknown>[];
    };

    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: expected.rowIds,
      select: expected.select,
    });

    expect(result.rows).toEqual(expected.rows);
    expect(result.stats.rowsRequested).toBe(5);
    expect(result.stats.rowsMaterialized).toBe(5);
    expect(result.stats.fragmentsTouched).toBe(3);
  });

  it("materializes the supported scalar, binary, date, and timestamp type matrix", async () => {
    const fixture = await fixtureStore(TYPE_FIXTURE_ROOT, TYPE_DATASET_PATH);
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: TYPE_DATASET_PATH,
      budget: generousBudget(),
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [1, 2],
      select: [
        "i8",
        "u8",
        "i16",
        "u16",
        "i32",
        "u32",
        "i64",
        "u64",
        "f32",
        "f64",
        "flag",
        "plain_text",
        "payload",
        "event_date",
        "utc_millis",
        "local_micros",
        "maybe_i32",
      ],
    });

    expect(result.rows[0]).toMatchObject({
      i8: -7,
      u8: 249,
      i16: -1_599,
      u16: 64_999,
      i32: -1_999_999,
      u32: 4_000_000_001,
      i64: -9_007_199_254_740_992n,
      u64: 18_000_000_000_000_000_001n,
      f32: 2.25,
      f64: -4.5,
      flag: false,
      plain_text: "plain-1",
      payload: Uint8Array.of(0, 254, 1),
      event_date: new Date("2026-07-24T00:00:00.000Z"),
      utc_millis: {
        epochNanoseconds: 1_784_768_523_457_000_000n,
        unit: "millis",
        isAdjustedToUTC: true,
      },
      local_micros: {
        epochNanoseconds: 1_784_768_523_456_790_000n,
        unit: "micros",
        isAdjustedToUTC: false,
      },
      maybe_i32: null,
    });
    expect(result.rows[1]).toMatchObject({
      i8: -6,
      plain_text: "plain-2",
      payload: Uint8Array.of(0, 253, 2),
      event_date: new Date("2026-07-25T00:00:00.000Z"),
      maybe_i32: 2,
    });
  });

  it("treats snapshot deletions as explicit missing rows without hiding live rows", async () => {
    const fixture = await fixtureStore(DELETION_FIXTURE_ROOT, DELETION_DATASET_PATH);
    const expected = JSON.parse(
      await readFile(resolve(DELETION_FIXTURE_ROOT, "expected.json"), "utf8"),
    ) as { originalRowIds: string[]; deletedRowIds: string[] };
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DELETION_DATASET_PATH,
      budget: generousBudget(),
    });
    const requested = [2, 3, 7, 13].map((offset) => expected.originalRowIds[offset] ?? "");

    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: requested,
        select: ["serial", "label"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
      details: {
        missingRowIds: expected.deletedRowIds,
        deletedRowIds: expected.deletedRowIds,
      },
    });

    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: requested,
        select: ["serial", "label"],
        onMissing: "null",
      }),
    ).resolves.toMatchObject({
      rows: [null, { serial: 103, label: "row-3" }, null, null],
      missingRowIds: expected.deletedRowIds,
      deletedRowIds: expected.deletedRowIds,
    });
  });

  it("materializes 32 scattered rows with bounded reads and concurrency", async () => {
    const fixture = await fixtureStore();
    let active = 0;
    let peak = 0;
    const store = wrapStore(fixture.store, {
      async getRange(path, range) {
        active += 1;
        peak = Math.max(peak, active);
        try {
          await Promise.resolve();
          return await fixture.store.getRange(path, range);
        } finally {
          active -= 1;
        }
      },
    });
    const dataset = await openLanceDataset({
      store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxConcurrentReads: 2 },
    });
    const rowIds = Array.from({ length: 32 }, (_value, index) => (index * 17) % 64);
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds,
      select: ["serial", "status", "score"],
    });

    expect(result.rows).toHaveLength(32);
    expect(result.rows.map((row) => row?.serial)).toEqual(
      rowIds.map((rowId) => 10_000_000 + rowId),
    );
    expect(peak).toBeLessThanOrEqual(2);
    expect(result.stats.fragmentsTouched).toBe(4);
    expect(result.stats.physicalBytesRequested).toBeLessThan(16_000);
  });

  it("defines missing and invalid row-ID behavior", async () => {
    const fixture = await fixtureStore();
    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });

    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 999],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
      details: { missingRowIds: ["999"] },
    });
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 999, 0],
        select: ["serial"],
        onMissing: "null",
      }),
    ).resolves.toMatchObject({
      rows: [{ serial: 10000000 }, null, { serial: 10000000 }],
      missingRowIds: ["999"],
    });
    for (const invalid of [-1, Number.MAX_SAFE_INTEGER + 1, "1.5", "-1"]) {
      await expect(
        dataset.takeRows({
          snapshotId: dataset.snapshotId,
          rowIds: [invalid],
          select: ["serial"],
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
  });

  it("validates dataset paths, range planning, projections, and row-shape budgets", async () => {
    const fixture = await fixtureStore();
    for (const path of ["", ".", "a/../b"]) {
      await expect(openLanceDataset({ store: fixture.store, path })).rejects.toMatchObject({
        code: "LAKEQL_VALIDATION_ERROR",
      });
    }
    for (const options of [
      { coalesceGapBytes: -1 },
      { coalesceGapBytes: 0.5 },
      { maxCoalescedRangeBytes: 0 },
      { maxCoalescedRangeBytes: 0.5 },
    ]) {
      await expect(
        openLanceDataset({ store: fixture.store, path: DATASET_PATH, ...options }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
    await expect(
      openLanceDataset({ store: fixture.store, path: DATASET_PATH, version: 0 }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });

    const dataset = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxOutputRows: 1, maxRowsDecoded: 1 },
    });
    for (const select of [[], [""], ["serial", "serial"]]) {
      await expect(
        dataset.takeRows({
          snapshotId: dataset.snapshotId,
          rowIds: [0],
          select,
        }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    }
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0, 1],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "output rows" },
    });
    const decodedBudget = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxRowsDecoded: 1 },
    });
    await expect(
      decodedBudget.takeRows({
        snapshotId: decodedBudget.snapshotId,
        rowIds: [0, 1],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "rows decoded" },
    });
  });

  it("rejects row IDs coupled to another snapshot before data reads", async () => {
    const fixture = await fixtureStore();
    const observed = recordingStore(fixture.store);
    const dataset = await openLanceDataset({
      store: observed.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const requestsAfterOpen = observed.rangeRequests;

    await expect(
      dataset.takeRows({
        snapshotId: `${dataset.snapshotId}-stale`,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_SNAPSHOT_MISMATCH" });
    expect(observed.rangeRequests).toBe(requestsAfterOpen);
  });

  it("uses a byte metadata cache without weakening snapshot identity", async () => {
    const fixture = await fixtureStore();
    const cache = memoryCache<Uint8Array>();
    const first = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
      metadataCache: cache,
    });
    const second = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: generousBudget(),
      metadataCache: cache,
    });
    const result = await second.takeRows({
      snapshotId: second.snapshotId,
      rowIds: [0],
      select: ["serial"],
    });

    expect(second.snapshotId).toBe(first.snapshotId);
    expect(result.stats.cacheHits).toBe(1);
    expect(result.stats.cacheMisses).toBe(0);
    expect(result.stats.snapshotMetadataBytes).toBe(13);
  });

  it("enforces byte, request, output, memory, elapsed, and cancellation budgets", async () => {
    const fixture = await fixtureStore();
    await expect(
      openLanceDataset({
        store: fixture.store,
        path: DATASET_PATH,
        budget: { maxBytes: 12 },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const limitedRequests = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxRangeRequests: 3 },
    });
    await expect(
      limitedRequests.takeRows({
        snapshotId: limitedRequests.snapshotId,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const limitedRows = await openLanceDataset({
      store: fixture.store,
      path: DATASET_PATH,
      budget: { ...generousBudget(), maxOutputRows: 1 },
    });
    await expect(
      limitedRows.takeRows({
        snapshotId: limitedRows.snapshotId,
        rowIds: [0, 1],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    await expect(
      openLanceDataset({
        store: fixture.store,
        path: DATASET_PATH,
        budget: { ...generousBudget(), maxMemoryBytes: 8 },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    let now = 0;
    const timedStore = wrapStore(fixture.store, {
      async getRange(path, range) {
        now += 2;
        return await fixture.store.getRange(path, range);
      },
    });
    await expect(
      openLanceDataset({
        store: timedStore,
        path: DATASET_PATH,
        budget: { ...generousBudget(), maxElapsedMs: 1 },
        now: () => now,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    const controller = new AbortController();
    controller.abort("test");
    await expect(
      openLanceDataset({
        store: fixture.store,
        path: DATASET_PATH,
        budget: { signal: controller.signal },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  });

  it("rejects corrupt manifests and data-file offsets with typed errors", async () => {
    const manifestCorrupt = await fixtureStore();
    const manifestPath = `${DATASET_PATH}/_versions/18446744073709551614.manifest`;
    const manifest = await manifestCorrupt.store.get(manifestPath);
    if (manifest === null) throw new Error("fixture manifest missing");
    manifest[manifest.byteLength - 1] ^= 0xff;
    await manifestCorrupt.store.put(manifestPath, manifest);
    await expect(
      openLanceDataset({
        store: manifestCorrupt.store,
        path: DATASET_PATH,
        budget: generousBudget(),
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });

    const dataCorrupt = await fixtureStore();
    for (const dataPath of [...dataCorrupt.files.keys()].filter((path) =>
      path.endsWith(".lance"),
    )) {
      const data = await dataCorrupt.store.get(dataPath);
      if (data === null) throw new Error("fixture data file missing");
      const view = new DataView(data.buffer, data.byteOffset + data.byteLength - 40, 40);
      view.setBigUint64(8, BigInt(data.byteLength + 1), true);
      await dataCorrupt.store.put(dataPath, data);
    }
    const dataset = await openLanceDataset({
      store: dataCorrupt.store,
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    await expect(
      dataset.takeRows({
        snapshotId: dataset.snapshotId,
        rowIds: [0],
        select: ["serial"],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_LANCE_READ_ERROR" });
  });

  it("reads the official fixture through HTTP byte ranges", async () => {
    const fixture = await fixtureStore();
    const requests: string[] = [];
    const server = createServer(async (request, response) => {
      const key = decodeURIComponent(
        new URL(request.url ?? "/", "http://localhost").pathname.slice(1),
      );
      const bytes = fixture.files.get(key);
      if (bytes === undefined) {
        response.writeHead(404).end();
        return;
      }
      const range = /^bytes=(\d+)-(\d+)$/u.exec(request.headers.range ?? "");
      requests.push(request.headers.range ?? "full");
      if (range === null) {
        response.writeHead(200, { "content-length": bytes.byteLength }).end(bytes);
        return;
      }
      const start = Number(range[1]);
      const end = Number(range[2]);
      const body = bytes.subarray(start, end + 1);
      response
        .writeHead(206, {
          "content-length": body.byteLength,
          "content-range": `bytes ${start}-${end}/${bytes.byteLength}`,
        })
        .end(body);
    });
    await new Promise<void>((resolveListen) => server.listen(0, "127.0.0.1", resolveListen));
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("HTTP server not bound");
    servers.push({
      close: () => new Promise((resolveClose) => server.close(() => resolveClose())),
    });

    const dataset = await openLanceDataset({
      store: httpStore({ baseUrl: `http://127.0.0.1:${address.port}/` }),
      path: DATASET_PATH,
      budget: generousBudget(),
    });
    const result = await dataset.takeRows({
      snapshotId: dataset.snapshotId,
      rowIds: [0, 63],
      select: ["serial", "mark_text"],
    });

    expect(result.rows).toEqual([
      { serial: 10000000, mark_text: "MARK 000" },
      { serial: 10000063, mark_text: "MARK 063" },
    ]);
    expect(requests.length).toBeGreaterThan(0);
    expect(requests.every((range) => range.startsWith("bytes="))).toBe(true);
  });
});

function generousBudget() {
  return {
    maxBytes: 1024 * 1024,
    maxRangeRequests: 128,
    maxElapsedMs: 5_000,
    maxMemoryBytes: 1024 * 1024,
    maxConcurrentReads: 4,
    maxOutputRows: 64,
    maxRowsDecoded: 64,
  };
}

async function fixtureStore(
  root = FIXTURE_ROOT,
  datasetPath = DATASET_PATH,
): Promise<{
  store: ObjectStore;
  files: Map<string, Uint8Array>;
}> {
  const store = memoryStore();
  const files = new Map<string, Uint8Array>();
  for (const relative of await fixtureFiles(root)) {
    if (relative === "expected.json") continue;
    const bytes = new Uint8Array(await readFile(resolve(root, relative)));
    const path = `${datasetPath}/${relative}`;
    files.set(path, bytes);
    await store.put(path, bytes);
  }
  return { store, files };
}

async function fixtureFiles(root: string, prefix = ""): Promise<string[]> {
  const output: string[] = [];
  for (const entry of await readdir(resolve(root, prefix), { withFileTypes: true })) {
    const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
    if (entry.isDirectory()) output.push(...(await fixtureFiles(root, relative)));
    else output.push(relative);
  }
  return output;
}

function recordingStore(inner: ObjectStore): {
  store: ObjectStore;
  rangeRequests: number;
  fullDataGets: number;
  dataRanges: { length: number; fileSize: number }[];
} {
  const state = {
    rangeRequests: 0,
    fullDataGets: 0,
    dataRanges: [] as { length: number; fileSize: number }[],
  };
  return {
    ...state,
    get rangeRequests() {
      return state.rangeRequests;
    },
    get fullDataGets() {
      return state.fullDataGets;
    },
    get dataRanges() {
      return state.dataRanges;
    },
    store: wrapStore(inner, {
      async get(path) {
        if (path.endsWith(".lance")) state.fullDataGets += 1;
        return await inner.get(path);
      },
      async getRange(path, range) {
        state.rangeRequests += 1;
        if (path.endsWith(".lance")) {
          const head = await inner.head(path);
          if (head === null) throw new Error("missing fixture object");
          state.dataRanges.push({ length: range.length, fileSize: head.size });
        }
        return await inner.getRange(path, range);
      },
    }),
  };
}

function wrapStore(
  inner: ObjectStore,
  overrides: Partial<Pick<ObjectStore, "get" | "getRange">>,
): ObjectStore {
  return {
    get: overrides.get ?? ((path) => inner.get(path)),
    getRange: overrides.getRange ?? ((path, range) => inner.getRange(path, range)),
    put: (path, body, options) => inner.put(path, body, options),
    delete: (path) => inner.delete(path),
    async *list(prefix, options) {
      yield* inner.list(prefix, options);
    },
    head: (path) => inner.head(path),
  };
}
