import { appendFile, mkdir, open, rm, stat } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import type { ObjectHead, ObjectInfo, ObjectStore, QueryStats, Row } from "lakeql-core";
import { EXTERNAL_PLOTLY, externalFixturePath } from "lakeql-fixtures";
import { parseSql } from "lakeql-sql";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createParquetLake } from "./index.js";

const runHotPerf = process.env.LAKEQL_HOT_PERF === "1";
const hotPerfTimeoutMs = positiveIntegerEnv("LAKEQL_HOT_PERF_TIMEOUT_MS", 600_000);
const selectedCaseNames = new Set(
  (process.env.LAKEQL_HOT_PERF_CASES ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean),
);
const fixturePath = externalFixturePath(EXTERNAL_PLOTLY.flights2015);
const fixtureRoot = dirname(fixturePath);
const fixtureKey = basename(fixturePath);
const reportPath = resolve("bench/generated/flights-hot-performance.jsonl");
const scanRangeCacheBytes = 32 * 1024 * 1024;
const lakeCacheBytes = positiveIntegerEnv("LAKEQL_HOT_PERF_CACHE_BYTES", 64 * 1024 * 1024);
const lakeCachePolicy = cachePolicyEnv("LAKEQL_HOT_PERF_CACHE_POLICY");
const expectZeroHotBytes = envFlag("LAKEQL_HOT_PERF_EXPECT_ZERO_HOT_BYTES", true);

interface StoreCounters {
  get: number;
  getRange: number;
  head: number;
  bytesFetched: number;
}

interface HotCase {
  name: string;
  sql: string;
}

interface InstrumentedStore extends ObjectStore {
  resetCounters(): void;
  takeCounters(): StoreCounters;
}

interface HotRuntime {
  lake: ReturnType<typeof createParquetLake>;
  store: InstrumentedStore;
}

interface HotRun {
  phase: "cold" | "warmup" | "hot";
  rows: number;
  timings: { parseMs: number; buildMs: number; runMs: number; totalMs: number };
  counters: StoreCounters;
  stats: QueryStats;
}

const hotCases: HotCase[] = [
  {
    name: "limit-one",
    sql: `select "ARRIVAL_DELAY"
from flights.parquet
limit 1`,
  },
  {
    name: "top-delays",
    sql: `select "DEPARTURE_DELAY", "ARRIVAL_DELAY", "DISTANCE"
from flights.parquet
where "DEPARTURE_DELAY" > 120
order by "DEPARTURE_DELAY" desc
limit 10`,
  },
  {
    name: "distance-group",
    sql: `select "DISTANCE", count() as flights, avg("ARRIVAL_DELAY") as avg_arrival_delay
from flights.parquet
where "DISTANCE" > 2500
group by "DISTANCE"
order by flights desc
limit 10`,
  },
];

const selectedHotCases =
  selectedCaseNames.size === 0
    ? hotCases
    : hotCases.filter((hotCase) => selectedCaseNames.has(hotCase.name));

describe.skipIf(!runHotPerf)("Plotly flights hot performance", () => {
  beforeAll(async () => {
    await expect(stat(fixturePath), missingFixtureMessage()).resolves.toMatchObject({
      size: EXTERNAL_PLOTLY.flights2015Size,
    });
    await mkdir(dirname(reportPath), { recursive: true });
    await rm(reportPath, { force: true });
  });

  afterAll(() => {
    console.info(
      "lakeql hot perf: run with LAKEQL_HOT_PERF=1 pnpm exec vitest run packages/parquet/src/flights-hot-performance.test.ts",
    );
  });

  for (const hotCase of selectedHotCases) {
    it(
      `profiles ${hotCase.name}`,
      async () => {
        const cold = await runCase(hotCase, "cold");
        const warmup = await runCase(hotCase, "warmup");
        const hot = await runCase(hotCase, "hot", warmup.runtime);

        expect(cold.result.rows).toBeGreaterThan(0);
        expect(warmup.result.rows).toBe(cold.result.rows);
        expect(hot.result.rows).toBe(cold.result.rows);
        if (expectZeroHotBytes) expect(hot.result.counters.bytesFetched).toBe(0);

        const profile = {
          case: hotCase.name,
          cold: cold.result,
          warmup: warmup.result,
          hot: hot.result,
        };
        await appendFile(reportPath, `${JSON.stringify(profile)}\n`);
        console.info(JSON.stringify(profile, null, 2));
      },
      hotPerfTimeoutMs,
    );
  }
});

async function runCase(
  hotCase: HotCase,
  phase: "cold" | "warmup" | "hot",
  existingRuntime?: HotRuntime,
): Promise<{ result: HotRun; runtime?: HotRuntime }> {
  const runtime =
    existingRuntime ??
    (() => {
      const store = fileStore(fixtureRoot);
      const lake = createParquetLake({
        store,
        scanRangeCache: { maxBytes: scanRangeCacheBytes },
        ...(phase === "cold"
          ? {}
          : {
              cache: {
                maxBytes: lakeCacheBytes,
                ...(lakeCachePolicy === undefined ? {} : { policy: lakeCachePolicy }),
              },
            }),
        queryId: () => `hot-flights-${hotCase.name}-${phase}`,
      });
      return { lake, store };
    })();
  runtime.store.resetCounters();
  const totalStarted = performance.now();
  const ast = measure(() => parseSql(hotCase.sql));
  const built = measure(() => buildQuery(runtime.lake, ast.value));
  const runStarted = performance.now();
  const rows = await built.value.run();
  const runMs = performance.now() - runStarted;
  const result: HotRun = {
    phase,
    rows: rows.length,
    timings: {
      parseMs: ast.ms,
      buildMs: built.ms,
      runMs,
      totalMs: performance.now() - totalStarted,
    },
    counters: runtime.store.takeCounters(),
    stats: built.value.stats(),
  };
  return {
    result,
    ...(phase === "warmup" ? { runtime } : {}),
  };
}

function buildQuery(
  lake: ReturnType<typeof createParquetLake>,
  ast: ReturnType<typeof parseSql>,
): { run: () => Promise<Row[]>; stats: () => QueryStats } {
  const source = fixtureKey;
  if (ast.aggregates && Object.keys(ast.aggregates).length > 0) {
    let builder = lake.path(source);
    if (ast.where) builder = builder.where(ast.where);
    const result = builder.run();
    return {
      run: () =>
        result.aggregate(ast.groupBy ?? [], ast.aggregates ?? {}, {
          ...(ast.orderBy !== undefined ? { orderBy: ast.orderBy } : {}),
          ...(ast.limit !== undefined ? { limit: ast.limit } : {}),
          ...(ast.offset !== undefined ? { offset: ast.offset } : {}),
        }),
      stats: () => result.stats,
    };
  }

  let builder = lake.path(source);
  if (ast.select) builder = builder.select(ast.select);
  if (ast.where) builder = builder.where(ast.where);
  if (ast.orderBy) builder = builder.orderBy(ast.orderBy);
  if (ast.offset !== undefined) builder = builder.offset(ast.offset);
  if (ast.limit !== undefined) builder = builder.limit(ast.limit);
  const result = builder.run();
  return { run: () => result.toArray(), stats: () => result.stats };
}

function fileStore(root: string): InstrumentedStore {
  const counters: StoreCounters = { get: 0, getRange: 0, head: 0, bytesFetched: 0 };
  return {
    resetCounters() {
      counters.get = 0;
      counters.getRange = 0;
      counters.head = 0;
      counters.bytesFetched = 0;
    },
    takeCounters() {
      return { ...counters };
    },
    async get(path) {
      counters.get += 1;
      const handle = await open(join(root, path), "r").catch((error: unknown) => {
        if (isErrno(error, "ENOENT")) return undefined;
        throw error;
      });
      if (!handle) return null;
      try {
        const { size } = await handle.stat();
        const bytes = new Uint8Array(size);
        await handle.read(bytes, 0, bytes.byteLength, 0);
        counters.bytesFetched += bytes.byteLength;
        return bytes;
      } finally {
        await handle.close();
      }
    },
    async getRange(path, range) {
      counters.getRange += 1;
      const handle = await open(join(root, path), "r");
      try {
        const bytes = new Uint8Array(range.length);
        let offset = 0;
        while (offset < bytes.byteLength) {
          const { bytesRead } = await handle.read(
            bytes,
            offset,
            bytes.byteLength - offset,
            range.offset + offset,
          );
          if (bytesRead === 0) break;
          offset += bytesRead;
        }
        if (offset !== bytes.byteLength) {
          throw new Error(`short range read for ${path}: ${range.offset}+${range.length}`);
        }
        counters.bytesFetched += bytes.byteLength;
        return bytes;
      } finally {
        await handle.close();
      }
    },
    async put() {
      throw new Error("hot performance fixture store is read-only");
    },
    async delete() {
      throw new Error("hot performance fixture store is read-only");
    },
    async *list(prefix): AsyncIterable<ObjectInfo> {
      if (!fixtureKey.startsWith(prefix)) return;
      const info = await stat(join(root, fixtureKey));
      yield { path: fixtureKey, size: info.size, lastModified: info.mtime };
    },
    async head(path): Promise<ObjectHead | null> {
      counters.head += 1;
      try {
        const info = await stat(join(root, path));
        return { size: info.size, lastModified: info.mtime };
      } catch (error) {
        if (isErrno(error, "ENOENT")) return null;
        throw error;
      }
    },
  };
}

function measure<T>(fn: () => T): { value: T; ms: number } {
  const started = performance.now();
  const value = fn();
  return { value, ms: performance.now() - started };
}

function missingFixtureMessage(): string {
  return [
    `Missing ${fixturePath}.`,
    "Run `pnpm fixtures:external` to cache the Plotly flights Parquet fixture locally.",
  ].join(" ");
}

function isErrno(error: unknown, code: string): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === code
  );
}

function positiveIntegerEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function cachePolicyEnv(name: string): "balanced" | "io" | "latency" | undefined {
  const value = process.env[name];
  if (value === undefined) return undefined;
  if (value === "balanced" || value === "io" || value === "latency") return value;
  throw new Error(`${name} must be balanced, io, or latency`);
}

function envFlag(name: string, defaultValue: boolean): boolean {
  const value = process.env[name];
  if (value === undefined) return defaultValue;
  return value === "1" || value.toLowerCase() === "true";
}
