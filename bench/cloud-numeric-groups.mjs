// Read-only validation of internal grouped numeric alignment over public R2 HTTP.
import assert from "node:assert/strict";
import {
  MemoryBudget,
  MemoryScope,
  MemoryStageScheduler,
  materializeBatchRows,
  SharedMemoryCache,
  withObjectStoreReadControls,
} from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { DecodedColumnCache } from "../packages/parquet/dist/decoded-column-cache.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/index.js";
import { acquireIndexedParquetSource } from "../packages/parquet/dist/indexed-source.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import {
  prepareNumericColumnPages,
  readAlignedNumericPageGroups,
} from "../packages/parquet/dist/numeric-page-alignment.js";
import { readOwnedParquetVectorBatches } from "../packages/parquet/dist/owned-vector-batches.js";
import { cachedRangeBuffer } from "../packages/parquet/dist/range-cache.js";
import { readParquetVectorBatchesFromFile } from "../packages/parquet/dist/vector-batches.js";
import { DEFAULT_PARSERS } from "../packages/parquet/node_modules/hyparquet/src/convert.js";
import { getSchemaPath } from "../packages/parquet/node_modules/hyparquet/src/schema.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const path = "window-events.parquet";
const columns = ["event_id", "amount"];
const adapter = httpStore({
  baseUrl,
  fetch: async (input, init) => {
    const response = await fetch(input, {
      ...init,
      signal: AbortSignal.any([AbortSignal.timeout(30000), ...(init?.signal ? [init.signal] : [])]),
    });
    if (init?.method !== "HEAD") {
      assert.ok(new Headers(init?.headers).has("Range"));
      assert.equal(response.status, 206);
    }
    return response;
  },
});
const oracleFile = await asyncBufferFromStore(adapter, path);
const metadata = await readParquetMetadataFromFile(oracleFile);
const oracle = [];
for await (const output of readParquetVectorBatchesFromFile(oracleFile, metadata, { columns }))
  oracle.push(...materializeBatchRows(output.batch));
const expected = [];
for (const _region of ["north", "south", "west"])
  for (let account = 0; account < 4; account++)
    for (let i = 0; i < 12; i++)
      expected.push({
        event_id: expected.length + 1,
        amount: i % 5 === 4 ? null : 10 + account * 4 + i * 1.5,
      });
assert.deepEqual(oracle, expected);
const normalScan = process.env.LAKEQL_GROUP_NORMAL_SCAN === "1";
const indexedMetadata = process.env.LAKEQL_GROUP_INDEXED_METADATA === "1";
assert.ok(!indexedMetadata || normalScan);
const scheduled = process.env.LAKEQL_GROUP_SCHEDULED === "1";
const decodedCacheEnabled = process.env.LAKEQL_GROUP_DECODED_CACHE === "1";
const cacheBytes =
  process.env.LAKEQL_GROUP_CACHE === "1" || decodedCacheEnabled
    ? Number(process.env.LAKEQL_GROUP_CACHE_BYTES ?? 4096)
    : 0;
assert.ok(Number.isSafeInteger(cacheBytes) && cacheBytes >= 0);
const reports = [];
const jobBytes = Number(process.env.LAKEQL_GROUP_JOB_BYTES ?? 4096);
assert.ok(Number.isSafeInteger(jobBytes) && jobBytes > 0);
for (const maxConcurrentReads of [1, 2])
  for (const jobs of [1, 4]) {
    let active = 0,
      peakReads = 0,
      reads = 0,
      transferredBytes = 0;
    const ranges = [];
    const measured = {
      head: adapter.head.bind(adapter),
      list: adapter.list.bind(adapter),
      get: adapter.get.bind(adapter),
      getRange: adapter.getRange.bind(adapter),
      put: adapter.put.bind(adapter),
      delete: adapter.delete.bind(adapter),
      async getRangeOwned(key, range, scope) {
        active++;
        reads++;
        ranges.push({ offset: range.offset, length: range.length });
        peakReads = Math.max(peakReads, active);
        try {
          const owner = await adapter.getRangeOwned(key, range, scope);
          assert.equal(owner.value.byteLength, range.length);
          transferredBytes += owner.value.byteLength;
          return owner;
        } finally {
          active--;
        }
      },
    };
    const store = withObjectStoreReadControls(measured, { maxConcurrentReads });
    const rawFile = await asyncBufferFromStore(store, path);
    const root = new MemoryBudget(jobs * jobBytes + cacheBytes);
    const scheduler = scheduled ? new MemoryStageScheduler(root) : undefined;
    const cache = cacheBytes
      ? new SharedMemoryCache({ memoryBudget: root, maxBytes: cacheBytes })
      : undefined;
    const decodedCache =
      decodedCacheEnabled && cache
        ? new DecodedColumnCache(cache, { maxBytes: cacheBytes, policy: "latency" })
        : undefined;
    for (const temperature of cache ? ["cold", "warm"] : ["uncached"]) {
      active = peakReads = reads = transferredBytes = 0;
      ranges.length = 0;
      const started = performance.now();
      await Promise.all(
        Array.from({ length: jobs }, async (_, job) => {
          const file = cache
            ? cachedRangeBuffer(
                rawFile,
                { maxBytes: cacheBytes, coalesceBytes: 512, sharedCache: cache },
                baseUrl + path,
              )
            : rawFile;
          const start = job % 2 ? 13 : 0;
          const end = job % 2 ? Math.min(107, oracle.length) : oracle.length;
          const scope = new MemoryScope(root.child(jobBytes));
          const rows = [];
          let groupStart = 0;
          let indexed;
          try {
            if (normalScan) {
              if (indexedMetadata) indexed = await acquireIndexedParquetSource(file, scope);
              for await (const owner of readOwnedParquetVectorBatches(
                file,
                indexed ?? metadata,
                scope.budget,
                {
                  columns,
                  rowStart: start,
                  rowEnd: end,
                  batchSize: 7,
                  maxConcurrentReads,
                  ...(decodedCache === undefined
                    ? {}
                    : { decodedColumnCache: decodedCache, decodedColumnCacheKey: baseUrl + path }),
                },
              )) {
                assert.equal(owner.value.rowOffset, start + rows.length);
                rows.push(...materializeBatchRows(owner.value.batch));
                owner.release();
              }
            }
            for (const group of normalScan ? [] : metadata.row_groups) {
              const count = Number(group.num_rows);
              const rowStart = Math.max(0, start - groupStart),
                rowEnd = Math.min(count, end - groupStart);
              if (rowStart < rowEnd) {
                const sources = columns.map((columnName) => {
                  const column = group.columns.find(
                    (x) => x.meta_data.path_in_schema.join(".") === columnName,
                  ).meta_data;
                  const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
                  const decoder = {
                    ...column,
                    pathInSchema: column.path_in_schema,
                    schemaPath,
                    element: schemaPath.at(-1).element,
                    parsers: DEFAULT_PARSERS,
                  };
                  const chunkStart = Number(column.data_page_offset);
                  return {
                    column: columnName,
                    pages: prepareNumericColumnPages(
                      file,
                      decoder,
                      { start: chunkStart, end: chunkStart + Number(column.total_compressed_size) },
                      scope,
                      { start: rowStart, end: rowEnd },
                    ),
                  };
                });
                for await (const owner of readAlignedNumericPageGroups(file, sources, scope, {
                  rowStart,
                  rowEnd,
                  batchSize: 7,
                  maxConcurrentReads: 2,
                  ...(scheduler === undefined ? {} : { scheduler }),
                  ...(decodedCache === undefined
                    ? {}
                    : {
                        decodedCache: {
                          cache: decodedCache,
                          path: baseUrl + path,
                          rowGroupStart: groupStart,
                        },
                      }),
                })) {
                  assert.equal(groupStart + owner.value.rowOffset, start + rows.length);
                  rows.push(...materializeBatchRows(owner.value.batch));
                  owner.release();
                }
              }
              groupStart += count;
            }
            assert.deepEqual(rows, oracle.slice(start, end));
          } finally {
            indexed?.dispose();
            scope.dispose();
            assert.equal(scope.budget.usedBytes, 0);
          }
        }),
      );
      assert.ok(root.usedBytes <= cacheBytes);
      assert.ok(root.peakBytes <= root.maxBytes);
      assert.ok(peakReads <= maxConcurrentReads);
      reports.push({
        jobs,
        jobBytes,
        cacheBytes,
        decodedCacheEnabled,
        scheduled,
        normalScan,
        indexedMetadata,
        temperature,
        retainedCacheBytes: root.usedBytes,
        maxConcurrentReads,
        peakReads,
        reads,
        transferredBytes,
        ranges: [...ranges],
        peakBytes: root.peakBytes,
        poolBytes: root.maxBytes,
        elapsedMs: performance.now() - started,
      });
    }
    cache?.deleteWhere(() => true);
    assert.equal(root.usedBytes, 0);
  }
console.log(
  JSON.stringify(
    {
      source: baseUrl + path,
      scope: indexedMetadata
        ? "Actual R2 HTTP 206 owned vector scans with indexed footer acquisition inside each job budget; oracle and decoded JS excluded; shared cache accounting included; not whole-query heap coverage"
        : normalScan
          ? "Actual R2 HTTP 206 through ordinary owned Parquet scans; excludes metadata/oracle allocation and JS heap; shared-cache accounting included; not whole-query operator coverage"
          : "Actual R2 HTTP 206, internal grouped numeric iterator; excludes metadata/oracle allocation and JS heap; optional shared-cache accounting is reported separately; not normal-query scheduler integration",
      reports,
    },
    null,
    2,
  ),
);
