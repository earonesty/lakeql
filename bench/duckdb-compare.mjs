import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { DuckDBConnection } from "@duckdb/node-api";
import {
  concatBatches,
  evaluate,
  gatherBatch,
  materializeBatchRows,
  memoryCache,
  memoryStore,
  tryPredicateSelection,
  vectorHashJoin,
  vectorProjectBatch,
  vectorTopKIndices,
} from "../packages/core/dist/index.js";
import {
  aggregateParquetGroupTasksBatch,
  aggregateParquetTasks,
  createParquetLake,
  scanParquetTaskColumnBatches,
} from "../packages/parquet/dist/index.js";
import { parseSql } from "../packages/sql/dist/index.js";
import { queryStats, summarizeSamples, timed } from "./bench-utils.mjs";
import { cases } from "./duckdb-cases.mjs";
import { cteMetric, materializeBenchmarkCteIfNeeded } from "./duckdb-cte.mjs";
import { renderConsole, renderReport } from "./duckdb-report.mjs";
import {
  orderVectorScanBatch,
  referencedColumns,
  referencedJoinSideColumns,
  referencedSubqueryJoinColumns,
  selectionIndices,
} from "./duckdb-vector-helpers.mjs";
import { planBenchmarkWorkUnits, workUnitSourceMetrics } from "./duckdb-workunits.mjs";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const fixtureRoot = join(repoRoot, "fixtures/data");
const reportPath = join(repoRoot, "bench/DUCKDB.md");

const iterations = Number(process.env.LAKEQL_BENCH_ITERATIONS ?? 5);
const warmup = Number(process.env.LAKEQL_BENCH_WARMUP ?? 1);
const rowGroupsPerWorkUnit = Number(process.env.LAKEQL_BENCH_ROW_GROUPS_PER_TASK ?? 1);
const aggregateConcurrency = Number(process.env.LAKEQL_BENCH_CONCURRENT_TASKS ?? 1);

const duckdb = await DuckDBConnection.create();
const rows = [];
for (const testCase of cases) {
  rows.push(await compareCase(testCase));
}

await writeFile(reportPath, renderReport(rows, { iterations, warmup }));
console.log(renderConsole(rows));
console.log(`wrote ${reportPath}`);

async function compareCase(testCase) {
  const fixturePath = join(fixtureRoot, testCase.fixture);
  const extraFiles = testCase.extraFiles === undefined ? {} : await testCase.extraFiles();
  const duckdbSql = testCase.duckdb(fixturePath, extraFiles);

  const lakeqlRows = await runLakeql(
    testCase.fixture,
    testCase.lakeql,
    extraFiles,
    testCase.sourceAliases,
  );
  const duckdbRows = await runDuckDb(duckdbSql);
  assertSameRows(testCase.name, lakeqlRows.rows, duckdbRows);
  assertLakeqlExpectations(testCase.name, lakeqlRows, testCase.lakeqlExpect);

  for (let index = 0; index < warmup; index++) {
    await runLakeql(testCase.fixture, testCase.lakeql, extraFiles, testCase.sourceAliases);
    await runDuckDb(duckdbSql);
  }

  const lakeqlSamples = [];
  const duckdbSamples = [];
  const lakeqlPeakRssSamples = [];
  const duckdbPeakRssSamples = [];
  let lastLakeql = lakeqlRows;
  for (let index = 0; index < iterations; index++) {
    lastLakeql = await timedBenchmarkResult(() =>
      runLakeql(testCase.fixture, testCase.lakeql, extraFiles, testCase.sourceAliases),
    );
    lakeqlSamples.push(lastLakeql.ms);
    lakeqlPeakRssSamples.push(lastLakeql.peakRssBytes);
    const duck = await timedBenchmarkResult(() => runDuckDb(duckdbSql));
    duckdbSamples.push(duck.ms);
    duckdbPeakRssSamples.push(duck.peakRssBytes);
  }
  assertLakeqlExpectations(testCase.name, lastLakeql, testCase.lakeqlExpect);

  const lakeqlStats = summarizeSamples(lakeqlSamples, lakeqlPeakRssSamples);
  const duckdbStats = summarizeSamples(duckdbSamples, duckdbPeakRssSamples);
  return {
    name: testCase.name,
    rows: lakeqlRows.rows.length,
    lakeql: {
      ...lakeqlStats,
      bytes: lastLakeql.bytesFetched,
      requests: lastLakeql.objectRequests,
      rangeRequests: lastLakeql.rangeRequests,
      bytesRequested: lastLakeql.bytesRequested,
      rowsScanned: lastLakeql.rowsScanned,
      filesRead: lastLakeql.filesRead,
      filesSkipped: lastLakeql.filesSkipped,
      rowGroupsRead: lastLakeql.rowGroupsRead,
      rowGroupsSkipped: lastLakeql.rowGroupsSkipped,
      rowsMatched: lastLakeql.rowsMatched,
      workUnits: lastLakeql.workUnits,
      path: lastLakeql.path,
    },
    duckdb: duckdbStats,
  };
}

async function runLakeql(fixture, sql, extraFiles = {}, sourceAliases = {}) {
  const { store, counters } = countingStore();
  await store.put(fixture, await readFile(join(fixtureRoot, fixture)));
  for (const [name, file] of Object.entries(extraFiles)) await store.put(name, file.bytes);
  counters.reset();
  const metadataCache = memoryCache();
  const lake = createParquetLake({ store, metadataCache });
  const ast = await materializeBenchmarkCteIfNeeded(
    store,
    lake,
    await resolveBenchmarkScalarSubqueries(
      store,
      lake,
      normalizeBenchmarkSources(parseSql(sql), fixture, sourceAliases),
      fixture,
      metadataCache,
    ),
    {
      aggregateConcurrency,
      benchQueryStats,
      defaultSource: fixture,
      metadataCache,
      rowGroupsPerWorkUnit,
    },
  );
  let rows;
  let stats;

  const aggregates =
    ast.aggregates && Object.keys(ast.aggregates).length > 0 ? ast.aggregates : undefined;
  const grouped = (ast.groupBy?.length ?? 0) > 0;

  if (canUseVectorJoin(ast, aggregates, grouped)) {
    const stats = benchQueryStats();
    const join = ast.join;
    const left = await scanJoinedSourceBatch(
      store,
      lake,
      fixture,
      join.leftAlias,
      ast,
      stats,
      metadataCache,
    );
    const right = await scanJoinedSourceBatch(
      store,
      lake,
      join.source,
      join.alias,
      ast,
      stats,
      metadataCache,
    );
    const joined = vectorHashJoin(left.batch, right.batch, {
      leftKey: join.leftKey,
      rightKey: join.rightKey,
      type: join.type,
      rightPrefix: `${join.alias}.`,
      maxRightRows: 100_000,
    });
    const filtered = filterVectorJoinBatch(joined, ast);
    const orderedBatch = orderVectorScanBatch(filtered, ast);
    rows = orderedBatch === undefined ? [] : projectRows(materializeBatchRows(orderedBatch), ast);
    return {
      rows,
      path: "vector hash join work units",
      workUnits: left.workUnits + right.workUnits,
      bytesFetched: counters.bytesFetched,
      objectRequests: counters.totalRequests,
      rangeRequests: stats.rangeRequests + scalarMetric(ast, "rangeRequests"),
      bytesRequested: stats.bytesRequested + scalarMetric(ast, "bytesRequested"),
      rowsScanned: stats.rowsDecoded,
      rowsMatched: stats.rowsMatched,
      filesRead: left.filesRead + right.filesRead,
      filesSkipped: stats.filesSkipped,
      rowGroupsRead: stats.rowGroupsRead,
      rowGroupsSkipped: stats.rowGroupsSkipped,
    };
  }

  if (canUseVectorSubqueryJoin(ast, aggregates, grouped)) {
    const stats = benchQueryStats();
    const join = ast.subqueryJoin;
    const left = await scanSubqueryJoinSourceBatch(
      store,
      lake,
      fixture,
      ast,
      stats,
      "left",
      metadataCache,
    );
    const right = await scanSubqueryJoinSourceBatch(
      store,
      lake,
      join.source,
      ast,
      stats,
      "right",
      metadataCache,
    );
    const joined = vectorHashJoin(left.batch, right.batch, {
      leftKey: join.leftKey,
      rightKey: join.rightKey,
      type: join.type,
      maxRightRows: 100_000,
    });
    const orderedBatch = orderVectorScanBatch(joined, ast);
    const resultBatch =
      orderedBatch === undefined
        ? undefined
        : vectorProjectBatch(orderedBatch, ast.select, ast.projections);
    rows = resultBatch === undefined ? [] : materializeBatchRows(resultBatch);
    return {
      rows,
      path: `vector hash ${join.type} join work units`,
      workUnits: left.workUnits + right.workUnits,
      bytesFetched: counters.bytesFetched,
      objectRequests: counters.totalRequests,
      rangeRequests: stats.rangeRequests,
      bytesRequested: stats.bytesRequested,
      rowsScanned: stats.rowsDecoded,
      rowsMatched: stats.rowsMatched,
      filesRead: left.filesRead + right.filesRead,
      filesSkipped: stats.filesSkipped,
      rowGroupsRead: stats.rowGroupsRead,
      rowGroupsSkipped: stats.rowGroupsSkipped,
    };
  }

  if (canUseVectorAggregate(ast, aggregates, grouped)) {
    const stats = benchQueryStats();
    let base = lake.path(ast.source);
    if (ast.where) base = base.where(ast.where);
    const workUnits = await planBenchmarkWorkUnits(store, base, {
      metadataCache,
      rowGroupsPerWorkUnit,
    });
    rows = grouped
      ? materializeBatchRows(
          await aggregateParquetGroupTasksBatch(store, workUnits, ast.groupBy ?? [], aggregates, {
            stats,
            metadataCache,
            maxConcurrentTasks: aggregateConcurrency,
            ...(ast.orderBy === undefined ? {} : { orderBy: ast.orderBy }),
            ...(ast.limit === undefined ? {} : { limit: ast.limit }),
            ...(ast.offset === undefined ? {} : { offset: ast.offset }),
          }),
        )
      : [
          await aggregateParquetTasks(store, workUnits, aggregates, {
            stats,
            metadataCache,
            maxConcurrentTasks: aggregateConcurrency,
          }),
        ];
    return {
      rows,
      path: grouped ? "vector group work units" : "vector work units",
      workUnits: workUnits.length,
      bytesFetched: counters.bytesFetched,
      objectRequests: counters.totalRequests,
      rangeRequests: stats.rangeRequests,
      bytesRequested: stats.bytesRequested,
      rowsScanned: stats.rowsDecoded,
      rowsMatched: stats.rowsMatched,
      filesRead: workUnitSourceMetrics(workUnits).filesRead,
      filesSkipped: stats.filesSkipped,
      rowGroupsRead: stats.rowGroupsRead,
      rowGroupsSkipped: stats.rowGroupsSkipped,
    };
  }

  if (canUseVectorScan(ast, aggregates, grouped)) {
    const stats = benchQueryStats();
    let base = lake.path(ast.source).select(referencedColumns(ast));
    if (ast.where) base = base.where(ast.where);
    const workUnits = await planBenchmarkWorkUnits(store, base, {
      metadataCache,
      rowGroupsPerWorkUnit,
    });
    let candidates;
    for (const task of workUnits) {
      for await (const batch of scanParquetTaskColumnBatches(store, task, {
        metadataCache,
        stats,
      })) {
        const combined =
          candidates === undefined ? batch.batch : concatBatches([candidates, batch.batch]);
        candidates =
          ast.limit === undefined
            ? combined
            : gatherBatch(
                combined,
                vectorTopKIndices(combined, ast.orderBy, (ast.offset ?? 0) + ast.limit),
              );
      }
    }
    const orderedBatch = orderVectorScanBatch(candidates, ast);
    const resultBatch =
      orderedBatch === undefined
        ? undefined
        : vectorProjectBatch(orderedBatch, ast.select, ast.projections);
    rows = resultBatch === undefined ? [] : materializeBatchRows(resultBatch);
    return {
      rows,
      path: vectorScanPath(ast),
      workUnits: workUnits.length + scalarMetric(ast, "workUnits") + cteMetric(ast, "workUnits"),
      bytesFetched: counters.bytesFetched,
      objectRequests: counters.totalRequests,
      rangeRequests:
        stats.rangeRequests + scalarMetric(ast, "rangeRequests") + cteMetric(ast, "rangeRequests"),
      bytesRequested:
        stats.bytesRequested +
        scalarMetric(ast, "bytesRequested") +
        cteMetric(ast, "bytesRequested"),
      rowsScanned:
        stats.rowsDecoded + scalarMetric(ast, "rowsScanned") + cteMetric(ast, "rowsScanned"),
      rowsMatched:
        stats.rowsMatched + scalarMetric(ast, "rowsMatched") + cteMetric(ast, "rowsMatched"),
      filesRead: workUnitSourceMetrics(workUnits).filesRead,
      filesSkipped: stats.filesSkipped,
      rowGroupsRead:
        stats.rowGroupsRead + scalarMetric(ast, "rowGroupsRead") + cteMetric(ast, "rowGroupsRead"),
      rowGroupsSkipped:
        stats.rowGroupsSkipped +
        scalarMetric(ast, "rowGroupsSkipped") +
        cteMetric(ast, "rowGroupsSkipped"),
    };
  }

  if (aggregates || grouped) {
    let base = lake.path(ast.source);
    if (ast.where) base = base.where(ast.where);
    rows = await base.groupBy(ast.groupBy ?? []).aggregate(aggregates ?? {}, {
      ...(ast.having !== undefined ? { having: ast.having } : {}),
      ...(ast.orderBy !== undefined ? { orderBy: ast.orderBy } : {}),
      ...(ast.distinct === true ? {} : ast.limit !== undefined ? { limit: ast.limit } : {}),
      ...(ast.distinct === true ? {} : ast.offset !== undefined ? { offset: ast.offset } : {}),
    });
    if (ast.distinct === true) rows = distinctRows(rows);
    const offset = ast.offset ?? 0;
    if (ast.distinct === true && ast.limit !== undefined)
      rows = rows.slice(offset, offset + ast.limit);
    else if (ast.distinct === true && offset > 0) rows = rows.slice(offset);
  } else {
    let query = lake.path(ast.source);
    for (const [alias, expr] of Object.entries(ast.windows ?? {}))
      query = query.window(alias, expr);
    if (ast.projections !== undefined) query = query.select(referencedColumns(ast));
    else if (ast.select) query = query.select(ast.select);
    if (ast.where) query = query.where(ast.where);
    if (ast.qualify) query = query.qualify(ast.qualify);
    if (ast.orderBy) query = query.orderBy(ast.orderBy);
    if (ast.offset !== undefined) query = query.offset(ast.offset);
    if (ast.limit !== undefined) query = query.limit(ast.limit);
    const result = query.run();
    rows = await result.toArray();
    if (ast.projections !== undefined) rows = projectRows(rows, ast);
    stats = result.stats;
    if (ast.windows !== undefined || ast.qualify !== undefined) {
      ast.__benchWindowExecution = (await result.explain()).json.windowPlan?.execution;
    }
  }

  return {
    rows,
    path:
      aggregates || grouped
        ? "row aggregate"
        : ast.__benchWindowExecution === undefined
          ? "row scan"
          : ast.__benchWindowExecution,
    workUnits: "",
    bytesFetched: counters.bytesFetched,
    objectRequests: counters.totalRequests,
    rangeRequests: stats?.rangeRequests ?? counters.getRange,
    bytesRequested: stats?.bytesRequested ?? counters.bytesFetched,
    rowsScanned: stats?.rowsDecoded ?? "",
    rowsMatched: stats?.rowsMatched ?? "",
    filesRead: stats?.filesRead ?? "",
    filesSkipped: stats?.filesSkipped ?? "",
    rowGroupsRead: stats?.rowGroupsRead ?? "",
    rowGroupsSkipped: stats?.rowGroupsSkipped ?? "",
  };
}

function vectorScanPath(ast) {
  if (ast.orderBy === undefined) return "vector scan work units";
  return ast.limit === undefined ? "vector ordered work units" : "vector top-k work units";
}

function canUseVectorAggregate(ast, aggregates, grouped) {
  return (
    aggregates !== undefined &&
    !hasWindows(ast) &&
    ast.having === undefined &&
    ast.distinct !== true &&
    (grouped || (ast.orderBy === undefined && ast.limit === undefined && ast.offset === undefined))
  );
}

function canUseVectorJoin(ast, aggregates, grouped) {
  return (
    ast.join !== undefined &&
    !hasWindows(ast) &&
    aggregates === undefined &&
    !grouped &&
    ast.distinct !== true &&
    ast.having === undefined &&
    (ast.join.type === "inner" || ast.join.type === "left")
  );
}

async function resolveBenchmarkScalarSubqueries(store, lake, ast, defaultSource, metadataCache) {
  if (ast.scalarSubqueries === undefined) return ast;
  const values = new Map();
  const metrics = {
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    rangeRequests: 0,
    bytesRequested: 0,
    rowsMatched: 0,
    rowsScanned: 0,
    workUnits: 0,
  };
  for (const [id, subquery] of Object.entries(ast.scalarSubqueries)) {
    const query = normalizeBenchmarkSources(subquery.query, defaultSource);
    const result = await scalarSubqueryRows(store, lake, query, metadataCache);
    const rows = result.rows;
    if (rows.length > 1) throw new Error("Scalar subquery returned more than one row");
    values.set(id, rows.length === 0 ? null : (rows[0]?.[subquery.column] ?? null));
    metrics.rowGroupsRead += result.rowGroupsRead;
    metrics.rowGroupsSkipped += result.rowGroupsSkipped;
    metrics.rangeRequests += result.rangeRequests;
    metrics.bytesRequested += result.bytesRequested;
    metrics.rowsMatched += result.rowsMatched;
    metrics.rowsScanned += result.rowsScanned;
    metrics.workUnits += result.workUnits;
  }
  const out = { ...ast, __benchScalarMetrics: metrics };
  if (ast.where !== undefined) out.where = replaceScalarSubqueryExpr(ast.where, values);
  if (ast.having !== undefined) out.having = replaceScalarSubqueryExpr(ast.having, values);
  if (ast.projections !== undefined) {
    out.projections = Object.fromEntries(
      Object.entries(ast.projections).map(([alias, expr]) => [
        alias,
        replaceScalarSubqueryExpr(expr, values),
      ]),
    );
  }
  delete out.scalarSubqueries;
  return out;
}

async function scalarSubqueryRows(store, lake, ast, metadataCache) {
  const aggregates =
    ast.aggregates && Object.keys(ast.aggregates).length > 0 ? ast.aggregates : undefined;
  const grouped = (ast.groupBy?.length ?? 0) > 0;
  if (!canUseVectorAggregate(ast, aggregates, grouped)) {
    throw new Error("Scalar subquery benchmark requires a vectorizable aggregate subquery");
  }
  const stats = benchQueryStats();
  let base = lake.path(ast.source);
  if (ast.where) base = base.where(ast.where);
  const workUnits = await planBenchmarkWorkUnits(store, base, {
    metadataCache,
    rowGroupsPerWorkUnit,
  });
  const row = await aggregateParquetTasks(store, workUnits, aggregates, {
    stats,
    metadataCache,
    maxConcurrentTasks: aggregateConcurrency,
  });
  return {
    rows: [row],
    rowGroupsRead: stats.rowGroupsRead,
    rowGroupsSkipped: stats.rowGroupsSkipped,
    rangeRequests: stats.rangeRequests,
    bytesRequested: stats.bytesRequested,
    rowsMatched: stats.rowsMatched,
    rowsScanned: stats.rowsDecoded,
    workUnits: workUnits.length,
  };
}

function scalarMetric(ast, metric) {
  return ast.__benchScalarMetrics?.[metric] ?? 0;
}

function normalizeBenchmarkSources(ast, defaultSource, sourceAliases = {}) {
  const out = { ...ast, source: benchmarkSource(ast.source, defaultSource, sourceAliases) };
  if (ast.cte !== undefined) {
    out.cte = {
      ...ast.cte,
      query: normalizeBenchmarkSources(ast.cte.query, defaultSource, sourceAliases),
    };
    out.source = ast.source;
  }
  if (ast.scalarSubqueries !== undefined) {
    out.scalarSubqueries = Object.fromEntries(
      Object.entries(ast.scalarSubqueries).map(([id, subquery]) => [
        id,
        {
          ...subquery,
          query: normalizeBenchmarkSources(subquery.query, defaultSource, sourceAliases),
        },
      ]),
    );
  }
  return out;
}

function benchmarkSource(source, defaultSource, sourceAliases) {
  if (source in sourceAliases) return sourceAliases[source];
  return source === "input" ? defaultSource : source;
}

function replaceScalarSubqueryExpr(expr, values) {
  switch (expr.kind) {
    case "call":
      if (expr.fn === "__lakeql_scalar_subquery") {
        const id = expr.args[0];
        if (id?.kind !== "literal" || typeof id.value !== "string" || !values.has(id.value)) {
          throw new Error("Invalid scalar subquery placeholder");
        }
        return { kind: "literal", value: values.get(id.value) };
      }
      return { ...expr, args: expr.args.map((arg) => replaceScalarSubqueryExpr(arg, values)) };
    case "compare":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, values),
        right: replaceScalarSubqueryExpr(expr.right, values),
      };
    case "between":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, values),
        low: replaceScalarSubqueryExpr(expr.low, values),
        high: replaceScalarSubqueryExpr(expr.high, values),
      };
    case "in":
      return {
        ...expr,
        target: replaceScalarSubqueryExpr(expr.target, values),
        values: expr.values.map((value) => replaceScalarSubqueryExpr(value, values)),
      };
    case "logical":
      return {
        ...expr,
        operands: expr.operands.map((operand) => replaceScalarSubqueryExpr(operand, values)),
      };
    case "not":
      return { ...expr, operand: replaceScalarSubqueryExpr(expr.operand, values) };
    case "null-check":
    case "like":
      return { ...expr, target: replaceScalarSubqueryExpr(expr.target, values) };
    case "arithmetic":
      return {
        ...expr,
        left: replaceScalarSubqueryExpr(expr.left, values),
        right: replaceScalarSubqueryExpr(expr.right, values),
      };
    case "case":
      return {
        ...expr,
        whens: expr.whens.map((branch) => ({
          when: replaceScalarSubqueryExpr(branch.when, values),
          value: replaceScalarSubqueryExpr(branch.value, values),
        })),
        ...(expr.else === undefined ? {} : { else: replaceScalarSubqueryExpr(expr.else, values) }),
      };
    case "column":
    case "literal":
      return expr;
  }
}

function canUseVectorSubqueryJoin(ast, aggregates, grouped) {
  return (
    ast.subqueryJoin !== undefined &&
    !hasWindows(ast) &&
    aggregates === undefined &&
    !grouped &&
    ast.distinct !== true &&
    ast.having === undefined &&
    (ast.subqueryJoin.type === "semi" || ast.subqueryJoin.type === "anti")
  );
}

function canUseVectorScan(ast, aggregates, grouped) {
  return aggregates === undefined && !grouped && ast.distinct !== true && !hasWindows(ast);
}

function hasWindows(ast) {
  return ast.windows !== undefined || ast.qualify !== undefined;
}

async function scanSubqueryJoinSourceBatch(store, lake, source, ast, stats, side, metadataCache) {
  const columns = referencedSubqueryJoinColumns(ast, side);
  const join = ast.subqueryJoin;
  let base = lake.path(source);
  if (side === "right" && join.where !== undefined) base = base.where(join.where);
  if (columns.length > 0) base = base.select(columns);
  const workUnits = await planBenchmarkWorkUnits(store, base, {
    metadataCache,
    rowGroupsPerWorkUnit,
  });
  let combined;
  for (const task of workUnits) {
    for await (const batch of scanParquetTaskColumnBatches(store, task, {
      metadataCache,
      stats,
    })) {
      combined = combined === undefined ? batch.batch : concatBatches([combined, batch.batch]);
    }
  }
  if (combined === undefined) {
    throw new Error(`No batches produced for joined source ${source}`);
  }
  return {
    batch: combined,
    ...workUnitSourceMetrics(workUnits),
  };
}

async function scanJoinedSourceBatch(store, lake, source, alias, ast, stats, metadataCache) {
  const columns = referencedJoinSideColumns(ast, alias);
  let base = lake.path(source);
  if (columns.length > 0) base = base.select(columns);
  const workUnits = await planBenchmarkWorkUnits(store, base, {
    metadataCache,
    rowGroupsPerWorkUnit,
  });
  let combined;
  for (const task of workUnits) {
    for await (const batch of scanParquetTaskColumnBatches(store, task, {
      metadataCache,
      stats,
    })) {
      const qualified = qualifyBatchColumns(batch.batch, alias);
      combined = combined === undefined ? qualified : concatBatches([combined, qualified]);
    }
  }
  if (combined === undefined) {
    throw new Error(`No batches produced for joined source ${source}`);
  }
  return {
    batch: combined,
    ...workUnitSourceMetrics(workUnits),
  };
}

function filterVectorJoinBatch(batch, ast) {
  if (ast.where === undefined) return batch;
  const selection = tryPredicateSelection(batch, ast.where);
  if (selection === undefined) throw new Error("Join benchmark WHERE is not vectorizable");
  return gatherBatch(batch, selectionIndices(selection));
}

function qualifyBatchColumns(batch, alias) {
  return {
    rowCount: batch.rowCount,
    columns: Object.fromEntries(
      Object.entries(batch.columns).map(([column, vector]) => [`${alias}.${column}`, vector]),
    ),
  };
}

function benchQueryStats() {
  return queryStats("bench-duckdb");
}

function projectRows(rows, ast) {
  return rows.map((row) => {
    const out = {};
    for (const select of ast.select ?? []) out[select] = row[select];
    for (const [alias, expr] of Object.entries(ast.projections ?? {})) {
      out[alias] = evaluate(expr, row);
    }
    return out;
  });
}

async function runDuckDb(sql) {
  const result = await duckdb.runAndReadAll(sql);
  return result.getRowObjectsJson();
}

async function timedBenchmarkResult(fn) {
  const result = await timed(fn);
  return {
    ...benchmarkValueObject(result.value),
    ms: result.ms,
    peakRssBytes: result.peakMemoryBytes,
    peakRssDeltaBytes: result.peakMemoryDeltaBytes,
  };
}

function benchmarkValueObject(value) {
  return Array.isArray(value) ? { rows: value } : value;
}

function countingStore() {
  const inner = memoryStore();
  const counters = {
    get: 0,
    getRange: 0,
    head: 0,
    list: 0,
    put: 0,
    delete: 0,
    bytesFetched: 0,
    get totalRequests() {
      return this.get + this.getRange + this.head + this.list + this.put + this.delete;
    },
    reset() {
      this.get = 0;
      this.getRange = 0;
      this.head = 0;
      this.list = 0;
      this.put = 0;
      this.delete = 0;
      this.bytesFetched = 0;
    },
  };
  return {
    counters,
    store: {
      async get(path) {
        counters.get += 1;
        const bytes = await inner.get(path);
        if (bytes !== null) counters.bytesFetched += bytes.byteLength;
        return bytes;
      },
      async getRange(path, range) {
        counters.getRange += 1;
        const bytes = await inner.getRange(path, range);
        counters.bytesFetched += bytes.byteLength;
        return bytes;
      },
      async put(path, body, options) {
        counters.put += 1;
        return await inner.put(path, body, options);
      },
      async delete(path) {
        counters.delete += 1;
        return await inner.delete(path);
      },
      async head(path) {
        counters.head += 1;
        return await inner.head(path);
      },
      async *list(prefix, options) {
        counters.list += 1;
        yield* inner.list(prefix, options);
      },
    },
  };
}

function distinctRows(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = stableStringify(normalizeRow(row));
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function assertSameRows(name, lakeqlRows, duckdbRows) {
  const left = lakeqlRows.map(normalizeRow);
  const right = duckdbRows.map(normalizeRow);
  if (!sameRows(left, right)) {
    throw new Error(
      `${name} result mismatch\nlakeql: ${JSON.stringify(lakeqlRows)}\nduckdb: ${JSON.stringify(duckdbRows)}`,
    );
  }
}

function sameRows(left, right) {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    if (!sameRow(left[index], right[index])) return false;
  }
  return true;
}

function sameRow(left, right) {
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();
  if (JSON.stringify(leftKeys) !== JSON.stringify(rightKeys)) return false;
  for (const key of leftKeys) {
    if (!sameValue(left[key], right[key])) return false;
  }
  return true;
}

function sameValue(left, right) {
  if (typeof left === "number" && typeof right === "number") {
    const scale = Math.max(1, Math.abs(left), Math.abs(right));
    return Math.abs(left - right) <= scale * 1e-12;
  }
  return Object.is(left, right);
}

function assertLakeqlExpectations(name, actual, expected) {
  if (expected === undefined) return;
  for (const [metric, value] of Object.entries(expected)) {
    const actualValue = actual[metric];
    if (actualValue !== value) {
      throw new Error(
        `${name} lakeql metric mismatch for ${metric}: expected ${JSON.stringify(
          value,
        )}, got ${JSON.stringify(actualValue)}`,
      );
    }
  }
}

function normalizeRow(row) {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [
      key,
      typeof value === "string" && /^-?\d+(?:\.\d+)?$/u.test(value) ? Number(value) : value,
    ]),
  );
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(",")}}`;
}
