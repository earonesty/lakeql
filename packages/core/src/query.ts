import { LaQLError } from "./errors.js";
import { encodeJsonLine, jsonSafeValue, matches } from "./evaluator.js";
import type { Expr } from "./expr.js";
import { col, eq, gt, gte, isIn, isNotNull, isNull, lt, lte, ne, notIn } from "./expr.js";
import {
  assertBookmarkMatches,
  createBookmark,
  createTaskManifest,
  stableStringify,
  type TaskManifest,
} from "./manifest.js";
import { classifyPredicate, type PredicatePlan } from "./predicate-plan.js";
import type { ObjectInfo, ObjectStore } from "./store.js";
import type { Bookmark, BookmarkQuery, QueryStats, Row, SliceResult } from "./types.js";

const textEncoder = new TextEncoder();

export interface QueryBudget {
  maxFiles?: number;
  maxBytes?: number;
  maxRowsDecoded?: number;
  maxOutputRows?: number;
  maxRangeRequests?: number;
  maxElapsedMs?: number;
  /** Maximum rows an operator may buffer in memory for orderBy/top-k work. */
  maxBufferedRows?: number;
  /** Maximum deterministic serialized bytes an in-memory operator may retain. */
  maxMemoryBytes?: number;
}

export type QueryPolicyContext = Record<string, unknown>;

export interface QueryPolicy {
  /** Restrict visible and predicate columns. When no select is supplied, this becomes the projection. */
  allowedColumns?: string[];
  /** Hard cap on rows returned by a query, applied even when the caller omits limit. */
  maxLimit?: number;
  /** Additional caller-owned predicate applied to every query. */
  rowFilter?: Expr | ((context: QueryPolicyContext) => Expr | undefined);
  context?: QueryPolicyContext;
}

export interface LakeConfig {
  store: ObjectStore;
  scanner: ScanAdapter;
  budget?: QueryBudget;
  policy?: QueryPolicy;
  now?: () => number;
  queryId?: () => string;
}

export interface ScanOptions {
  columns?: string[];
  where?: Expr;
  batchSize: number;
  stats: QueryStats;
  budget: QueryBudget;
  now: () => number;
  startedAt: number;
}

export interface ScanAdapter {
  scan(path: string, options: ScanOptions): AsyncIterable<Row[]>;
  planTask?(path: string, options: ScanTaskPlanOptions): Promise<ScanTaskPlan>;
}

export interface ScanTaskPlanOptions {
  columns?: string[];
  where?: Expr;
  partitionValues: Record<string, string>;
}

export interface ScanTaskPlan {
  rowGroupRanges: { start: number; end: number }[];
}

export interface PathQueryInit {
  source: string;
  select?: string[];
  where?: Expr;
  orderBy?: OrderByTerm[];
  limit?: number;
  offset?: number;
  batchSize?: number;
  hive?: boolean;
}

export interface OrderByTerm {
  column: string;
  direction?: "asc" | "desc";
  nulls?: "first" | "last";
}

export interface SliceOptions {
  maxRows: number;
  bookmark?: Bookmark;
}

export interface QueryRunOptions {
  slice: SliceOptions;
}

export interface ResumableBatchOptions {
  bookmarkEvery: number;
  bookmark?: Bookmark;
}

export interface CsvStreamOptions {
  /** Emit the header row. Defaults to true. */
  header?: boolean;
  /** Column order for CSV output. Defaults to select columns or first-row keys. */
  columns?: string[];
}

export type AggregateOp =
  | "count"
  | "sum"
  | "avg"
  | "min"
  | "max"
  | "count_distinct"
  | "approx_count_distinct"
  | "first"
  | "last"
  | "any";

export interface AggregateExpr {
  op: AggregateOp;
  column?: string;
}

export type AggregateSpec = Record<string, AggregateExpr>;

export interface AggregateOptions {
  maxGroups?: number;
  operatorState?: Uint8Array | AggregateOperatorState;
}

export interface AggregateResult {
  rows: Row[];
  operatorState: Uint8Array;
}

export interface TopKOptions {
  operatorState?: Uint8Array | TopKOperatorState;
}

export interface TopKResult {
  rows: Row[];
  operatorState: Uint8Array;
}

export interface TopKOperatorState {
  version: 1;
  orderBy: OrderByTerm[];
  offset: number;
  limit: number;
  rows: Record<string, OperatorSnapshotValue>[];
}

export type OperatorSnapshotValue = string | number | boolean | null;

export type AggregateSnapshotValue = OperatorSnapshotValue;

export interface AggregateOperatorState {
  version: 1;
  groupColumns: string[];
  spec: AggregateSpec;
  groups: AggregateGroupSnapshot[];
}

export interface AggregateGroupSnapshot {
  key: string;
  keys: Record<string, AggregateSnapshotValue>;
  states: Record<string, AggregateStateSnapshot>;
}

export type AggregateStateSnapshot =
  | { op: "count"; count: number }
  | { op: "sum"; sum: number }
  | { op: "avg"; sum: number; count: number }
  | { op: "min" | "max"; value: AggregateSnapshotValue }
  | { op: "count_distinct" | "approx_count_distinct"; values: string[] }
  | { op: "first" | "last" | "any"; seen: boolean; value: AggregateSnapshotValue };

export interface TaskInput {
  path: string;
  etag?: string;
  rowGroupRanges: { start: number; end: number }[];
  projectedColumns?: string[];
  residualPredicate?: Expr;
  partitionValues: Record<string, string>;
}

export interface ExplainJson {
  queryId: string;
  filesPlanned: number;
  filesSkipped: number;
  projectedColumns: string[];
  predicatePlan: PredicatePlan;
  tasks: TaskInput[];
}

export interface ExplainResult {
  text: string;
  json: ExplainJson;
}

export interface JsonQueryV1 {
  version: 1;
  from: string;
  select?: string[];
  where?: JsonExpr;
  orderBy?: JsonOrderByTerm[];
  limit?: number;
  offset?: number;
}

export interface JsonOrderByTerm {
  column: string;
  direction?: "asc" | "desc";
  nulls?: "first" | "last";
}

export type JsonExpr =
  | { eq: [string, unknown] }
  | { ne: [string, unknown] }
  | { lt: [string, unknown] }
  | { lte: [string, unknown] }
  | { gt: [string, unknown] }
  | { gte: [string, unknown] }
  | { in: [string, unknown[]] }
  | { notIn: [string, unknown[]] }
  | { between: [string, unknown, unknown] }
  | { isNull: string }
  | { isNotNull: string }
  | { like: [string, string] }
  | { ilike: [string, string] }
  | { and: JsonExpr[] }
  | { or: JsonExpr[] }
  | { not: JsonExpr };

export class Lake {
  readonly store: ObjectStore;
  private readonly scanner: ScanAdapter;
  private readonly budget: QueryBudget;
  private readonly policy: QueryPolicy;
  private readonly now: () => number;
  private readonly queryId: () => string;

  constructor(config: LakeConfig) {
    this.store = config.store;
    this.scanner = config.scanner;
    this.budget = config.budget ?? {};
    this.policy = config.policy ?? {};
    this.now = config.now ?? (() => performance.now());
    this.queryId = config.queryId ?? (() => `q_${Math.random().toString(36).slice(2)}`);
  }

  path(source: string): QueryBuilder {
    return new QueryBuilder(this, { source });
  }

  hive(source: string): QueryBuilder {
    return new QueryBuilder(this, { source, hive: true });
  }

  query(input: JsonQueryV1): QueryBuilder {
    return new QueryBuilder(this, parseJsonQuery(input));
  }

  resume(bookmark: Bookmark): ResumedQuery {
    return new ResumedQuery(this, bookmark);
  }

  createResult(init: PathQueryInit): QueryResult {
    const effective = applyQueryPolicy(init, this.policy);
    return new QueryResult({
      ...effective,
      lake: this,
      bookmarkQuery: cloneBookmarkQuery(init),
      budget: this.budget,
      now: this.now,
      queryId: this.queryId(),
      scanner: this.scanner,
    });
  }
}

export class ResumedQuery {
  private readonly lake: Lake;
  private readonly bookmark: Bookmark;

  constructor(lake: Lake, bookmark: Bookmark) {
    this.lake = lake;
    this.bookmark = bookmark;
  }

  run(options: QueryRunOptions): Promise<SliceResult> {
    if (this.bookmark.query === undefined) {
      throw new LaQLError("LAQL_BOOKMARK_INVALID", "Bookmark does not contain a resumable query");
    }
    return this.lake.createResult(this.bookmark.query).slice({
      ...options.slice,
      bookmark: this.bookmark,
    });
  }
}

export class QueryBuilder {
  private readonly lake: Lake;
  private readonly init: PathQueryInit;

  constructor(lake: Lake, init: PathQueryInit) {
    this.lake = lake;
    this.init = init;
  }

  select(columns: string[]): QueryBuilder {
    return new QueryBuilder(this.lake, { ...this.init, select: columns });
  }

  where(expr: Expr): QueryBuilder {
    return new QueryBuilder(this.lake, { ...this.init, where: expr });
  }

  orderBy(terms: OrderByTerm[]): QueryBuilder {
    return new QueryBuilder(this.lake, { ...this.init, orderBy: normalizeOrderBy(terms) });
  }

  limit(limit: number): QueryBuilder {
    return new QueryBuilder(this.lake, { ...this.init, limit });
  }

  offset(offset: number): QueryBuilder {
    return new QueryBuilder(this.lake, { ...this.init, offset });
  }

  batchSize(batchSize: number): QueryBuilder {
    return new QueryBuilder(this.lake, { ...this.init, batchSize });
  }

  explain(): Promise<ExplainResult> {
    return this.run().explain();
  }

  planTasks(): Promise<TaskInput[]> {
    return this.run().planTasks();
  }

  taskManifest(jobId?: string): Promise<TaskManifest> {
    return this.run().taskManifest(jobId);
  }

  run(): QueryResult;
  run(options: QueryRunOptions): Promise<SliceResult>;
  run(options?: QueryRunOptions): QueryResult | Promise<SliceResult> {
    const result = this.lake.createResult(this.init);
    if (options) return result.slice(options.slice);
    return result;
  }

  rows(): AsyncIterable<Row> {
    return this.run().rows();
  }

  batches(): AsyncIterable<Row[]> {
    return this.run().batches();
  }

  toArray(): Promise<Row[]> {
    return this.run().toArray();
  }

  topKWithState(options: TopKOptions = {}): Promise<TopKResult> {
    return this.run().topKWithState(options);
  }

  first(): Promise<Row | undefined> {
    return this.run().first();
  }

  count(): Promise<number> {
    return this.run().count();
  }

  streamNdjson(): ReadableStream<Uint8Array> {
    return this.run().streamNdjson();
  }

  streamJson(): ReadableStream<Uint8Array> {
    return this.run().streamJson();
  }

  streamCsv(options: CsvStreamOptions = {}): ReadableStream<Uint8Array> {
    return this.run().streamCsv(options);
  }

  resumableBatches(options: ResumableBatchOptions): AsyncIterable<SliceResult> {
    return this.run().resumableBatches(options);
  }

  groupBy(columns: string[]): AggregationBuilder {
    return new AggregationBuilder(this.run(), columns);
  }
}

export class AggregationBuilder {
  private readonly result: QueryResult;
  private readonly columns: string[];

  constructor(result: QueryResult, columns: string[]) {
    this.result = result;
    this.columns = columns;
  }

  aggregate(spec: AggregateSpec, options: AggregateOptions = {}): Promise<Row[]> {
    return this.result.aggregate(this.columns, spec, options);
  }

  aggregateWithState(
    spec: AggregateSpec,
    options: AggregateOptions = {},
  ): Promise<AggregateResult> {
    return this.result.aggregateWithState(this.columns, spec, options);
  }
}

interface QueryResultConfig extends PathQueryInit {
  lake: Lake;
  bookmarkQuery: BookmarkQuery;
  scanner: ScanAdapter;
  budget: QueryBudget;
  now: () => number;
  queryId: string;
}

export class QueryResult {
  readonly stats: QueryStats;
  private readonly config: QueryResultConfig;

  constructor(config: QueryResultConfig) {
    validateQueryInit(config);
    this.config = config;
    this.stats = initialStats(config.queryId);
  }

  async *rows(): AsyncIterable<Row> {
    for await (const batch of this.batches()) {
      for (const row of batch) yield row;
    }
  }

  async *batches(): AsyncIterable<Row[]> {
    if (this.config.orderBy !== undefined) {
      yield* this.orderedBatches();
      return;
    }
    const config = this.config;
    const { stats } = this;
    const startedAt = config.now();
    let offsetSkipped = 0;
    let returned = 0;
    const { planned: paths, skipped: skippedFiles } = await this.planObjects();
    stats.filesSkipped = skippedFiles;
    const columns = projectedReadColumns(config.select, config.where);
    for (const object of paths) {
      stats.filesPlanned += 1;
      stats.filesRead += 1;
      stats.bytesRequested += object.size;
      enforceBudget(config.budget, stats, config.now, startedAt);
      const scanOptions: ScanOptions = {
        batchSize: config.batchSize ?? 4096,
        stats,
        budget: config.budget,
        now: config.now,
        startedAt,
      };
      const partitionValues = config.hive ? parseHivePartitions(object.path) : {};
      const physicalColumns = columns?.filter((column) => !(column in partitionValues));
      if (physicalColumns !== undefined && physicalColumns.length > 0) {
        scanOptions.columns = physicalColumns;
      }
      if (config.where !== undefined) scanOptions.where = config.where;
      for await (const rawBatch of config.scanner.scan(object.path, scanOptions)) {
        const out: Row[] = [];
        for (const rawRow of rawBatch) {
          const row = config.hive ? { ...partitionValues, ...rawRow } : rawRow;
          stats.rowsDecoded += 1;
          enforceBudget(config.budget, stats, config.now, startedAt);
          if (!matches(config.where, row)) continue;
          stats.rowsMatched += 1;
          if (offsetSkipped < (config.offset ?? 0)) {
            offsetSkipped += 1;
            continue;
          }
          if (config.limit !== undefined && returned >= config.limit) break;
          out.push(project(row, config.select));
          returned += 1;
          stats.rowsReturned += 1;
          enforceBudget(config.budget, stats, config.now, startedAt);
        }
        stats.elapsedMs = config.now() - startedAt;
        if (out.length > 0) yield out;
        if (config.limit !== undefined && returned >= config.limit) return;
      }
    }
    stats.elapsedMs = config.now() - startedAt;
  }

  async toArray(): Promise<Row[]> {
    const rows: Row[] = [];
    for await (const row of this.rows()) rows.push(row);
    return rows;
  }

  async topKWithState(options: TopKOptions = {}): Promise<TopKResult> {
    const config = this.config;
    if (config.orderBy === undefined) {
      throw new LaQLError("LAQL_TYPE_ERROR", "topKWithState requires orderBy");
    }
    if (config.limit === undefined) {
      throw new LaQLError("LAQL_TYPE_ERROR", "topKWithState requires limit");
    }
    const orderBy = normalizeOrderBy(config.orderBy);
    const topK = (config.offset ?? 0) + config.limit;
    const matched = topKRowsFromState(
      orderBy,
      config.offset ?? 0,
      config.limit,
      options.operatorState,
    );
    enforceBufferedRowsBudget(config.budget, matched.length);
    enforceOperatorMemoryBudget(config.budget, estimateOperatorMemoryBytes(matched));
    const startedAt = config.now();
    await this.collectOrderedMatches(matched, topK, startedAt);
    matched.sort((left, right) => compareRows(left, right, orderBy));
    const start = config.offset ?? 0;
    const end = start + config.limit;
    const rows = matched.slice(start, end).map((row) => project(row, config.select));
    for (const _row of rows) {
      this.stats.rowsReturned += 1;
      enforceBudget(config.budget, this.stats, config.now, startedAt);
    }
    this.stats.elapsedMs = config.now() - startedAt;
    const state = topKOperatorState(orderBy, config.offset ?? 0, config.limit, matched);
    return { rows, operatorState: serializeTopKOperatorState(state) };
  }

  async first(): Promise<Row | undefined> {
    for await (const row of this.rows()) return row;
    return undefined;
  }

  async count(): Promise<number> {
    let count = 0;
    for await (const _row of this.rows()) count += 1;
    return count;
  }

  async planTasks(): Promise<TaskInput[]> {
    const { planned: objects } = await this.planObjects();
    return this.tasksFromObjects(objects);
  }

  async taskManifest(jobId = this.config.queryId): Promise<TaskManifest> {
    return createTaskManifest({ jobId, tasks: await this.planTasks() });
  }

  async slice(options: SliceOptions): Promise<SliceResult> {
    if (!Number.isInteger(options.maxRows) || options.maxRows <= 0) {
      throw new LaQLError("LAQL_TYPE_ERROR", "slice maxRows must be a positive integer");
    }
    const manifest = await this.taskManifest();
    const startOffset = options.bookmark?.position.rowOffset ?? 0;
    if (options.bookmark) assertBookmarkMatches(options.bookmark, manifest.planFingerprint);

    const rows: Row[] = [];
    let skipped = 0;
    let seen = 0;
    let hasMore = false;
    for await (const row of this.rows()) {
      if (skipped < startOffset) {
        skipped += 1;
        seen += 1;
        continue;
      }
      if (rows.length >= options.maxRows) {
        hasMore = true;
        break;
      }
      rows.push(row);
      seen += 1;
    }

    if (!hasMore) return { rows };
    const position: { fileIndex: number; rowGroup: number; rowOffset: number; taskId?: string } = {
      fileIndex: 0,
      rowGroup: 0,
      rowOffset: seen,
    };
    const taskId = manifest.tasks[0]?.id;
    if (taskId !== undefined) position.taskId = taskId;
    return {
      rows,
      bookmark: createBookmark({
        planFingerprint: manifest.planFingerprint,
        snapshot: manifest.snapshot,
        query: this.config.bookmarkQuery,
        position,
      }),
    };
  }

  async *resumableBatches(options: ResumableBatchOptions): AsyncIterable<SliceResult> {
    let bookmark = options.bookmark;
    while (true) {
      const sliceOptions: SliceOptions = { maxRows: options.bookmarkEvery };
      if (bookmark !== undefined) sliceOptions.bookmark = bookmark;
      const result = await this.slice(sliceOptions);
      yield result;
      if (!result.bookmark) return;
      bookmark = result.bookmark;
    }
  }

  async aggregate(
    groupColumns: string[],
    spec: AggregateSpec,
    options: AggregateOptions = {},
  ): Promise<Row[]> {
    return (await this.aggregateWithState(groupColumns, spec, options)).rows;
  }

  async aggregateWithState(
    groupColumns: string[],
    spec: AggregateSpec,
    options: AggregateOptions = {},
  ): Promise<AggregateResult> {
    validateAggregateRequest(groupColumns, spec, options);
    const groups = aggregateGroupsFromState(groupColumns, spec, options.operatorState);
    for await (const row of this.rows()) {
      const keyValues = groupColumns.map((column) => valueForColumn(row, column));
      const key = stableStringify(keyValues);
      let group = groups.get(key);
      if (!group) {
        if (options.maxGroups !== undefined && groups.size >= options.maxGroups) {
          throw new LaQLError(
            "LAQL_GROUP_LIMIT_EXCEEDED",
            `Query exceeded group budget (${groups.size + 1} > ${options.maxGroups})`,
            { limit: options.maxGroups, actual: groups.size + 1 },
          );
        }
        group = createAggregateGroup(groupColumns, keyValues, spec);
        groups.set(key, group);
      }
      group.add(row);
      enforceOperatorMemoryBudget(
        this.config.budget,
        estimateAggregateOperatorMemoryBytes(groupColumns, spec, groups),
      );
    }
    const state = aggregateOperatorState(groupColumns, spec, groups);
    return {
      rows: [...groups.values()].map((group) => group.finish()),
      operatorState: serializeAggregateOperatorState(state),
    };
  }

  private async *orderedBatches(): AsyncIterable<Row[]> {
    const config = this.config;
    const { stats } = this;
    const matched: Row[] = [];
    const topK = config.limit === undefined ? undefined : (config.offset ?? 0) + config.limit;
    const startedAt = config.now();
    await this.collectOrderedMatches(matched, topK, startedAt);
    matched.sort((left, right) => compareRows(left, right, config.orderBy ?? []));
    const start = config.offset ?? 0;
    const end = config.limit === undefined ? matched.length : start + config.limit;
    const batchSize = config.batchSize ?? 4096;
    let batch: Row[] = [];
    for (const row of matched.slice(start, end)) {
      batch.push(project(row, config.select));
      stats.rowsReturned += 1;
      enforceBudget(config.budget, stats, config.now, startedAt);
      if (batch.length >= batchSize) {
        stats.elapsedMs = config.now() - startedAt;
        yield batch;
        batch = [];
      }
    }
    stats.elapsedMs = config.now() - startedAt;
    if (batch.length > 0) yield batch;
  }

  private async collectOrderedMatches(
    matched: Row[],
    topK: number | undefined,
    startedAt: number,
  ): Promise<void> {
    const config = this.config;
    const { stats } = this;
    const { planned: paths, skipped: skippedFiles } = await this.planObjects();
    stats.filesSkipped = skippedFiles;
    const orderBy = config.orderBy ?? [];
    const columns = projectedReadColumns(config.select, config.where, orderBy);
    for (const object of paths) {
      stats.filesPlanned += 1;
      stats.filesRead += 1;
      stats.bytesRequested += object.size;
      enforceBudget(config.budget, stats, config.now, startedAt);
      const scanOptions: ScanOptions = {
        batchSize: config.batchSize ?? 4096,
        stats,
        budget: config.budget,
        now: config.now,
        startedAt,
      };
      const partitionValues = config.hive ? parseHivePartitions(object.path) : {};
      const physicalColumns = columns?.filter((column) => !(column in partitionValues));
      if (physicalColumns !== undefined && physicalColumns.length > 0) {
        scanOptions.columns = physicalColumns;
      }
      if (config.where !== undefined) scanOptions.where = config.where;
      for await (const rawBatch of config.scanner.scan(object.path, scanOptions)) {
        for (const rawRow of rawBatch) {
          const row = config.hive ? { ...partitionValues, ...rawRow } : rawRow;
          stats.rowsDecoded += 1;
          enforceBudget(config.budget, stats, config.now, startedAt);
          if (!matches(config.where, row)) continue;
          stats.rowsMatched += 1;
          validateSortRow(row, orderBy);
          addOrderedMatch(matched, row, orderBy, topK);
          enforceBufferedRowsBudget(config.budget, matched.length);
          enforceOperatorMemoryBudget(config.budget, estimateOperatorMemoryBytes(matched));
        }
      }
    }
    stats.elapsedMs = config.now() - startedAt;
  }

  async explain(): Promise<ExplainResult> {
    const { planned, skipped } = await this.planObjects();
    const tasks = await this.tasksFromObjects(planned);
    const projectedColumns = projectedReadColumns(this.config.select, this.config.where) ?? [];
    const json: ExplainJson = {
      queryId: this.stats.queryId,
      filesPlanned: tasks.length,
      filesSkipped: skipped,
      projectedColumns,
      predicatePlan: classifyPredicate(this.config.where, {
        partitionColumns: partitionColumnsFromTasks(tasks),
        rowGroupStatsColumns: projectedColumns,
      }),
      tasks,
    };
    return {
      json,
      text: [
        `files planned: ${json.filesPlanned}`,
        `files skipped: ${json.filesSkipped}`,
        `projected columns: ${json.projectedColumns.join(", ") || "*"}`,
      ].join("\n"),
    };
  }

  private async tasksFromObjects(objects: ObjectInfo[]): Promise<TaskInput[]> {
    const config = this.config;
    const projectedColumns = projectedReadColumns(config.select, config.where, config.orderBy);
    const tasks: TaskInput[] = [];
    for (const object of objects) {
      const partitionValues = config.hive ? parseHivePartitions(object.path) : {};
      const physicalColumns = projectedColumns?.filter((column) => !(column in partitionValues));
      const scanPlan = await config.scanner.planTask?.(object.path, {
        partitionValues,
        ...(physicalColumns !== undefined && physicalColumns.length > 0
          ? { columns: physicalColumns }
          : {}),
        ...(config.where !== undefined ? { where: config.where } : {}),
      });
      const task: TaskInput = {
        path: object.path,
        rowGroupRanges: scanPlan?.rowGroupRanges ?? [{ start: 0, end: Number.POSITIVE_INFINITY }],
        partitionValues,
      };
      if (object.etag !== undefined) task.etag = object.etag;
      if (projectedColumns !== undefined) task.projectedColumns = projectedColumns;
      if (config.where !== undefined) task.residualPredicate = config.where;
      tasks.push(task);
    }
    return tasks;
  }

  private async planObjects(): Promise<{ planned: ObjectInfo[]; skipped: number }> {
    const config = this.config;
    const objects = await expandPaths(config.lake.store, config.source);
    if (!config.hive || !config.where) return { planned: objects, skipped: 0 };
    const planned: ObjectInfo[] = [];
    let skipped = 0;
    for (const object of objects) {
      const partitions = parseHivePartitions(object.path);
      if (partitionMayMatch(config.where, partitions)) planned.push(object);
      else skipped += 1;
    }
    return { planned, skipped };
  }

  streamNdjson(): ReadableStream<Uint8Array> {
    const iterator = this.rows()[Symbol.asyncIterator]();
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        const next = await iterator.next();
        if (next.done) {
          controller.close();
          return;
        }
        controller.enqueue(encodeJsonLine(next.value));
      },
      async cancel() {
        await iterator.return?.();
      },
    });
  }

  streamJson(): ReadableStream<Uint8Array> {
    const iterator = this.rows()[Symbol.asyncIterator]();
    let first = true;
    return new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("["));
      },
      async pull(controller) {
        const next = await iterator.next();
        if (next.done) {
          controller.enqueue(new TextEncoder().encode("]"));
          controller.close();
          return;
        }
        const prefix = first ? "" : ",";
        first = false;
        controller.enqueue(
          new TextEncoder().encode(`${prefix}${JSON.stringify(jsonSafeValue(next.value))}`),
        );
      },
      async cancel() {
        await iterator.return?.();
      },
    });
  }

  streamCsv(options: CsvStreamOptions = {}): ReadableStream<Uint8Array> {
    const iterator = this.rows()[Symbol.asyncIterator]();
    const header = options.header ?? true;
    let columns = options.columns ?? this.config.select;
    let initialized = false;
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        if (initialized) {
          const next = await iterator.next();
          if (next.done) {
            controller.close();
            return;
          }
          controller.enqueue(textEncoder.encode(csvRow(next.value, columns ?? [])));
          return;
        }

        initialized = true;
        const next = await iterator.next();
        if (next.done) {
          if (header && columns !== undefined) {
            controller.enqueue(textEncoder.encode(csvHeader(columns)));
          }
          controller.close();
          return;
        }
        columns = columns ?? Object.keys(next.value);
        const out = `${header ? csvHeader(columns) : ""}${csvRow(next.value, columns)}`;
        controller.enqueue(textEncoder.encode(out));
      },
      async cancel() {
        await iterator.return?.();
      },
    });
  }
}

function csvHeader(columns: string[]): string {
  return `${columns.map(csvCell).join(",")}\n`;
}

function csvRow(row: Row, columns: string[]): string {
  return `${columns.map((column) => csvCell(row[column])).join(",")}\n`;
}

function csvCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const safe = jsonSafeValue(value);
  const text =
    typeof safe === "string" || typeof safe === "number" || typeof safe === "boolean"
      ? String(safe)
      : JSON.stringify(safe);
  return /[",\n\r]/u.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

interface AggregateState {
  add(value: unknown): void;
  finish(): unknown;
  snapshot(): AggregateStateSnapshot;
}

class AggregateGroup {
  private readonly keys: Record<string, unknown>;
  private readonly states: Record<string, AggregateState>;

  constructor(keys: Record<string, unknown>, states: Record<string, AggregateState>) {
    this.keys = keys;
    this.states = states;
  }

  add(row: Row): void {
    for (const [alias, state] of Object.entries(this.states)) {
      state.add(aggregateValue(row, alias, stateSpecs.get(state)));
    }
  }

  finish(): Row {
    const out: Row = { ...this.keys };
    for (const [alias, state] of Object.entries(this.states)) out[alias] = state.finish();
    return out;
  }

  snapshot(key: string): AggregateGroupSnapshot {
    const states: Record<string, AggregateStateSnapshot> = {};
    for (const [alias, state] of Object.entries(this.states)) states[alias] = state.snapshot();
    return { key, keys: snapshotRecord(this.keys), states };
  }
}

const stateSpecs = new WeakMap<AggregateState, AggregateExpr>();

function createAggregateGroup(
  groupColumns: string[],
  keyValues: unknown[],
  spec: AggregateSpec,
): AggregateGroup {
  const keys: Record<string, unknown> = {};
  for (let index = 0; index < groupColumns.length; index += 1) {
    const column = groupColumns[index];
    if (column !== undefined) keys[column] = keyValues[index];
  }
  const states: Record<string, AggregateState> = {};
  for (const [alias, aggregate] of Object.entries(spec)) {
    const state = createAggregateState(aggregate);
    stateSpecs.set(state, aggregate);
    states[alias] = state;
  }
  return new AggregateGroup(keys, states);
}

function createAggregateState(aggregate: AggregateExpr): AggregateState {
  switch (aggregate.op) {
    case "count":
      return new CountState();
    case "sum":
      return new SumState();
    case "avg":
      return new AvgState();
    case "min":
      return new MinMaxState("min");
    case "max":
      return new MinMaxState("max");
    case "count_distinct":
      return new CountDistinctState("count_distinct");
    case "approx_count_distinct":
      return new CountDistinctState("approx_count_distinct");
    case "first":
      return new FirstState();
    case "last":
      return new LastState();
    case "any":
      return new AnyState();
  }
}

class CountState implements AggregateState {
  constructor(private count = 0) {}

  add(_value: unknown): void {
    this.count += 1;
  }

  finish(): number {
    return this.count;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: "count", count: this.count };
  }
}

class SumState implements AggregateState {
  constructor(private sum = 0) {}

  add(value: unknown): void {
    if (value === null || value === undefined) return;
    if (typeof value !== "number") throwAggregateType("sum");
    this.sum += value;
  }

  finish(): number {
    return this.sum;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: "sum", sum: this.sum };
  }
}

class AvgState implements AggregateState {
  constructor(
    private sum = 0,
    private count = 0,
  ) {}

  add(value: unknown): void {
    if (value === null || value === undefined) return;
    if (typeof value !== "number") throwAggregateType("avg");
    this.sum += value;
    this.count += 1;
  }

  finish(): number | null {
    return this.count === 0 ? null : this.sum / this.count;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: "avg", sum: this.sum, count: this.count };
  }
}

class MinMaxState implements AggregateState {
  constructor(
    private readonly op: "min" | "max",
    private value: string | number | boolean | null = null,
  ) {}

  add(value: unknown): void {
    if (value === null || value === undefined) return;
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
      throwAggregateType(this.op);
    }
    if (this.value === null) {
      this.value = value;
      return;
    }
    if (typeof this.value !== typeof value) throwAggregateType(this.op);
    if (this.op === "min" ? value < this.value : value > this.value) this.value = value;
  }

  finish(): string | number | boolean | null {
    return this.value;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: this.op, value: this.value };
  }
}

class CountDistinctState implements AggregateState {
  private readonly values: Set<string>;

  constructor(
    private readonly op: "count_distinct" | "approx_count_distinct",
    values: string[] = [],
  ) {
    this.values = new Set(values);
  }

  add(value: unknown): void {
    if (value === null || value === undefined) return;
    this.values.add(stableStringify(value));
  }

  finish(): number {
    return this.values.size;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: this.op, values: [...this.values].sort() };
  }
}

class FirstState implements AggregateState {
  constructor(
    protected value: unknown = undefined,
    protected seen = false,
  ) {}

  add(value: unknown): void {
    if (this.seen) return;
    this.value = value;
    this.seen = true;
  }

  finish(): unknown {
    return this.seen ? this.value : null;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: "first", seen: this.seen, value: snapshotValue(this.seen ? this.value : null) };
  }
}

class LastState implements AggregateState {
  constructor(
    private value: unknown = undefined,
    private seen = false,
  ) {}

  add(value: unknown): void {
    this.value = value;
    this.seen = true;
  }

  finish(): unknown {
    return this.seen ? this.value : null;
  }

  snapshot(): AggregateStateSnapshot {
    return { op: "last", seen: this.seen, value: snapshotValue(this.seen ? this.value : null) };
  }
}

class AnyState extends FirstState {
  override snapshot(): AggregateStateSnapshot {
    return { op: "any", seen: this.seen, value: snapshotValue(this.seen ? this.value : null) };
  }
}

export function serializeAggregateOperatorState(state: AggregateOperatorState): Uint8Array {
  return new TextEncoder().encode(stableStringify(state));
}

export function deserializeAggregateOperatorState(
  bytes: Uint8Array | AggregateOperatorState,
): AggregateOperatorState {
  if (!(bytes instanceof Uint8Array)) return validateAggregateOperatorState(bytes);
  return validateAggregateOperatorState(JSON.parse(new TextDecoder().decode(bytes)));
}

export function serializeTopKOperatorState(state: TopKOperatorState): Uint8Array {
  return new TextEncoder().encode(stableStringify(state));
}

export function deserializeTopKOperatorState(
  bytes: Uint8Array | TopKOperatorState,
): TopKOperatorState {
  if (!(bytes instanceof Uint8Array)) return validateTopKOperatorState(bytes);
  return validateTopKOperatorState(JSON.parse(new TextDecoder().decode(bytes)));
}

function topKRowsFromState(
  orderBy: OrderByTerm[],
  offset: number,
  limit: number,
  state: Uint8Array | TopKOperatorState | undefined,
): Row[] {
  if (state === undefined) return [];
  const snapshot = deserializeTopKOperatorState(state);
  if (
    stableStringify(snapshot.orderBy) !== stableStringify(orderBy) ||
    snapshot.offset !== offset ||
    snapshot.limit !== limit
  ) {
    throw new LaQLError("LAQL_BOOKMARK_STALE", "Top-k operator state does not match request", {
      stateOrderBy: snapshot.orderBy,
      orderBy,
      stateOffset: snapshot.offset,
      offset,
      stateLimit: snapshot.limit,
      limit,
    });
  }
  return snapshot.rows.map((row) => ({ ...row }));
}

function topKOperatorState(
  orderBy: OrderByTerm[],
  offset: number,
  limit: number,
  rows: Row[],
): TopKOperatorState {
  return {
    version: 1,
    orderBy: normalizeOrderBy(orderBy),
    offset,
    limit,
    rows: rows.map(snapshotRecord),
  };
}

function validateTopKOperatorState(value: unknown): TopKOperatorState {
  if (!isTopKOperatorState(value)) {
    throw new LaQLError("LAQL_BOOKMARK_INVALID", "Top-k operator state is invalid");
  }
  return {
    version: 1,
    orderBy: normalizeOrderBy(value.orderBy),
    offset: value.offset,
    limit: value.limit,
    rows: value.rows.map((row) => ({ ...row })),
  };
}

function aggregateGroupsFromState(
  groupColumns: string[],
  spec: AggregateSpec,
  state: Uint8Array | AggregateOperatorState | undefined,
): Map<string, AggregateGroup> {
  if (state === undefined) return new Map();
  const snapshot = deserializeAggregateOperatorState(state);
  if (
    stableStringify(snapshot.groupColumns) !== stableStringify(groupColumns) ||
    stableStringify(snapshot.spec) !== stableStringify(spec)
  ) {
    throw new LaQLError("LAQL_BOOKMARK_STALE", "Aggregate operator state does not match request", {
      stateGroupColumns: snapshot.groupColumns,
      groupColumns,
    });
  }
  const groups = new Map<string, AggregateGroup>();
  for (const group of snapshot.groups)
    groups.set(group.key, aggregateGroupFromSnapshot(spec, group));
  return groups;
}

function aggregateGroupFromSnapshot(
  spec: AggregateSpec,
  snapshot: AggregateGroupSnapshot,
): AggregateGroup {
  const states: Record<string, AggregateState> = {};
  for (const [alias, aggregate] of Object.entries(spec)) {
    const state = snapshot.states[alias];
    if (state === undefined) {
      throw new LaQLError("LAQL_BOOKMARK_INVALID", `Missing aggregate state ${alias}`);
    }
    states[alias] = aggregateStateFromSnapshot(aggregate, state);
    stateSpecs.set(states[alias], aggregate);
  }
  return new AggregateGroup(snapshot.keys, states);
}

function aggregateStateFromSnapshot(
  aggregate: AggregateExpr,
  snapshot: AggregateStateSnapshot,
): AggregateState {
  if (snapshot.op !== aggregate.op) {
    throw new LaQLError("LAQL_BOOKMARK_INVALID", "Aggregate state operation mismatch", {
      expected: aggregate.op,
      actual: snapshot.op,
    });
  }
  switch (snapshot.op) {
    case "count":
      return new CountState(snapshot.count);
    case "sum":
      return new SumState(snapshot.sum);
    case "avg":
      return new AvgState(snapshot.sum, snapshot.count);
    case "min":
    case "max":
      return new MinMaxState(snapshot.op, snapshot.value);
    case "count_distinct":
    case "approx_count_distinct":
      return new CountDistinctState(snapshot.op, snapshot.values);
    case "first":
      return new FirstState(snapshot.value, snapshot.seen);
    case "last":
      return new LastState(snapshot.value, snapshot.seen);
    case "any":
      return new AnyState(snapshot.value, snapshot.seen);
  }
}

function aggregateOperatorState(
  groupColumns: string[],
  spec: AggregateSpec,
  groups: Map<string, AggregateGroup>,
): AggregateOperatorState {
  return {
    version: 1,
    groupColumns: [...groupColumns],
    spec,
    groups: [...groups.entries()]
      .map(([key, group]) => group.snapshot(key))
      .sort((left, right) => left.key.localeCompare(right.key)),
  };
}

function validateAggregateOperatorState(value: unknown): AggregateOperatorState {
  if (!isAggregateOperatorState(value)) {
    throw new LaQLError("LAQL_BOOKMARK_INVALID", "Aggregate operator state is invalid");
  }
  return value;
}

function isAggregateOperatorState(value: unknown): value is AggregateOperatorState {
  return (
    isRecord(value) &&
    value.version === 1 &&
    Array.isArray(value.groupColumns) &&
    value.groupColumns.every((column) => typeof column === "string") &&
    isRecord(value.spec) &&
    Object.values(value.spec).every(isAggregateExpr) &&
    Array.isArray(value.groups) &&
    value.groups.every(isAggregateGroupSnapshot)
  );
}

function isTopKOperatorState(value: unknown): value is TopKOperatorState {
  return (
    isRecord(value) &&
    value.version === 1 &&
    Array.isArray(value.orderBy) &&
    value.orderBy.every(isOrderByTerm) &&
    typeof value.offset === "number" &&
    Number.isInteger(value.offset) &&
    value.offset >= 0 &&
    typeof value.limit === "number" &&
    Number.isInteger(value.limit) &&
    value.limit >= 0 &&
    Array.isArray(value.rows) &&
    value.rows.length <= value.offset + value.limit &&
    value.rows.every(isTopKSnapshotRow)
  );
}

function isOrderByTerm(value: unknown): value is OrderByTerm {
  return (
    isRecord(value) &&
    typeof value.column === "string" &&
    value.column.length > 0 &&
    (value.direction === undefined || value.direction === "asc" || value.direction === "desc") &&
    (value.nulls === undefined || value.nulls === "first" || value.nulls === "last")
  );
}

function isTopKSnapshotRow(value: unknown): value is Record<string, OperatorSnapshotValue> {
  return isRecord(value) && Object.values(value).every(isOperatorSnapshotValue);
}

function isAggregateGroupSnapshot(value: unknown): value is AggregateGroupSnapshot {
  return (
    isRecord(value) &&
    typeof value.key === "string" &&
    isRecord(value.keys) &&
    Object.values(value.keys).every(isAggregateSnapshotValue) &&
    isRecord(value.states) &&
    Object.values(value.states).every(isAggregateStateSnapshot)
  );
}

function isAggregateStateSnapshot(value: unknown): value is AggregateStateSnapshot {
  if (!isRecord(value) || typeof value.op !== "string") return false;
  switch (value.op) {
    case "count":
      return typeof value.count === "number";
    case "sum":
      return typeof value.sum === "number";
    case "avg":
      return typeof value.sum === "number" && typeof value.count === "number";
    case "min":
    case "max":
      return isAggregateSnapshotValue(value.value);
    case "count_distinct":
    case "approx_count_distinct":
      return (
        Array.isArray(value.values) && value.values.every((inner) => typeof inner === "string")
      );
    case "first":
    case "last":
    case "any":
      return typeof value.seen === "boolean" && isAggregateSnapshotValue(value.value);
    default:
      return false;
  }
}

function isAggregateSnapshotValue(value: unknown): value is AggregateSnapshotValue {
  return isOperatorSnapshotValue(value);
}

function isOperatorSnapshotValue(value: unknown): value is OperatorSnapshotValue {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  );
}

function isAggregateExpr(value: unknown): value is AggregateExpr {
  return isRecord(value) && typeof value.op === "string";
}

function snapshotRecord(record: Record<string, unknown>): Record<string, AggregateSnapshotValue> {
  const out: Record<string, AggregateSnapshotValue> = {};
  for (const [key, value] of Object.entries(record)) out[key] = snapshotValue(value);
  return out;
}

function snapshotValue(value: unknown): AggregateSnapshotValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  throw new LaQLError("LAQL_TYPE_ERROR", "Aggregate operator state values must be JSON scalars", {
    value,
  });
}

function aggregateValue(row: Row, alias: string, aggregate: AggregateExpr | undefined): unknown {
  if (!aggregate) throw new LaQLError("LAQL_VALIDATION_ERROR", `Missing aggregate ${alias}`);
  if (aggregate.op === "count" && aggregate.column === undefined) return true;
  if (aggregate.column === undefined) {
    throw new LaQLError("LAQL_TYPE_ERROR", `${aggregate.op} requires a column`, { aggregate });
  }
  return valueForColumn(row, aggregate.column);
}

function valueForColumn(row: Row, column: string): unknown {
  if (!(column in row)) {
    throw new LaQLError("LAQL_UNKNOWN_COLUMN", `Unknown column ${column}`, { column });
  }
  return row[column];
}

function validateAggregateRequest(
  groupColumns: string[],
  spec: AggregateSpec,
  options: AggregateOptions,
): void {
  if (groupColumns.some((column) => typeof column !== "string" || column.length === 0)) {
    throw new LaQLError("LAQL_TYPE_ERROR", "groupBy columns must be non-empty strings");
  }
  if (Object.keys(spec).length === 0) {
    throw new LaQLError("LAQL_TYPE_ERROR", "aggregate spec must contain at least one aggregate");
  }
  if (
    options.maxGroups !== undefined &&
    (!Number.isInteger(options.maxGroups) || options.maxGroups < 1)
  ) {
    throw new LaQLError("LAQL_TYPE_ERROR", "maxGroups must be a positive integer");
  }
  for (const aggregate of Object.values(spec)) validateAggregateExpr(aggregate);
}

function validateAggregateExpr(aggregate: AggregateExpr): void {
  const ops: AggregateOp[] = [
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "count_distinct",
    "approx_count_distinct",
    "first",
    "last",
    "any",
  ];
  if (!ops.includes(aggregate.op)) {
    throw new LaQLError("LAQL_TYPE_ERROR", `Unsupported aggregate ${aggregate.op}`, { aggregate });
  }
  if (aggregate.op !== "count" && aggregate.column === undefined) {
    throw new LaQLError("LAQL_TYPE_ERROR", `${aggregate.op} requires a column`, { aggregate });
  }
}

function throwAggregateType(op: string): never {
  throw new LaQLError("LAQL_TYPE_ERROR", `${op} aggregate received an incompatible value`, { op });
}

export function parseJsonQuery(input: unknown): PathQueryInit {
  if (!isRecord(input)) throwParse("JSON query must be an object");
  if (input.version !== 1) throwParse("JSON query version must be 1");
  if (typeof input.from !== "string") throwParse("JSON query from must be a string");
  const init: PathQueryInit = { source: input.from };
  if (input.select !== undefined) {
    if (!Array.isArray(input.select) || input.select.some((value) => typeof value !== "string")) {
      throwParse("JSON query select must be an array of strings");
    }
    init.select = input.select;
  }
  if (input.where !== undefined) init.where = parseJsonExpr(input.where);
  if (input.orderBy !== undefined) {
    if (!Array.isArray(input.orderBy)) throwParse("JSON query orderBy must be an array");
    init.orderBy = normalizeOrderBy(input.orderBy.map(parseJsonOrderByTerm));
  }
  if (input.limit !== undefined) init.limit = parseNonNegativeInt(input.limit, "limit");
  if (input.offset !== undefined) init.offset = parseNonNegativeInt(input.offset, "offset");
  return init;
}

function parseJsonOrderByTerm(input: unknown): OrderByTerm {
  if (!isRecord(input)) throwParse("JSON query orderBy terms must be objects");
  if (typeof input.column !== "string") throwParse("JSON query orderBy column must be a string");
  const term: OrderByTerm = { column: input.column };
  if (input.direction !== undefined) {
    if (input.direction !== "asc" && input.direction !== "desc") {
      throwParse("JSON query orderBy direction must be asc or desc");
    }
    term.direction = input.direction;
  }
  if (input.nulls !== undefined) {
    if (input.nulls !== "first" && input.nulls !== "last") {
      throwParse("JSON query orderBy nulls must be first or last");
    }
    term.nulls = input.nulls;
  }
  return term;
}

function parseJsonExpr(input: unknown): Expr {
  if (!isRecord(input)) throwParse("JSON expression must be an object");
  const entries = Object.entries(input);
  if (entries.length !== 1) throwParse("JSON expression must have exactly one operator");
  const [op, value] = entries[0] ?? [];
  switch (op) {
    case "eq":
      return tuple2(value, op, eq);
    case "ne":
      return tuple2(value, op, ne);
    case "lt":
      return tuple2(value, op, lt);
    case "lte":
      return tuple2(value, op, lte);
    case "gt":
      return tuple2(value, op, gt);
    case "gte":
      return tuple2(value, op, gte);
    case "in":
      return tupleArray(value, op, isIn);
    case "notIn":
      return tupleArray(value, op, notIn);
    case "between": {
      const tuple = requireTuple(value, op, 3);
      return {
        kind: "between",
        target: col(requireColumn(tuple[0], op)),
        low: literal(tuple[1]),
        high: literal(tuple[2]),
      };
    }
    case "isNull":
      return isNull(requireColumn(value, op));
    case "isNotNull":
      return isNotNull(requireColumn(value, op));
    case "like":
      return tuplePattern(value, op, false);
    case "ilike":
      return tuplePattern(value, op, true);
    case "and":
    case "or": {
      if (!Array.isArray(value)) throwParse(`${op} expects an array`);
      if (value.length < 2) throwParse(`${op} expects at least two expressions`);
      return { kind: "logical", op, operands: value.map(parseJsonExpr) };
    }
    case "not":
      return { kind: "not", operand: parseJsonExpr(value) };
    default:
      throwParse(`Unsupported JSON expression operator ${op}`);
  }
}

function tuple2(
  value: unknown,
  op: string,
  cb: (column: string, value: string | number | boolean | bigint | null) => Expr,
): Expr {
  const tuple = requireTuple(value, op, 2);
  return cb(requireColumn(tuple[0], op), requireScalar(tuple[1], op));
}

function tupleArray(
  value: unknown,
  op: string,
  cb: (column: string, values: (string | number | boolean | bigint | null)[]) => Expr,
): Expr {
  const tuple = requireTuple(value, op, 2);
  if (!Array.isArray(tuple[1])) throwParse(`${op} values must be an array`);
  return cb(
    requireColumn(tuple[0], op),
    tuple[1].map((inner) => requireScalar(inner, op)),
  );
}

function tuplePattern(value: unknown, op: string, caseInsensitive: boolean): Expr {
  const tuple = requireTuple(value, op, 2);
  const pattern = tuple[1];
  if (typeof pattern !== "string") throwParse(`${op} pattern must be a string`);
  return { kind: "like", caseInsensitive, target: col(requireColumn(tuple[0], op)), pattern };
}

function literal(value: unknown): Expr {
  return { kind: "literal", value: requireScalar(value, "literal") };
}

function requireTuple(value: unknown, op: string, length: number): unknown[] {
  if (!Array.isArray(value) || value.length !== length) {
    throwParse(`${op} expects a ${length}-item array`);
  }
  return value;
}

function requireColumn(value: unknown, op: string): string {
  if (typeof value !== "string") throwParse(`${op} column must be a string`);
  return value;
}

function requireScalar(value: unknown, op: string): string | number | boolean | bigint | null {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  throwParse(`${op} value must be a scalar`);
}

function parseNonNegativeInt(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throwParse(`${field} must be a non-negative integer`);
  }
  return value;
}

function validateQueryInit(init: PathQueryInit): void {
  if (init.limit !== undefined && (!Number.isInteger(init.limit) || init.limit < 0)) {
    throw new LaQLError("LAQL_TYPE_ERROR", "limit must be a non-negative integer");
  }
  if (init.offset !== undefined && (!Number.isInteger(init.offset) || init.offset < 0)) {
    throw new LaQLError("LAQL_TYPE_ERROR", "offset must be a non-negative integer");
  }
  if (init.batchSize !== undefined && (!Number.isInteger(init.batchSize) || init.batchSize <= 0)) {
    throw new LaQLError("LAQL_TYPE_ERROR", "batchSize must be a positive integer");
  }
  if (init.orderBy !== undefined) normalizeOrderBy(init.orderBy);
}

function applyQueryPolicy(init: PathQueryInit, policy: QueryPolicy): PathQueryInit {
  const allowedColumns =
    policy.allowedColumns === undefined
      ? undefined
      : normalizeAllowedColumns(policy.allowedColumns);
  const rowFilter =
    typeof policy.rowFilter === "function"
      ? policy.rowFilter(policy.context ?? {})
      : policy.rowFilter;
  const effectiveWhere = combineWhere(init.where, rowFilter);
  validatePolicyColumns(init, effectiveWhere, allowedColumns);
  const out: PathQueryInit = {
    ...init,
  };
  const effectiveLimit = policyLimit(init.limit, policy.maxLimit);
  if (effectiveWhere !== undefined) out.where = effectiveWhere;
  else delete out.where;
  if (effectiveLimit !== undefined) out.limit = effectiveLimit;
  else delete out.limit;
  if (allowedColumns !== undefined && init.select === undefined) out.select = allowedColumns;
  return out;
}

function cloneBookmarkQuery(init: PathQueryInit): BookmarkQuery {
  const query: BookmarkQuery = { source: init.source };
  if (init.select !== undefined) query.select = [...init.select];
  if (init.where !== undefined) query.where = init.where;
  if (init.orderBy !== undefined) {
    query.orderBy = init.orderBy.map((term) => {
      const queryTerm: NonNullable<BookmarkQuery["orderBy"]>[number] = { column: term.column };
      if (term.direction !== undefined) queryTerm.direction = term.direction;
      if (term.nulls !== undefined) queryTerm.nulls = term.nulls;
      return queryTerm;
    });
  }
  if (init.limit !== undefined) query.limit = init.limit;
  if (init.offset !== undefined) query.offset = init.offset;
  if (init.batchSize !== undefined) query.batchSize = init.batchSize;
  if (init.hive !== undefined) query.hive = init.hive;
  return query;
}

function normalizeAllowedColumns(columns: string[]): string[] {
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new LaQLError("LAQL_TYPE_ERROR", "policy allowedColumns must be non-empty strings");
  }
  const unique = new Set<string>();
  for (const column of columns) {
    if (typeof column !== "string" || column.length === 0) {
      throw new LaQLError("LAQL_TYPE_ERROR", "policy allowedColumns must be non-empty strings");
    }
    unique.add(column);
  }
  return [...unique].sort();
}

function combineWhere(queryWhere: Expr | undefined, rowFilter: Expr | undefined): Expr | undefined {
  if (queryWhere === undefined) return rowFilter;
  if (rowFilter === undefined) return queryWhere;
  return { kind: "logical", op: "and", operands: [rowFilter, queryWhere] };
}

function policyLimit(limit: number | undefined, maxLimit: number | undefined): number | undefined {
  if (maxLimit === undefined) return limit;
  if (!Number.isInteger(maxLimit) || maxLimit < 0) {
    throw new LaQLError("LAQL_TYPE_ERROR", "policy maxLimit must be a non-negative integer");
  }
  return limit === undefined ? maxLimit : Math.min(limit, maxLimit);
}

function validatePolicyColumns(
  init: PathQueryInit,
  effectiveWhere: Expr | undefined,
  allowedColumns: string[] | undefined,
): void {
  if (allowedColumns === undefined) return;
  const allowed = new Set(allowedColumns);
  const requested = new Set<string>();
  for (const column of init.select ?? []) requested.add(column);
  for (const term of init.orderBy ?? []) requested.add(term.column);
  collectExprColumns(effectiveWhere, requested);
  for (const column of requested) {
    if (!allowed.has(column)) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Query references a disallowed column", {
        column,
      });
    }
  }
}

async function expandPaths(store: ObjectStore, pattern: string): Promise<ObjectInfo[]> {
  if (!hasGlob(pattern)) {
    const head = await store.head(pattern);
    if (!head) {
      throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${pattern}`, { path: pattern });
    }
    const object: ObjectInfo = { path: pattern, size: head.size };
    if (head.etag !== undefined) object.etag = head.etag;
    if (head.lastModified !== undefined) object.lastModified = head.lastModified;
    return [object];
  }
  const prefix = globPrefix(pattern);
  const regex = globRegex(pattern);
  const paths: ObjectInfo[] = [];
  for await (const object of store.list(prefix)) {
    if (regex.test(object.path)) paths.push(object);
  }
  paths.sort((a, b) => a.path.localeCompare(b.path));
  return paths;
}

function partitionColumnsFromTasks(tasks: TaskInput[]): string[] {
  const columns = new Set<string>();
  for (const task of tasks) {
    for (const column of Object.keys(task.partitionValues)) columns.add(column);
  }
  return [...columns].sort();
}

function hasGlob(pattern: string): boolean {
  return pattern.includes("*") || pattern.includes("?");
}

function globPrefix(pattern: string): string {
  const wildcard = pattern.search(/[*?]/u);
  const slash = pattern.lastIndexOf("/", wildcard);
  return slash === -1 ? "" : pattern.slice(0, slash + 1);
}

function globRegex(pattern: string): RegExp {
  let source = "^";
  for (const char of pattern) {
    if (char === "*") source += ".*";
    else if (char === "?") source += ".";
    else source += char.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  }
  return new RegExp(`${source}$`, "u");
}

function projectedReadColumns(
  select: string[] | undefined,
  where: Expr | undefined,
  orderBy: OrderByTerm[] | undefined = undefined,
): string[] | undefined {
  const columns = new Set<string>();
  for (const column of select ?? []) columns.add(column);
  for (const term of orderBy ?? []) columns.add(term.column);
  collectExprColumns(where, columns);
  return columns.size === 0 ? undefined : [...columns].sort();
}

function collectExprColumns(expr: Expr | undefined, columns: Set<string>): void {
  if (!expr) return;
  switch (expr.kind) {
    case "column":
      columns.add(expr.name);
      return;
    case "literal":
      return;
    case "compare":
      collectExprColumns(expr.left, columns);
      collectExprColumns(expr.right, columns);
      return;
    case "in":
      collectExprColumns(expr.target, columns);
      for (const value of expr.values) collectExprColumns(value, columns);
      return;
    case "between":
      collectExprColumns(expr.target, columns);
      collectExprColumns(expr.low, columns);
      collectExprColumns(expr.high, columns);
      return;
    case "null-check":
      collectExprColumns(expr.target, columns);
      return;
    case "logical":
      for (const operand of expr.operands) collectExprColumns(operand, columns);
      return;
    case "not":
      collectExprColumns(expr.operand, columns);
      return;
    case "like":
      collectExprColumns(expr.target, columns);
      return;
    case "call":
      for (const arg of expr.args) collectExprColumns(arg, columns);
      return;
  }
}

function project(row: Row, select: string[] | undefined): Row {
  if (!select) return row;
  const out: Row = {};
  for (const column of select) {
    if (!(column in row)) {
      throw new LaQLError("LAQL_UNKNOWN_COLUMN", `Unknown column ${column}`, { column });
    }
    out[column] = row[column];
  }
  return out;
}

function normalizeOrderBy(terms: OrderByTerm[]): OrderByTerm[] {
  if (!Array.isArray(terms) || terms.length === 0) {
    throw new LaQLError("LAQL_TYPE_ERROR", "orderBy must contain at least one term");
  }
  return terms.map((term) => {
    if (typeof term.column !== "string" || term.column.length === 0) {
      throw new LaQLError("LAQL_TYPE_ERROR", "orderBy columns must be non-empty strings");
    }
    const direction = term.direction ?? "asc";
    if (direction !== "asc" && direction !== "desc") {
      throw new LaQLError("LAQL_TYPE_ERROR", "orderBy direction must be asc or desc", { term });
    }
    const nulls = term.nulls ?? (direction === "asc" ? "last" : "first");
    if (nulls !== "first" && nulls !== "last") {
      throw new LaQLError("LAQL_TYPE_ERROR", "orderBy nulls must be first or last", { term });
    }
    return { column: term.column, direction, nulls };
  });
}

function compareRows(left: Row, right: Row, orderBy: OrderByTerm[]): number {
  for (const term of orderBy) {
    const comparison = compareSortValues(
      valueForColumn(left, term.column),
      valueForColumn(right, term.column),
      term,
    );
    if (comparison !== 0) return comparison;
  }
  return 0;
}

function addOrderedMatch(
  matched: Row[],
  row: Row,
  orderBy: OrderByTerm[],
  topK: number | undefined,
): void {
  if (topK === undefined) {
    matched.push(row);
    return;
  }
  if (topK === 0) return;
  if (matched.length < topK) {
    matched.push(row);
    return;
  }
  let worstIndex = 0;
  for (let index = 1; index < matched.length; index += 1) {
    const candidate = matched[index];
    const worst = matched[worstIndex];
    if (
      candidate !== undefined &&
      worst !== undefined &&
      compareRows(candidate, worst, orderBy) > 0
    ) {
      worstIndex = index;
    }
  }
  const worst = matched[worstIndex];
  if (worst !== undefined && compareRows(row, worst, orderBy) < 0) matched[worstIndex] = row;
}

function validateSortRow(row: Row, orderBy: OrderByTerm[]): void {
  for (const term of orderBy) {
    const value = valueForColumn(row, term.column);
    if (value === null || value === undefined) continue;
    if (!isSortableValue(value)) {
      throw new LaQLError("LAQL_TYPE_ERROR", "orderBy values must be scalar", {
        column: term.column,
      });
    }
  }
}

function compareSortValues(left: unknown, right: unknown, term: OrderByTerm): number {
  const leftNull = left === null || left === undefined;
  const rightNull = right === null || right === undefined;
  if (leftNull || rightNull) {
    if (leftNull && rightNull) return 0;
    const nullOrder = term.nulls === "first" ? -1 : 1;
    return leftNull ? nullOrder : -nullOrder;
  }
  if (!isSortableValue(left) || !isSortableValue(right)) {
    throw new LaQLError("LAQL_TYPE_ERROR", "orderBy values must be scalar", {
      column: term.column,
    });
  }
  if (typeof left !== typeof right) {
    throw new LaQLError("LAQL_TYPE_ERROR", "orderBy values must have matching types", {
      column: term.column,
    });
  }
  const direction = term.direction === "desc" ? -1 : 1;
  if (left < right) return -1 * direction;
  if (left > right) return direction;
  return 0;
}

function isSortableValue(value: unknown): value is string | number | bigint | boolean {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  );
}

export function parseHivePartitions(path: string): Record<string, string> {
  const values: Record<string, string> = {};
  for (const segment of path.split("/")) {
    const equals = segment.indexOf("=");
    if (equals <= 0) continue;
    const key = segment.slice(0, equals);
    const value = segment.slice(equals + 1);
    if (key && value) values[key] = decodeURIComponent(value);
  }
  return values;
}

function partitionMayMatch(expr: Expr, partitions: Record<string, string>): boolean {
  const state = partitionEval(expr, partitions);
  return state !== false;
}

type PartitionEval = boolean | "unknown";

function partitionEval(expr: Expr, partitions: Record<string, string>): PartitionEval {
  switch (expr.kind) {
    case "literal":
      return "unknown";
    case "column":
      return "unknown";
    case "compare":
    case "in":
    case "between":
    case "null-check":
    case "like":
    case "call":
      return expressionIsPartitionOnly(expr, partitions) ? matches(expr, partitions) : "unknown";
    case "not": {
      const value = partitionEval(expr.operand, partitions);
      return value === "unknown" ? "unknown" : !value;
    }
    case "logical": {
      const values = expr.operands.map((operand) => partitionEval(operand, partitions));
      if (expr.op === "and") {
        if (values.some((value) => value === false)) return false;
        if (values.every((value) => value === true)) return true;
        return "unknown";
      }
      if (values.some((value) => value === true)) return true;
      if (values.every((value) => value === false)) return false;
      return "unknown";
    }
  }
}

function expressionIsPartitionOnly(expr: Expr, partitions: Record<string, string>): boolean {
  const columns = new Set<string>();
  collectExprColumns(expr, columns);
  return columns.size > 0 && [...columns].every((column) => column in partitions);
}

function enforceBudget(
  budget: QueryBudget,
  stats: QueryStats,
  now: () => number,
  startedAt: number,
): void {
  const elapsedMs = now() - startedAt;
  stats.elapsedMs = elapsedMs;
  if (budget.maxFiles !== undefined && stats.filesRead > budget.maxFiles) {
    throwBudget("files", budget.maxFiles, stats.filesRead);
  }
  if (budget.maxBytes !== undefined && stats.bytesRequested > budget.maxBytes) {
    throwBudget("bytes", budget.maxBytes, stats.bytesRequested);
  }
  if (budget.maxRowsDecoded !== undefined && stats.rowsDecoded > budget.maxRowsDecoded) {
    throwBudget("rows decoded", budget.maxRowsDecoded, stats.rowsDecoded);
  }
  if (budget.maxOutputRows !== undefined && stats.rowsReturned > budget.maxOutputRows) {
    throwBudget("output rows", budget.maxOutputRows, stats.rowsReturned);
  }
  if (budget.maxRangeRequests !== undefined && stats.rangeRequests > budget.maxRangeRequests) {
    throwBudget("range requests", budget.maxRangeRequests, stats.rangeRequests);
  }
  if (budget.maxElapsedMs !== undefined && elapsedMs > budget.maxElapsedMs) {
    throwBudget("elapsed milliseconds", budget.maxElapsedMs, elapsedMs);
  }
}

function enforceBufferedRowsBudget(budget: QueryBudget, bufferedRows: number): void {
  if (budget.maxBufferedRows !== undefined && bufferedRows > budget.maxBufferedRows) {
    throwBudget("buffered rows", budget.maxBufferedRows, bufferedRows);
  }
}

function enforceOperatorMemoryBudget(budget: QueryBudget, memoryBytes: number): void {
  if (budget.maxMemoryBytes !== undefined && memoryBytes > budget.maxMemoryBytes) {
    throwBudget("operator memory bytes", budget.maxMemoryBytes, memoryBytes);
  }
}

function estimateAggregateOperatorMemoryBytes(
  groupColumns: string[],
  spec: AggregateSpec,
  groups: Map<string, AggregateGroup>,
): number {
  return estimateOperatorMemoryBytes(aggregateOperatorState(groupColumns, spec, groups));
}

function estimateOperatorMemoryBytes(value: unknown): number {
  return textEncoder.encode(stableStringify(jsonSafeValue(value))).byteLength;
}

function throwBudget(metric: string, limit: number, actual: number): never {
  throw new LaQLError(
    "LAQL_BUDGET_EXCEEDED",
    `Query exceeded ${metric} budget (${actual} > ${limit}). Add a partition filter, date filter, h3 filter, or limit.`,
    { metric, limit, actual },
  );
}

function initialStats(queryId: string): QueryStats {
  return {
    queryId,
    elapsedMs: 0,
    manifestsRead: 0,
    manifestsSkipped: 0,
    filesPlanned: 0,
    filesRead: 0,
    filesSkipped: 0,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    columnsRead: [],
    bytesRequested: 0,
    rangeRequests: 0,
    rowsDecoded: 0,
    rowsMatched: 0,
    rowsReturned: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };
}

function throwParse(message: string): never {
  throw new LaQLError("LAQL_PARSE_ERROR", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
