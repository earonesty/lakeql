import { describe, expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { add, and, between, col, eq, fn, gt, isIn, isNull, like, lit, not, or } from "./expr.js";
import { createBookmark } from "./manifest.js";
import { memoryStore } from "./memory-store.js";
import {
  type BackendAssessment,
  type BackendExecutionContext,
  type BackendPlanningContext,
  type CompiledPhysicalFragment,
  CpuPhysicalBackend,
  type PhysicalCapabilities,
  type PhysicalExecutionBackend,
  type PhysicalFragment,
  type PhysicalFragmentInput,
  type PhysicalFragmentResult,
} from "./physical.js";
import {
  type AggregateSpec,
  deserializeAggregateOperatorState,
  deserializeSortOperatorState,
  deserializeTopKOperatorState,
  deserializeWindowOperatorState,
  Lake,
  parseHivePartitions,
  parseJsonQuery,
  type ScanAdapter,
  type ScanColumnBatch,
  type ScanOptions,
  serializeAggregateOperatorState,
  serializeSortOperatorState,
  serializeTopKOperatorState,
  serializeWindowOperatorState,
  type TaskInput,
  type WindowExpr,
  type WindowTaskExecutionOptions,
} from "./query.js";
import { memoryCache, memorySpillAdapter, type RuntimeSubstrate } from "./runtime.js";
import type { ObjectInfo } from "./store.js";
import { timestampValueFromIso } from "./timestamp.js";
import type { Bookmark, Row } from "./types.js";

class RecordingPhysicalBackend implements PhysicalExecutionBackend {
  readonly id = "recording-accelerator";
  readonly compiledOperators: PhysicalFragment["operators"][number]["kind"][][] = [];
  private readonly cpu = new CpuPhysicalBackend("recording-cpu-delegate");

  capabilities(): PhysicalCapabilities {
    return {
      ...this.cpu.capabilities(),
      backendKind: "accelerator",
      supportsResidentInput: false,
      supportsResidentOutput: false,
    };
  }

  assess(fragment: PhysicalFragment, context: BackendPlanningContext): BackendAssessment {
    const assessment = this.cpu.assess(fragment, context);
    return {
      ...assessment,
      cost: {
        ...assessment.cost,
        totalMs: 0,
        computeMs: 0,
      },
    };
  }

  async compile(fragment: PhysicalFragment): Promise<CompiledPhysicalFragment> {
    this.compiledOperators.push(fragment.operators.map((operator) => operator.kind));
    await this.cpu.compile(fragment);
    return { backendId: this.id, fragment };
  }

  async execute(
    compiled: CompiledPhysicalFragment,
    input: PhysicalFragmentInput,
    context: BackendExecutionContext,
  ): Promise<PhysicalFragmentResult> {
    const delegated = await this.cpu.execute(
      { backendId: this.cpu.id, fragment: compiled.fragment },
      input,
      context,
    );
    return {
      ...delegated,
      metrics: {
        ...delegated.metrics,
        backendId: this.id,
        uploadBytes: compiled.fragment.estimates.inputBytes,
        readbackBytes: compiled.fragment.estimates.outputBytes,
        dispatches: 1,
      },
    };
  }
}

class FakeScanner implements ScanAdapter {
  readonly requestedColumns: (string[] | undefined)[] = [];
  readonly requestedColumnBatchColumns: (string[] | undefined)[] = [];
  readonly requestedColumnBatchWindows: { rowStart?: number; rowEnd?: number }[] = [];
  readonly requestedVectorBatchColumns: (string[] | undefined)[] = [];
  readonly requestedVectorBatchWindows: { rowStart?: number; rowEnd?: number }[] = [];
  readonly requestedBatchSizes: number[] = [];
  readonly requestedPaths: string[] = [];
  readonly requestedWindowTasks: TaskInput[][] = [];
  readonly scanColumns?: ScanAdapter["scanColumns"];
  readonly scanVectorBatches?: ScanAdapter["scanVectorBatches"];
  readonly executeWindowTasks?: ScanAdapter["executeWindowTasks"];

  constructor(
    private readonly rowsByPath: Record<string, Row[]>,
    enableVectorBatches = false,
    enableColumnBatches = false,
    windowTaskRows?: Row[],
  ) {
    if (windowTaskRows !== undefined) {
      this.executeWindowTasks = async (
        tasks: TaskInput[],
        _windows: Record<string, WindowExpr>,
        _options: WindowTaskExecutionOptions,
      ): Promise<Row[]> => {
        this.requestedWindowTasks.push(tasks);
        return windowTaskRows;
      };
    }
    if (enableVectorBatches) {
      this.scanVectorBatches = async function* (
        this: FakeScanner,
        path: string,
        options: ScanOptions,
      ): AsyncIterable<ScanColumnBatch> {
        this.requestedPaths.push(path);
        this.requestedVectorBatchColumns.push(options.columns);
        this.requestedVectorBatchWindows.push({
          ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
          ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        });
        const rows = this.rowsByPath[path] ?? [];
        const start = options.rowStart ?? 0;
        const end = Math.min(options.rowEnd ?? rows.length, rows.length);
        for (let offset = start; offset < end; offset += options.batchSize) {
          const slice = rows.slice(offset, Math.min(offset + options.batchSize, end));
          const columns = Object.fromEntries(
            (options.columns ?? Object.keys(slice[0] ?? {})).map((column) => [
              column,
              slice.map((row) => row[column]),
            ]),
          );
          yield { rowOffset: offset, batch: batchFromColumns(columns) };
        }
      };
    }
    if (enableColumnBatches) {
      this.scanColumns = async function* (this: FakeScanner, path: string, options: ScanOptions) {
        this.requestedPaths.push(path);
        this.requestedColumnBatchColumns.push(options.columns);
        const rows = this.rowsByPath[path] ?? [];
        for (let offset = 0; offset < rows.length; offset += options.batchSize) {
          const slice = rows.slice(offset, offset + options.batchSize);
          const columns = Object.fromEntries(
            (options.columns ?? Object.keys(slice[0] ?? {})).map((column) => [
              column,
              slice.map((row) => row[column]),
            ]),
          );
          yield batchFromColumns(columns);
        }
      };
    }
  }

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    this.requestedPaths.push(path);
    this.requestedColumns.push(options.columns);
    this.requestedBatchSizes.push(options.batchSize);
    options.stats.rangeRequests += 1;
    const rows = this.rowsByPath[path] ?? [];
    for (let offset = 0; offset < rows.length; offset += options.batchSize) {
      yield rows.slice(offset, offset + options.batchSize).map((row) => {
        if (!options.columns) return row;
        const out: Row = {};
        for (const column of options.columns) {
          if (column in row) out[column] = row[column];
        }
        return out;
      });
    }
  }

  async *scanColumnBatches(path: string, options: ScanOptions): AsyncIterable<ScanColumnBatch> {
    this.requestedPaths.push(path);
    this.requestedColumnBatchColumns.push(options.columns);
    this.requestedColumnBatchWindows.push({
      ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
      ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
    });
    const rows = this.rowsByPath[path] ?? [];
    const start = options.rowStart ?? 0;
    const end = Math.min(options.rowEnd ?? rows.length, rows.length);
    for (let offset = start; offset < end; offset += options.batchSize) {
      const slice = rows.slice(offset, Math.min(offset + options.batchSize, end));
      const columns = Object.fromEntries(
        (options.columns ?? Object.keys(slice[0] ?? {})).map((column) => [
          column,
          slice.map((row) => row[column]),
        ]),
      );
      yield { rowOffset: offset, batch: batchFromColumns(columns) };
    }
  }
}

async function makeLake(config: {
  rowsByPath: Record<string, Row[]>;
  sidecarIndex?: ConstructorParameters<typeof Lake>[0]["sidecarIndex"];
  planningCache?: ConstructorParameters<typeof Lake>[0]["planningCache"];
  budget?: ConstructorParameters<typeof Lake>[0]["budget"];
  policy?: ConstructorParameters<typeof Lake>[0]["policy"];
  now?: () => number;
  vectorBatches?: boolean;
  columnBatches?: boolean;
  windowTaskRows?: Row[];
  physicalExecution?: ConstructorParameters<typeof Lake>[0]["physicalExecution"];
}) {
  const store = memoryStore();
  for (const path of Object.keys(config.rowsByPath)) {
    await store.put(path, new Uint8Array([1, 2, 3]));
  }
  const scanner = new FakeScanner(
    config.rowsByPath,
    config.vectorBatches,
    config.columnBatches,
    config.windowTaskRows,
  );
  const lake = new Lake({
    store,
    scanner,
    ...(config.sidecarIndex !== undefined ? { sidecarIndex: config.sidecarIndex } : {}),
    ...(config.planningCache !== undefined ? { planningCache: config.planningCache } : {}),
    budget: config.budget,
    policy: config.policy,
    now: config.now,
    queryId: () => "q_test",
    physicalExecution: config.physicalExecution,
  });
  return { lake, scanner, store };
}

describe("Lake query runtime", () => {
  it("expands globs, filters, offsets, limits, projects, and records stats", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "data/b.parquet": [{ id: 3, region: "west" }],
        "data/a.parquet": [
          { id: 1, region: "west" },
          { id: 2, region: "east" },
        ],
      },
    });

    const result = lake
      .path("data/*.parquet")
      .select(["id"])
      .where(eq("region", "west"))
      .offset(1)
      .limit(1)
      .batchSize(1)
      .run();

    expect(await result.toArray()).toEqual([{ id: 3 }]);
    expect(result.stats).toMatchObject({
      queryId: "q_test",
      filesPlanned: 2,
      filesRead: 2,
      rowsDecoded: 3,
      rowsMatched: 2,
      rowsReturned: 1,
      rangeRequests: 2,
    });
    expect(scanner.requestedColumns[0]).toEqual(["id", "region"]);
  });

  it("deduplicates projected rows for distinct queries", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, region: "west" },
          { id: 2, region: "west" },
          { id: 3, region: "east" },
        ],
      },
    });

    await expect(lake.path("table").select(["region"]).distinct().toArray()).resolves.toEqual([
      { region: "west" },
      { region: "east" },
    ]);
  });

  it("pushes small unordered limits into scan batch sizing", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: Array.from({ length: 100 }, (_, id) => ({ id })),
      },
    });

    await expect(lake.path("table").select(["id"]).limit(20).toArray()).resolves.toHaveLength(20);
    expect(scanner.requestedBatchSizes).toEqual([20]);
  });

  it("late-materializes projected columns for ordered top-k queries", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [
          { score: 2, payload: "b" },
          { score: 9, payload: "winner" },
          { score: 4, payload: "d" },
          { score: 1, payload: "a" },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .select(["score", "payload"])
        .where(gt("score", 3))
        .orderBy([{ column: "score", direction: "desc" }])
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ score: 9, payload: "winner" }]);

    expect(scanner.requestedColumnBatchColumns[0]).toEqual(["score"]);
    expect(scanner.requestedColumnBatchColumns[1]).toEqual(["payload"]);
    expect(scanner.requestedColumnBatchWindows[1]).toEqual({ rowStart: 1, rowEnd: 2 });
  });

  it("evaluates computed projections and reads their source columns", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [{ id: 1, amount: 12, region: "west" }],
      },
    });

    await expect(
      lake
        .path("table")
        .select(["id"])
        .project({
          doubled: {
            kind: "arithmetic",
            op: "mul",
            left: { kind: "column", name: "amount" },
            right: { kind: "literal", value: 2 },
          },
          bucket: {
            kind: "case",
            whens: [{ when: gt("amount", 10), value: { kind: "literal", value: "large" } }],
            else: { kind: "literal", value: "small" },
          },
        })
        .toArray(),
    ).resolves.toEqual([{ id: 1, doubled: 24, bucket: "large" }]);
    expect(scanner.requestedColumns[0]).toEqual(["amount", "id"]);
  });

  it("uses vector batch scans for projected queries with vector-supported expressions", async () => {
    const { lake, scanner } = await makeLake({
      vectorBatches: true,
      rowsByPath: {
        table: [
          { id: 1, amount: 5, region: "west" },
          { id: 2, amount: 12, region: "east" },
          { id: 3, amount: 20, region: "west" },
          { id: 4, amount: 30, region: "west" },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .select(["id"])
        .project({ doubled: { kind: "arithmetic", op: "mul", left: col("amount"), right: lit(2) } })
        .where(and(eq("region", "west"), gt("amount", 10)))
        .offset(1)
        .limit(1)
        .batchSize(2)
        .toArray(),
    ).resolves.toEqual([{ id: 4, doubled: 60 }]);

    expect(scanner.requestedColumns).toEqual([]);
    expect(scanner.requestedVectorBatchColumns).toEqual([["amount", "region"], ["id"]]);
    expect(scanner.requestedVectorBatchWindows).toEqual([{}, { rowStart: 3, rowEnd: 4 }]);
  });

  it("places projected, grouped-reduce, and top-k fragments on an installed backend", async () => {
    const accelerator = new RecordingPhysicalBackend();
    const { lake } = await makeLake({
      vectorBatches: true,
      columnBatches: true,
      physicalExecution: {
        backends: [accelerator],
        acceleratorPolicy: "required",
      },
      rowsByPath: {
        table: [
          { id: 1, amount: 5, region: "west" },
          { id: 2, amount: 12, region: "east" },
          { id: 3, amount: 20, region: "west" },
          { id: 4, amount: 30, region: "east" },
        ],
      },
    });

    const projected = lake.path("table").select(["id"]).where(gt("amount", 10)).batchSize(2).run();
    await expect(projected.toArray()).resolves.toEqual([{ id: 2 }, { id: 3 }, { id: 4 }]);
    const projectedExplain = await projected.explain();
    expect(projectedExplain.json.physicalFragments).toEqual([
      expect.objectContaining({
        backendId: "recording-accelerator",
        operators: ["select", "project"],
        executions: 2,
        inputRows: 4,
        selectedRows: 3,
        outputRows: 3,
        dispatches: 2,
      }),
    ]);
    expect(projectedExplain.text).toContain("select -> project on recording-accelerator");
    expect(projected.stats).toMatchObject({
      physicalFragments: 2,
      acceleratorFragments: 2,
      acceleratorDispatches: 2,
    });

    await expect(
      lake
        .path("table")
        .where(gt("amount", 10))
        .groupBy(["region"])
        .aggregate({ total: { op: "sum", column: "amount" } }),
    ).resolves.toEqual([
      { region: "east", total: 42 },
      { region: "west", total: 20 },
    ]);
    await expect(
      lake
        .path("table")
        .where(gt("amount", 10))
        .groupBy([])
        .aggregate({
          rows: { op: "count" },
          high: { op: "max", column: "amount" },
        }),
    ).resolves.toEqual([{ rows: 3, high: 30 }]);
    await expect(
      lake
        .path("table")
        .select(["id", "amount"])
        .where(gt("amount", 5))
        .orderBy([{ column: "id", direction: "desc" }])
        .limit(2)
        .toArray(),
    ).resolves.toEqual([
      { id: 4, amount: 30 },
      { id: 3, amount: 20 },
    ]);

    expect(accelerator.compiledOperators).toEqual(
      expect.arrayContaining([
        ["select", "project"],
        ["select", "grouped-reduce"],
        ["select", "reduce"],
        ["select", "top-k"],
      ]),
    );
  });

  it("does not silently bypass required physical placement", async () => {
    const accelerator = new RecordingPhysicalBackend();
    const { lake } = await makeLake({
      physicalExecution: {
        backends: [accelerator],
        acceleratorPolicy: "required",
      },
      rowsByPath: { table: [{ id: 1 }, { id: 2 }] },
    });

    await expect(lake.path("table").select(["id"]).toArray()).rejects.toMatchObject({
      code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
    });
    await expect(
      lake
        .path("table")
        .groupBy([])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({
      code: "LAKEQL_PHYSICAL_BACKEND_UNAVAILABLE",
    });
  });

  it("pushes simple vector projection limits into scan windows", async () => {
    const { lake, scanner } = await makeLake({
      vectorBatches: true,
      rowsByPath: {
        table: Array.from({ length: 10 }, (_, id) => ({ id, amount: id * 2 })),
      },
    });

    await expect(lake.path("table").select(["id"]).limit(3).toArray()).resolves.toEqual([
      { id: 0 },
      { id: 1 },
      { id: 2 },
    ]);

    expect(scanner.requestedColumns).toEqual([]);
    expect(scanner.requestedVectorBatchColumns).toEqual([["id"]]);
    expect(scanner.requestedVectorBatchWindows).toEqual([{ rowEnd: 3 }]);
  });

  it("applies distinct before offset and limit for ordered queries", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 3, region: "west" },
          { id: 1, region: "east" },
          { id: 2, region: "east" },
          { id: 4, region: "north" },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .select(["region"])
        .distinct()
        .orderBy([{ column: "region" }])
        .offset(1)
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ region: "north" }]);
  });

  it("enforces operator budgets for distinct queries", async () => {
    const { lake } = await makeLake({
      budget: { maxBufferedRows: 1 },
      rowsByPath: {
        table: [
          { id: 1, region: "west" },
          { id: 2, region: "east" },
        ],
      },
    });

    await expect(lake.path("table").select(["region"]).distinct().toArray()).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "buffered rows", limit: 1 },
    });
  });

  it("rejects distinct queries for stateful sort and top-k operators", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [{ id: 1, region: "west" }],
      },
    });

    await expect(
      lake
        .path("table")
        .distinct()
        .orderBy([{ column: "id" }])
        .limit(1)
        .topKWithState(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      lake
        .path("table")
        .distinct()
        .orderBy([{ column: "id" }])
        .sortWithState(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("resumes object operator states and applies default spill ids", async () => {
    const rowsByPath = {
      table: [
        { id: 3, region: "west", amount: 10 },
        { id: 1, region: "east", amount: 20 },
        { id: 2, region: "west", amount: 5 },
      ],
    };
    const { lake } = await makeLake({ rowsByPath, budget: { maxBufferedRows: 2 } });

    await expect(lake.path("table").limit(1).topKWithState()).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await expect(
      lake
        .path("table")
        .orderBy([{ column: "id" }])
        .topKWithState(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(lake.path("table").sortWithState()).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });

    const spill = memorySpillAdapter();
    const topK = await lake
      .path("table")
      .orderBy([{ column: "id" }])
      .offset(1)
      .limit(1)
      .topKWithState({ spill });
    expect(topK.operatorSpill).toEqual({
      id: "topk-q_test",
      byteSize: topK.operatorState.byteLength,
    });
    await expect(spill.read("topk-q_test")).resolves.toEqual(topK.operatorState);

    const empty = await makeLake({ rowsByPath: { table: [] }, budget: { maxBufferedRows: 2 } });
    await expect(
      empty.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .offset(1)
        .limit(1)
        .topKWithState({ operatorState: deserializeTopKOperatorState(topK.operatorState) }),
    ).resolves.toMatchObject({ rows: [{ id: 2 }] });

    const sort = await lake
      .path("table")
      .orderBy([{ column: "id" }])
      .sortWithState({ spill });
    expect(sort.operatorSpill).toEqual({
      id: "sort-q_test",
      byteSize: sort.operatorState.byteLength,
    });
    await expect(spill.read("sort-q_test")).resolves.toEqual(sort.operatorState);
    const sortState = deserializeSortOperatorState(sort.operatorState);
    await expect(
      empty.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .sortWithState({ operatorState: sortState, spill }),
    ).resolves.toMatchObject({
      rows: [{ id: 1 }, { id: 2 }, { id: 3 }],
    });
    await expect(
      empty.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .sortWithState({ operatorState: sortState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    const aggregate = await lake
      .path("table")
      .groupBy(["region"])
      .aggregateWithState(
        {
          rows: { op: "count" },
          total: { op: "sum", column: "amount" },
        },
        { spill },
      );
    expect(aggregate.operatorSpill).toEqual({
      id: "aggregate-q_test",
      byteSize: aggregate.operatorState.byteLength,
    });
    await expect(spill.read("aggregate-q_test")).resolves.toEqual(aggregate.operatorState);
    const resumedAggregate = await empty.lake
      .path("table")
      .groupBy(["region"])
      .aggregateWithState(
        {
          rows: { op: "count" },
          total: { op: "sum", column: "amount" },
        },
        { operatorState: deserializeAggregateOperatorState(aggregate.operatorState) },
      );
    expect(resumedAggregate.rows).toEqual(
      expect.arrayContaining([
        { region: "west", rows: 2, total: 15 },
        { region: "east", rows: 1, total: 20 },
      ]),
    );
    expect(resumedAggregate.rows).toHaveLength(2);
  });

  it("covers fallback query execution branches for limits, hive partitions, and ordered batches", async () => {
    const hive = await makeLake({
      rowsByPath: {
        "lake/country=US/date=2026-01-01/part-1.parquet": [{ amount: 10 }, { amount: 20 }],
        "lake/country=CA/date=2026-01-01/part-1.parquet": [{ amount: 30 }],
      },
    });
    await expect(
      hive.lake
        .hive("lake/**/*.parquet")
        .where(eq("country", "US"))
        .groupBy(["country"])
        .aggregate({ rows: { op: "count" }, total: { op: "sum", column: "amount" } }),
    ).resolves.toEqual([{ country: "US", rows: 2, total: 30 }]);
    expect(hive.scanner.requestedColumns).toEqual([["amount"]]);

    const partitionOnly = await makeLake({
      rowsByPath: {
        "lake/country=US/date=2026-01-01/part-1.parquet": [{ ignored: 1 }],
      },
    });
    await expect(
      partitionOnly.lake
        .hive("lake/**/*.parquet")
        .where(eq("country", "US"))
        .groupBy(["country"])
        .aggregate({ rows: { op: "count" } }),
    ).resolves.toEqual([{ country: "US", rows: 1 }]);
    expect(partitionOnly.scanner.requestedColumns).toEqual([undefined]);

    const limited = await makeLake({
      rowsByPath: { table: [{ id: 1 }, { id: 2 }, { id: 3 }] },
    });
    await expect(limited.lake.path("table").limit(1).toArray()).resolves.toEqual([{ id: 1 }]);

    const ordered = await makeLake({
      rowsByPath: { table: [{ id: 3 }, { id: 1 }, { id: 2 }] },
    });
    const orderedBatches = [];
    for await (const batch of ordered.lake
      .path("table")
      .orderBy([{ column: "id" }])
      .batchSize(1)
      .batches()) {
      orderedBatches.push(batch);
    }
    expect(orderedBatches).toEqual([[{ id: 1 }], [{ id: 2 }], [{ id: 3 }]]);
  });

  it("covers vector projected batch selection, offset, and limit windows", async () => {
    const { lake, scanner } = await makeLake({
      vectorBatches: true,
      rowsByPath: {
        table: [
          { id: 1, keep: false, value: "a" },
          { id: 2, keep: true, value: "b" },
          { id: 3, keep: true, value: "c" },
          { id: 4, keep: true, value: "d" },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .select(["id", "value"])
        .where(eq("keep", true))
        .offset(1)
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ id: 3, value: "c" }]);
    expect(scanner.requestedVectorBatchColumns).toEqual([["keep"], ["id", "value"]]);
    expect(scanner.requestedVectorBatchWindows).toEqual([{}, { rowStart: 2, rowEnd: 3 }]);

    const unfiltered = await makeLake({
      vectorBatches: true,
      rowsByPath: { table: [{ id: 1 }, { id: 2 }, { id: 3 }] },
    });
    await expect(unfiltered.lake.path("table").select(["id"]).limit(2).toArray()).resolves.toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    expect(unfiltered.scanner.requestedVectorBatchWindows).toEqual([{ rowEnd: 2 }]);
  });

  it("supports first, count, batches, NDJSON, JSON, and CSV streams", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        "data/types.parquet": [
          { id: 1, big: 12n, label: 'a,"b"', maybe: null },
          { id: 2, big: 9007199254740993n, label: "line\nbreak", maybe: true },
        ],
      },
    });

    expect(await lake.path("data/types.parquet").first()).toEqual({
      id: 1,
      big: 12n,
      label: 'a,"b"',
      maybe: null,
    });
    expect(await lake.path("data/types.parquet").count()).toBe(2);
    await expect(lake.path("missing*.parquet").first()).rejects.toMatchObject({
      code: "LAKEQL_NO_FILES_MATCHED",
    });

    const batches: Row[][] = [];
    for await (const batch of lake.path("data/types.parquet").batchSize(1).batches()) {
      batches.push(batch);
    }
    expect(batches).toHaveLength(2);

    await expect(
      new Response(lake.path("data/types.parquet").limit(2).streamNdjson()).text(),
    ).resolves.toBe(
      '{"id":1,"big":12,"label":"a,\\"b\\"","maybe":null}\n{"id":2,"big":"9007199254740993","label":"line\\nbreak","maybe":true}\n',
    );

    await expect(
      new Response(lake.path("data/types.parquet").limit(1).streamJson()).text(),
    ).resolves.toBe('[{"id":1,"big":12,"label":"a,\\"b\\"","maybe":null}]');

    await expect(
      new Response(
        lake
          .path("data/types.parquet")
          .select(["id", "big", "label", "maybe"])
          .limit(2)
          .streamCsv(),
      ).text(),
    ).resolves.toBe('id,big,label,maybe\n1,12,"a,""b""",\n2,9007199254740993,"line\nbreak",true\n');

    await expect(
      new Response(
        lake
          .path("data/types.parquet")
          .limit(1)
          .streamCsv({ header: false, columns: ["label", "id"] }),
      ).text(),
    ).resolves.toBe('"a,""b""",1\n');

    await expect(
      new Response(
        lake.path("data/types.parquet").select(["id"]).where(eq("id", 99)).streamCsv(),
      ).text(),
    ).resolves.toBe("id\n");

    await lake.path("data/types.parquet").streamNdjson().cancel();
    await lake.path("data/types.parquet").streamJson().cancel();
    await lake.path("data/types.parquet").streamCsv().cancel();
  });

  it("parses JSON query v1 operators", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, region: "west", value: 10, maybe: null },
          { id: 2, region: "east", value: 20, maybe: "x" },
          { id: 3, region: "north", value: 30, maybe: "x" },
        ],
      },
    });

    const rows = await lake
      .query({
        version: 1,
        from: "table",
        select: ["id"],
        where: {
          or: [
            { and: [{ in: ["region", ["west", "east"]] }, { gte: ["value", 20] }] },
            { and: [{ isNull: "maybe" }, { not: { lt: ["value", 10] } }] },
          ],
        },
      })
      .toArray();

    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("parses JSON query v1 windows and qualify expressions", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", store_id: "a", amount: 10 },
          { region: "west", store_id: "b", amount: 30 },
          { region: "west", store_id: "c", amount: 20 },
          { region: "east", store_id: "d", amount: 40 },
          { region: "east", store_id: "e", amount: 5 },
        ],
      },
    });

    const rows = await lake
      .query({
        version: 1,
        from: "table",
        select: ["region", "store_id", "rank_in_region"],
        windows: {
          rank_in_region: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [{ kind: "column", name: "region" }],
              orderBy: [
                { expr: { kind: "column", name: "amount" }, direction: "desc" },
                { expr: { kind: "column", name: "store_id" }, direction: "asc" },
              ],
            },
          },
        },
        qualify: {
          kind: "compare",
          op: "lte",
          left: { kind: "column", name: "rank_in_region" },
          right: { kind: "literal", value: 2 },
        },
        orderBy: [{ column: "region" }, { column: "rank_in_region" }],
      })
      .toArray();

    expect(rows).toEqual([
      { region: "east", store_id: "d", rank_in_region: 1 },
      { region: "east", store_id: "e", rank_in_region: 2 },
      { region: "west", store_id: "b", rank_in_region: 1 },
      { region: "west", store_id: "c", rank_in_region: 2 },
    ]);
  });

  it("parses JSON expression AST families and complete window options", () => {
    const parsed = parseJsonQuery({
      version: 1,
      from: "table",
      where: {
        kind: "logical",
        op: "and",
        operands: [
          {
            kind: "in",
            negated: false,
            target: { kind: "column", name: "region" },
            values: [
              { kind: "literal", value: "west" },
              { kind: "literal", value: "east" },
            ],
          },
          {
            kind: "between",
            target: {
              kind: "arithmetic",
              op: "add",
              left: { kind: "column", name: "amount" },
              right: { kind: "literal", value: 1 },
            },
            low: { kind: "literal", value: 10 },
            high: { kind: "literal", value: 100 },
          },
          { kind: "null-check", negated: true, target: { kind: "column", name: "store_id" } },
          {
            kind: "not",
            operand: {
              kind: "like",
              caseInsensitive: true,
              target: { kind: "column", name: "region" },
              pattern: "x%",
            },
          },
          {
            kind: "case",
            whens: [
              {
                when: {
                  kind: "compare",
                  op: "gt",
                  left: { kind: "column", name: "amount" },
                  right: { kind: "literal", value: 50 },
                },
                value: { kind: "call", fn: "lower", args: [{ kind: "column", name: "region" }] },
              },
            ],
            else: { kind: "literal", value: "small" },
          },
        ],
      },
      windows: {
        running_amount: {
          fn: { aggregate: "sum" },
          args: [{ kind: "column", name: "amount" }],
          filter: {
            kind: "compare",
            op: "gte",
            left: { kind: "column", name: "amount" },
            right: { kind: "literal", value: 10 },
          },
          ignoreNulls: true,
          distinct: false,
          over: {
            partitionBy: [{ kind: "column", name: "region" }],
            orderBy: [
              { expr: { kind: "column", name: "amount" }, direction: "desc", nulls: "last" },
            ],
            frame: {
              mode: "rows",
              start: { kind: "preceding", offset: { kind: "literal", value: 2 } },
              end: { kind: "current-row" },
              exclude: "no-others",
            },
          },
        },
      },
      qualify: {
        kind: "compare",
        op: "ne",
        left: { kind: "column", name: "running_amount" },
        right: { kind: "literal", value: null },
      },
    });

    expect(parsed.where).toMatchObject({ kind: "logical", operands: expect.any(Array) });
    expect(parsed.windows?.running_amount).toMatchObject({
      fn: { aggregate: "sum" },
      filter: { kind: "compare" },
      ignoreNulls: true,
      over: { frame: { mode: "rows", exclude: "no-others" } },
    });
    expect(parsed.qualify).toMatchObject({ kind: "compare", op: "ne" });
  });

  it("collects predicate columns from every expression family for scanner projection", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [{ id: 1, a: 2, b: 3, c: "Los Angeles", d: "x" }],
      },
    });

    await lake
      .path("table")
      .select(["id"])
      .where(isIn(fn("lower", col("c")), [fn("lower", col("d")), "los angeles"]))
      .toArray();
    await lake
      .path("table")
      .where(between("a", col("b"), 4))
      .toArray();
    await lake.path("table").where(like("c", "Los%")).toArray();

    expect(scanner.requestedColumns).toEqual([["c", "d", "id"], ["a", "b"], ["c"]]);
  });

  it("plans hive partition tasks, prunes files, and explains the plan", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "lake/date=2026-01-01/country=US/a.parquet": [{ id: 1, amount: 10 }],
        "lake/date=2026-01-02/country=CA/b.parquet": [{ id: 2, amount: 20 }],
        "lake/date=2026-01-02/country=US/c.parquet": [{ id: 3, amount: 30 }],
      },
    });

    const query = lake
      .hive("lake/**/*.parquet")
      .select(["id"])
      .where(and(eq("country", "US"), gt("amount", 15)));

    expect(await query.toArray()).toEqual([{ id: 3 }]);
    expect(scanner.requestedColumns).toEqual([
      ["amount", "id"],
      ["amount", "id"],
    ]);

    const result = query.run();
    const tasks = await result.planTasks();
    expect(tasks.map((task) => task.path)).toEqual([
      "lake/date=2026-01-01/country=US/a.parquet",
      "lake/date=2026-01-02/country=US/c.parquet",
    ]);
    expect(tasks[0]?.partitionValues).toEqual({ country: "US", date: "2026-01-01" });
    expect(tasks[0]?.projectedColumns).toEqual(["amount", "country", "id"]);
    expect(tasks[0]?.residualPredicate).toMatchObject({ kind: "logical" });

    const explain = await result.explain();
    expect(explain.json).toMatchObject({
      filesPlanned: 2,
      filesSkipped: 1,
      projectedColumns: ["amount", "country", "id"],
      predicatePlan: {
        partition: [{ kind: "compare", op: "eq" }],
        rowGroupStats: [{ kind: "compare", op: "gt" }],
        residual: [],
      },
    });
    expect(explain.text).toContain("files skipped: 1");
  });

  it("uses an explicit planning cache for repeated object expansion", async () => {
    const planningCache = memoryCache<ObjectInfo[]>();
    const { lake, store } = await makeLake({
      planningCache,
      rowsByPath: {
        "data/a.parquet": [{ id: 1 }],
        "data/b.parquet": [{ id: 2 }],
      },
    });
    let listCalls = 0;
    const originalList = store.list.bind(store);
    store.list = async function* (prefix, options) {
      listCalls += 1;
      yield* originalList(prefix, options);
    };

    const query = lake.path("data/*.parquet").select(["id"]);
    const first = await query.taskManifest("job_plan_cache");
    const second = await query.taskManifest("job_plan_cache");

    expect(second).toEqual(first);
    expect(listCalls).toBe(1);

    await store.put("data/c.parquet", new Uint8Array([1, 2, 3]));
    const cached = await query.taskManifest("job_plan_cache_2");
    expect(cached.tasks.map((task) => task.input.path)).toEqual([
      "data/a.parquet",
      "data/b.parquet",
    ]);
  });

  it("uses sidecar indexes to prune planned objects before scanning", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "lake/a.parquet": [{ id: 1, amount: 10 }],
        "lake/b.parquet": [{ id: 2, amount: 20 }],
        "lake/c.parquet": [{ id: 3, amount: 30 }],
      },
      sidecarIndex: [
        { path: "lake/a.parquet", columns: { amount: { min: 0, max: 10 } } },
        { path: "lake/b.parquet", columns: { amount: { min: 20, max: 20 } } },
        { path: "lake/c.parquet", columns: { amount: { min: 30, max: 40 } } },
      ],
    });

    const query = lake.path("lake/*.parquet").select(["id"]).where(gt("amount", 15));

    expect(await query.toArray()).toEqual([{ id: 2 }, { id: 3 }]);
    expect(scanner.requestedPaths).toEqual(["lake/b.parquet", "lake/c.parquet"]);

    const result = query.run();
    expect(await result.explain()).toMatchObject({
      json: {
        filesPlanned: 2,
        filesSkipped: 1,
      },
      text: expect.stringContaining("files skipped: 1"),
    });
  });

  it("parses hive partition segments and preserves non-hive path pieces", () => {
    expect(
      parseHivePartitions("lake/date=2026-01-01/country=United%20States/file.parquet"),
    ).toEqual({
      country: "United States",
      date: "2026-01-01",
    });
    expect(parseHivePartitions("lake/=bad/empty=/plain/file.parquet")).toEqual({});
  });

  it("keeps hive pruning conservative for not/or/unknown partition expressions", async () => {
    const rowsByPath = {
      "lake/country=US/a.parquet": [{ id: 1, amount: 10 }],
      "lake/country=CA/b.parquet": [{ id: 2, amount: 20 }],
    };

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(not(eq("country", "CA")))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=US/a.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(or(eq("country", "CA"), eq("country", "MX")))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(or(eq("country", "CA"), gt("amount", 0)))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet", "lake/country=US/a.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(lit(true))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet", "lake/country=US/a.parquet"]);

    expect(
      (
        await (
          await makeLake({ rowsByPath })
        ).lake
          .hive("lake/**/*.parquet")
          .where(col("country"))
          .planTasks()
      ).map((task) => task.path),
    ).toEqual(["lake/country=CA/b.parquet", "lake/country=US/a.parquet"]);
  });

  it("supports all simple JSON comparison forms", async () => {
    expect(parseJsonQuery({ version: 1, from: "t", where: { ne: ["a", 1] } }).where).toMatchObject({
      kind: "compare",
      op: "ne",
    });
    expect(parseJsonQuery({ version: 1, from: "t", limit: 0, offset: 0 })).toMatchObject({
      limit: 0,
      offset: 0,
    });
    expect(parseJsonQuery({ version: 1, from: "t", distinct: true })).toMatchObject({
      distinct: true,
    });
    expect(parseJsonQuery({ version: 1, from: "t", where: { lte: ["a", 1] } }).where).toMatchObject(
      {
        kind: "compare",
        op: "lte",
      },
    );
    expect(parseJsonQuery({ version: 1, from: "t", where: { gt: ["a", 1] } }).where).toMatchObject({
      kind: "compare",
      op: "gt",
    });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { notIn: ["a", [1]] } }).where,
    ).toMatchObject({ kind: "in", negated: true });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { between: ["a", 1, 2] } }).where,
    ).toMatchObject({ kind: "between" });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { isNotNull: "a" } }).where,
    ).toMatchObject({
      kind: "null-check",
      negated: true,
    });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { like: ["a", "%x%"] } }).where,
    ).toMatchObject({
      kind: "like",
      caseInsensitive: false,
    });
    expect(
      parseJsonQuery({ version: 1, from: "t", where: { ilike: ["a", "%x%"] } }).where,
    ).toMatchObject({
      kind: "like",
      caseInsensitive: true,
    });
  });

  it("throws typed parse and validation errors", async () => {
    expect(() => parseJsonQuery(null)).toThrowError(LakeqlError);
    expect(() => parseJsonQuery({ version: 2, from: "t" })).toThrow(/version must be 1/u);
    expect(() => parseJsonQuery({ version: 1, from: 3 })).toThrow(/from must be a string/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", select: [1] })).toThrow(/select must be/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", limit: -1 })).toThrow(/limit/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", offset: 1.5 })).toThrow(/offset/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", distinct: "yes" })).toThrow(/distinct/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", orderBy: "bad" })).toThrow(/orderBy/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", orderBy: ["bad"] })).toThrow(/terms/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: 1 }] })).toThrow(
      /column/u,
    );
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: "a", direction: "bad" }] }),
    ).toThrow(/direction/u);
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: "a", nulls: "bad" }] }),
    ).toThrow(/nulls/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { nope: 1 } })).toThrow(
      /Unsupported/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { eq: ["a"] } })).toThrow(
      /2-item/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { in: ["a", 1] } })).toThrow(
      /values must/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { like: ["a", 1] } })).toThrow(
      /pattern/u,
    );
    expect(() => parseJsonQuery({ version: 1, from: "t", where: { eq: ["a", {}] } })).toThrow(
      /scalar/u,
    );
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        qualify: {
          kind: "compare",
          op: "bad",
          left: { kind: "column", name: "a" },
          right: { kind: "literal", value: 1 },
        },
      }),
    ).toThrow(/compare expression op/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          constructor: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [] } },
        },
      }),
    ).toThrow(/window aliases/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", windows: [] })).toThrow(/windows/u);
    expect(() => parseJsonQuery({ version: 1, from: "t", windows: { rn: [] } })).toThrow(
      /window expression/u,
    );
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", windows: { rn: { fn: "row_number", args: [] } } }),
    ).toThrow(/over/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: { rn: { fn: "row_number", args: "bad", over: { partitionBy: [], orderBy: [] } } },
      }),
    ).toThrow(/args/u);
    expect(
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: { rn: { fn: "row_number", args: [], over: {} } },
      }).windows?.rn.over,
    ).toEqual({ partitionBy: [], orderBy: [] });
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", qualify: { kind: "literal", value: {} } }),
    ).toThrow(/scalar/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        qualify: { kind: "logical", op: "xor", operands: [] },
      }),
    ).toThrow(/logical expression op/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        qualify: { kind: "logical", op: "and", operands: [] },
      }),
    ).toThrow(/at least two/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: { rn: { fn: "nope", args: [], over: { partitionBy: [], orderBy: [] } } },
      }),
    ).toThrow(/window function/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: { rn: { fn: "row_number", args: [], over: { partitionBy: "bad", orderBy: [] } } },
      }),
    ).toThrow(/partitionBy/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [{ expr: { kind: "column", name: "a" }, direction: "bad" }],
            },
          },
        },
      }),
    ).toThrow(/direction/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            ignoreNulls: "yes",
            over: { partitionBy: [], orderBy: [] },
          },
        },
      }),
    ).toThrow(/ignoreNulls/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            distinct: "yes",
            over: { partitionBy: [], orderBy: [] },
          },
        },
      }),
    ).toThrow(/distinct/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [{ expr: { kind: "column", name: "a" }, nulls: "bad" }],
            },
          },
        },
      }),
    ).toThrow(/nulls/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [],
              frame: {
                mode: "bad",
                start: { kind: "current-row" },
                end: { kind: "current-row" },
                exclude: "no-others",
              },
            },
          },
        },
      }),
    ).toThrow(/frame mode/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [],
              frame: {
                mode: "rows",
                start: { kind: "current-row" },
                end: { kind: "current-row" },
                exclude: "bad",
              },
            },
          },
        },
      }),
    ).toThrow(/exclude/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [],
              frame: {
                mode: "rows",
                start: "bad",
                end: { kind: "current-row" },
                exclude: "no-others",
              },
            },
          },
        },
      }),
    ).toThrow(/frame bound/u);
    expect(() =>
      parseJsonQuery({
        version: 1,
        from: "t",
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: {
              partitionBy: [],
              orderBy: [],
              frame: {
                mode: "rows",
                start: { kind: "bad" },
                end: { kind: "current-row" },
                exclude: "no-others",
              },
            },
          },
        },
      }),
    ).toThrow(/bound kind/u);

    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1 }] } });
    expect(() => lake.path("table").limit(-1).run()).toThrow(/limit/u);
    expect(() => lake.path("table").offset(-1).run()).toThrow(/offset/u);
    expect(() => lake.path("table").batchSize(0).run()).toThrow(/batchSize/u);
    const unsafeProjections = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(unsafeProjections, "__proto__", {
      value: col("id"),
      enumerable: true,
    });
    expect(() =>
      lake
        .path("table")
        .project(unsafeProjections as never)
        .run(),
    ).toThrow(/projection aliases/u);
    expect(() =>
      lake
        .path("table")
        .window("constructor", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [], orderBy: [] },
        })
        .run(),
    ).toThrow(/window aliases/u);
  });

  it("throws typed runtime errors for missing objects, unknown columns, and budgets", async () => {
    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1, value: 3 }] } });

    await expect(lake.path("missing").toArray()).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });
    await expect(lake.path("table").select(["missing"]).toArray()).rejects.toMatchObject({
      code: "LAKEQL_UNKNOWN_COLUMN",
    });
    await expect(lake.path("table").where(gt("missing", 1)).toArray()).rejects.toMatchObject({
      code: "LAKEQL_UNKNOWN_COLUMN",
    });
  });

  it("applies caller query policy for columns, limits, row filters, and context", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, tenant: "a", visible: true, secret: "no" },
          { id: 2, tenant: "b", visible: true, secret: "no" },
          { id: 3, tenant: "a", visible: false, secret: "no" },
        ],
      },
      policy: {
        allowedColumns: ["id", "tenant", "visible"],
        maxLimit: 1,
        context: { tenant: "a" },
        rowFilter: (context) => and(eq("tenant", String(context.tenant)), eq("visible", true)),
      },
    });

    await expect(lake.path("table").toArray()).resolves.toEqual([
      { id: 1, tenant: "a", visible: true },
    ]);
    expect(scanner.requestedColumns[0]).toEqual(["id", "tenant", "visible"]);

    expect(() => lake.path("table").select(["secret"]).toArray()).toThrowError(LakeqlError);
    expect(() => lake.path("table").select(["secret"]).toArray()).toThrow(/disallowed/u);
    expect(() => lake.path("table").where(eq("secret", "x")).toArray()).toThrow(/disallowed/u);
    expect(() =>
      lake
        .path("table")
        .orderBy([{ column: "secret" }])
        .toArray(),
    ).toThrow(/disallowed/u);
  });

  it("uses runtime substrate hooks for query ids, clock, and metrics", async () => {
    const store = memoryStore();
    await store.put("table", new Uint8Array([1, 2, 3]));
    const scanner = new FakeScanner({ table: [{ id: 1 }, { id: 2 }] });
    const counts: Array<{
      name: string;
      value: number | undefined;
      tags: Record<string, string> | undefined;
    }> = [];
    const timings: Array<{ name: string; ms: number; tags: Record<string, string> | undefined }> =
      [];
    let now = 100;
    const substrate: RuntimeSubstrate = {
      clock: { now: () => (now += 5) },
      ids: { id: (prefix = "id") => `${prefix}_substrate` },
      metrics: {
        count: (name, value, tags) => counts.push({ name, value, tags }),
        timing: (name, ms, tags) => timings.push({ name, ms, tags }),
      },
    };
    const lake = new Lake({ store, scanner, substrate });

    const result = lake.path("table").run();

    await expect(result.toArray()).resolves.toEqual([{ id: 1 }, { id: 2 }]);
    expect(result.stats.queryId).toBe("q_substrate");
    expect(result.stats.elapsedMs).toBe(45);
    expect(result.stats.planningMs).toBe(5);
    expect(counts).toEqual([
      { name: "lakeql.query.created", value: 1, tags: { queryId: "q_substrate" } },
    ]);
    expect(timings).toEqual([
      { name: "lakeql.query.elapsed", ms: 45, tags: { queryId: "q_substrate" } },
    ]);
  });

  it("validates malformed query policy", async () => {
    const rowsByPath = { table: [{ id: 1 }] };
    const emptyColumns = await makeLake({ rowsByPath, policy: { allowedColumns: [] } });
    expect(() => emptyColumns.lake.path("table").toArray()).toThrow(/allowedColumns/u);
    const badLimit = await makeLake({ rowsByPath, policy: { maxLimit: -1 } });
    expect(() => badLimit.lake.path("table").toArray()).toThrow(/maxLimit/u);
  });

  it("enforces every observable phase 1 budget", async () => {
    const rowsByPath = { table: [{ id: 1 }, { id: 2 }] };

    await expect(
      (await makeLake({ rowsByPath, budget: { maxFiles: 0 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxBytes: 1 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxRowsDecoded: 1 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: {
        metric: "rows decoded",
        measurements: expect.objectContaining({
          rowsDecoded: 2,
          rowsReturned: 1,
        }),
      },
    });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxOutputRows: 1 } })).lake.path("table").toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      (await makeLake({ rowsByPath, budget: { maxRangeRequests: 0 } })).lake
        .path("table")
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

    let now = 0;
    await expect(
      (
        await makeLake({
          rowsByPath,
          budget: { maxElapsedMs: 0 },
          now: () => {
            now += 1;
            return now;
          },
        })
      ).lake
        .path("table")
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("handles empty JSON streams and no-match predicates", async () => {
    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1, maybe: null }] } });

    expect(
      await lake
        .path("table")
        .where(not(isNull("maybe")))
        .count(),
    ).toBe(0);
    await expect(
      new Response(lake.path("table").where(eq("id", 2)).streamJson()).text(),
    ).resolves.toBe("[]");
  });

  it("slices queries with bookmarks and resumes to the same rows", async () => {
    const { lake, store } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, keep: true },
          { id: 2, keep: false },
          { id: 3, keep: true },
          { id: 4, keep: true },
        ],
      },
    });
    const query = lake.path("table").select(["id"]).where(eq("keep", true));

    const first = await query.run({ slice: { maxRows: 2 } });
    expect(first.rows).toEqual([{ id: 1 }, { id: 3 }]);
    expect(first.bookmark).toMatchObject({
      query: { source: "table", select: ["id"], where: eq("keep", true) },
      position: { rowOffset: 2, fileIndex: 0, rowGroup: 0 },
    });

    const second = await query.run({ slice: { maxRows: 2, bookmark: first.bookmark } });
    expect(second).toEqual({ rows: [{ id: 4 }] });
    if (!first.bookmark) throw new Error("expected bookmark");
    await expect(lake.resume(first.bookmark).run({ slice: { maxRows: 2 } })).resolves.toEqual({
      rows: [{ id: 4 }],
    });
    await store.put("table", new Uint8Array([1, 2, 3, 4]));
    await expect(lake.resume(first.bookmark).run({ slice: { maxRows: 2 } })).rejects.toMatchObject({
      code: "LAKEQL_BOOKMARK_STALE",
    });

    const replayed: Row[] = [];
    for await (const batch of query.resumableBatches({ bookmarkEvery: 1 })) {
      replayed.push(...batch.rows);
    }
    expect(replayed).toEqual(await query.toArray());
  });

  it("applies qualify predicates even when no windows are defined", async () => {
    const { lake } = await makeLake({
      rowsByPath: { table: [{ id: 1 }, { id: 2 }, { id: 3 }] },
    });
    await expect(
      lake
        .path("table")
        .qualify(gt("id", lit(1)))
        .select(["id"])
        .toArray(),
    ).resolves.toEqual([{ id: 2 }, { id: 3 }]);
  });

  it("supports select-star window queries without reading or projecting a literal star column", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, group: "a" },
          { id: 2, group: "a" },
        ],
      },
    });
    const rows = await lake
      .path("table")
      .select(["*"])
      .window("rn", {
        fn: "row_number",
        args: [],
        over: { partitionBy: [col("group")], orderBy: [{ expr: col("id") }] },
      })
      .qualify(gt("rn", lit(0)))
      .toArray();
    expect(rows).toEqual([
      { id: 1, group: "a", rn: 1 },
      { id: 2, group: "a", rn: 2 },
    ]);
    expect(scanner.requestedColumns.some((columns) => columns?.includes("*"))).toBe(false);
  });

  it("preserves window query shape when resuming bookmarks", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, group: "a" },
          { id: 2, group: "a" },
          { id: 3, group: "a" },
        ],
      },
    });
    const query = lake
      .path("table")
      .window("rn", {
        fn: "row_number",
        args: [],
        over: { partitionBy: [col("group")], orderBy: [{ expr: col("id") }] },
      })
      .qualify(gt("rn", lit(1)))
      .select(["id", "rn"]);

    const first = await query.run({ slice: { maxRows: 1 } });
    expect(first.rows).toEqual([{ id: 2, rn: 2 }]);
    expect(first.bookmark?.query).toMatchObject({
      windows: { rn: { fn: "row_number" } },
      qualify: gt("rn", lit(1)),
    });
    if (first.bookmark === undefined) throw new Error("expected bookmark");
    await expect(lake.resume(first.bookmark).run({ slice: { maxRows: 1 } })).resolves.toEqual({
      rows: [{ id: 3, rn: 3 }],
    });
  });

  it("preserves run-to-completion output across deterministic slice boundaries", async () => {
    for (const rowCount of [0, 1, 2, 5, 9, 17]) {
      const rows = Array.from({ length: rowCount }, (_, index) => ({
        id: index,
        keep: index % 3 !== 1,
        bucket: `b${index % 4}`,
      }));
      const { lake } = await makeLake({
        rowsByPath: {
          table: rows,
        },
      });
      const query = lake.path("table").select(["id", "bucket"]).where(eq("keep", true));
      const expected = await query.toArray();

      for (const maxRows of [1, 2, 3, 5]) {
        const replayed: Row[] = [];
        let bookmark: Bookmark | undefined;
        do {
          const result = bookmark
            ? await lake.resume(bookmark).run({ slice: { maxRows } })
            : await query.run({ slice: { maxRows } });
          replayed.push(...result.rows);
          bookmark = result.bookmark;
        } while (bookmark !== undefined);

        expect(replayed, `rowCount=${rowCount} maxRows=${maxRows}`).toEqual(expected);
      }
    }
  });

  it("orders rows before offset, limit, and projection", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "data/b.parquet": [
          { id: 4, score: 10, name: "low" },
          { id: 2, score: 30, name: "same-b" },
        ],
        "data/a.parquet": [
          { id: 3, score: 30, name: "same-a" },
          { id: 1, score: null, name: "missing" },
        ],
      },
    });

    await expect(
      lake
        .path("data/*.parquet")
        .select(["id", "name"])
        .orderBy([
          { column: "score", direction: "desc", nulls: "last" },
          { column: "id", direction: "asc" },
        ])
        .offset(1)
        .limit(2)
        .batchSize(1)
        .toArray(),
    ).resolves.toEqual([
      { id: 3, name: "same-a" },
      { id: 4, name: "low" },
    ]);
    expect(scanner.requestedColumnBatchColumns[0]).toEqual(["id", "score"]);
    expect(scanner.requestedColumnBatchColumns[1]).toEqual(["id", "score"]);
    expect(scanner.requestedColumnBatchColumns).toContainEqual(["name"]);
    await expect(
      lake
        .path("data/*.parquet")
        .orderBy([{ column: "score", direction: "asc" }])
        .limit(2)
        .toArray(),
    ).resolves.toEqual([{ score: 10 }, { score: 30 }]);
    await expect(
      lake
        .path("data/*.parquet")
        .orderBy([{ column: "score", direction: "desc" }])
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ score: null }]);
    await expect(
      lake
        .path("data/*.parquet")
        .orderBy([
          { column: "score", direction: "desc", nulls: "last" },
          { column: "id", direction: "desc" },
        ])
        .limit(1)
        .toArray(),
    ).resolves.toEqual([{ id: 3, score: 30 }]);
    expect(
      (
        await lake
          .path("data/*.parquet")
          .orderBy([{ column: "score" }])
          .planTasks()
      )[0],
    ).toMatchObject({ projectedColumns: ["score"] });
  });

  it("applies top-k over sparse selected vector rows", async () => {
    const rows = Array.from({ length: 100 }, (_, index) => ({
      id: index,
      keep: index % 10 === 0,
      score: 1000 - index,
    }));
    const { lake } = await makeLake({ rowsByPath: { table: rows } });

    await expect(
      lake
        .path("table")
        .select(["id", "score"])
        .where(eq("keep", true))
        .orderBy([{ column: "score", direction: "desc" }])
        .limit(3)
        .toArray(),
    ).resolves.toEqual([
      { id: 0, score: 1000 },
      { id: 10, score: 990 },
      { id: 20, score: 980 },
    ]);
  });

  it("enforces buffered-row budgets for ordered queries", async () => {
    const rowsByPath = { table: [{ id: 3 }, { id: 1 }, { id: 2 }] };

    await expect(
      (await makeLake({ rowsByPath, budget: { maxBufferedRows: 2 } })).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "buffered rows", limit: 2, actual: 3 },
    });

    await expect(
      (await makeLake({ rowsByPath, budget: { maxBufferedRows: 3 } })).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .toArray(),
    ).resolves.toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);

    await expect(
      (
        await makeLake({
          rowsByPath: { table: [{ id: 5 }, { id: 3 }, { id: 1 }, { id: 4 }, { id: 2 }] },
          budget: { maxBufferedRows: 2 },
        })
      ).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(2)
        .toArray(),
    ).resolves.toEqual([{ id: 1 }, { id: 2 }]);

    await expect(
      (
        await makeLake({
          rowsByPath: { table: [{ id: 5 }, { id: 3 }, { id: 1 }, { id: 4 }, { id: 2 }] },
          budget: { maxBufferedRows: 2 },
        })
      ).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .offset(1)
        .limit(2)
        .toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "buffered rows", limit: 2, actual: 3 },
    });

    await expect(
      (
        await makeLake({
          rowsByPath: { table: [{ id: 2 }, { id: 1 }] },
          budget: { maxMemoryBytes: 1 },
        })
      ).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(1)
        .toArray(),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "operator memory bytes", limit: 1 },
    });
  });

  it("parses JSON orderBy and rejects invalid order terms", async () => {
    expect(
      parseJsonQuery({
        version: 1,
        from: "t",
        orderBy: [{ column: "score", direction: "desc", nulls: "first" }],
      }).orderBy,
    ).toEqual([{ column: "score", direction: "desc", nulls: "first" }]);
    expect(() => parseJsonQuery({ version: 1, from: "t", orderBy: {} })).toThrow(/orderBy/u);
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: "a", direction: "sideways" }] }),
    ).toThrow(/direction/u);
    expect(() =>
      parseJsonQuery({ version: 1, from: "t", orderBy: [{ column: "a", nulls: "middle" }] }),
    ).toThrow(/nulls/u);

    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1, value: {} }] } });
    expect(() => lake.path("table").orderBy([]).run()).toThrow(/orderBy/u);
    expect(() =>
      lake
        .path("table")
        .orderBy([{ column: "" }])
        .run(),
    ).toThrow(/columns/u);
    expect(() =>
      lake
        .path("table")
        .orderBy([{ column: "id", direction: "sideways" }])
        .run(),
    ).toThrow(/direction/u);
    await expect(
      lake
        .path("table")
        .orderBy([{ column: "missing" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_UNKNOWN_COLUMN" });
    await expect(
      lake
        .path("table")
        .orderBy([{ column: "value" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });

    const mixed = await makeLake({
      rowsByPath: { table: [{ value: 1 }, { value: "two" }] },
    });
    await expect(
      mixed.lake
        .path("table")
        .orderBy([{ column: "value" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("serializes and resumes top-k operator state", async () => {
    const first = await makeLake({
      rowsByPath: { table: [{ id: 5 }, { id: 1 }, { id: 4 }] },
      budget: { maxBufferedRows: 3 },
    });
    const partial = await first.lake
      .path("table")
      .orderBy([{ column: "id" }])
      .limit(3)
      .topKWithState();
    expect(partial.rows).toEqual([{ id: 1 }, { id: 4 }, { id: 5 }]);

    const snapshot = deserializeTopKOperatorState(partial.operatorState);
    expect(deserializeTopKOperatorState(serializeTopKOperatorState(snapshot))).toEqual(snapshot);
    const spill = memorySpillAdapter();
    const spilled = await first.lake
      .path("table")
      .orderBy([{ column: "id" }])
      .limit(3)
      .topKWithState({ spill, spillId: "topk-state" });
    expect(spilled.operatorSpill).toEqual({
      id: "topk-state",
      byteSize: spilled.operatorState.byteLength,
    });
    await expect(spill.read("topk-state")).resolves.toEqual(spilled.operatorState);

    const second = await makeLake({
      rowsByPath: { table: [{ id: 2 }, { id: 3 }, { id: 0 }] },
      budget: { maxBufferedRows: 3 },
    });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(3)
        .topKWithState({ operatorState: partial.operatorState }),
    ).resolves.toMatchObject({
      rows: [{ id: 0 }, { id: 1 }, { id: 2 }],
    });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(3)
        .topKWithState({ operatorState: { spillRef: "topk-state" }, spill }),
    ).resolves.toMatchObject({
      rows: [{ id: 0 }, { id: 1 }, { id: 2 }],
    });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(3)
        .topKWithState({ operatorState: { spillRef: "topk-state" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id", direction: "desc" }])
        .limit(3)
        .topKWithState({ operatorState: partial.operatorState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(2)
        .topKWithState({ operatorState: partial.operatorState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(3)
        .topKWithState({ operatorState: new TextEncoder().encode("{}") }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    const nested = await makeLake({
      rowsByPath: { table: [{ id: 1, payload: { nested: true } }] },
    });
    await expect(
      nested.lake
        .path("table")
        .select(["id", "payload"])
        .orderBy([{ column: "id" }])
        .limit(1)
        .topKWithState(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("serializes and resumes full sort operator state", async () => {
    const first = await makeLake({
      rowsByPath: { table: [{ id: 5 }, { id: 1 }, { id: 4 }] },
      budget: { maxBufferedRows: 6 },
    });
    const partial = await first.lake
      .path("table")
      .orderBy([{ column: "id" }])
      .sortWithState();
    expect(partial.rows).toEqual([{ id: 1 }, { id: 4 }, { id: 5 }]);

    const snapshot = deserializeSortOperatorState(partial.operatorState);
    expect(deserializeSortOperatorState(serializeSortOperatorState(snapshot))).toEqual(snapshot);
    expect(
      snapshot.runs.map((run) => ("rows" in run ? run.rows.map((row) => row.id) : [])),
    ).toEqual([[1, 4, 5]]);
    const spill = memorySpillAdapter();
    const spilled = await first.lake
      .path("table")
      .orderBy([{ column: "id" }])
      .sortWithState({ spill, spillId: "sort-state" });
    expect(spilled.operatorSpill).toEqual({
      id: "sort-state",
      byteSize: spilled.operatorState.byteLength,
    });
    await expect(spill.read("sort-state")).resolves.toEqual(spilled.operatorState);
    const spilledSnapshot = deserializeSortOperatorState(spilled.operatorState);
    expect(spilledSnapshot.runs).toEqual([
      { spillRef: "sort-state-run-000000", rowCount: 3, byteSize: expect.any(Number) },
    ]);
    await expect(spill.read("sort-state-run-000000")).resolves.toBeInstanceOf(Uint8Array);

    const second = await makeLake({
      rowsByPath: { table: [{ id: 2 }, { id: 3 }, { id: 0 }] },
      budget: { maxBufferedRows: 6 },
    });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .sortWithState({ operatorState: partial.operatorState }),
    ).resolves.toMatchObject({
      rows: [{ id: 0 }, { id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }],
    });
    const chunked = await (
      await makeLake({
        rowsByPath: { table: [{ id: 5 }, { id: 1 }, { id: 4 }, { id: 2 }, { id: 3 }] },
        budget: { maxBufferedRows: 2, maxWindowPartitionRows: 10 },
      })
    ).lake
      .path("table")
      .orderBy([{ column: "id" }])
      .sortWithState();
    expect(chunked.rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]);
    expect(
      deserializeSortOperatorState(chunked.operatorState).runs.map((run) =>
        "rows" in run ? run.rows.length : run.rowCount,
      ),
    ).toEqual([2, 2, 1]);
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .offset(2)
        .limit(2)
        .sortWithState({ operatorState: { spillRef: "sort-state" }, spill }),
    ).resolves.toMatchObject({
      rows: [{ id: 2 }, { id: 3 }],
    });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .sortWithState({ operatorState: spilled.operatorState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id", direction: "desc" }])
        .sortWithState({ operatorState: partial.operatorState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
    await expect(
      second.lake
        .path("table")
        .orderBy([{ column: "id" }])
        .sortWithState({ operatorState: new TextEncoder().encode("{}") }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
  });

  it("serializes and resumes window operator state", async () => {
    const windows = {
      rn: {
        fn: "row_number",
        args: [],
        over: {
          partitionBy: [col("region")],
          orderBy: [{ expr: col("id") }],
        },
      },
      running: {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: {
          partitionBy: [col("region")],
          orderBy: [{ expr: col("id") }],
        },
      },
    } as const;
    const first = await makeLake({
      rowsByPath: {
        table: [
          { id: 2, region: "west", amount: 20 },
          { id: 1, region: "east", amount: 10 },
        ],
      },
      budget: { maxBufferedRows: 4 },
    });
    const partial = await first.lake
      .path("table")
      .window("rn", windows.rn)
      .window("running", windows.running)
      .select(["id", "region", "rn", "running"])
      .orderBy([{ column: "id" }])
      .windowWithState();
    expect(partial.rows).toEqual([
      { id: 1, region: "east", rn: 1, running: 10 },
      { id: 2, region: "west", rn: 1, running: 20 },
    ]);

    const snapshot = deserializeWindowOperatorState(partial.operatorState);
    expect(deserializeWindowOperatorState(serializeWindowOperatorState(snapshot))).toEqual(
      snapshot,
    );
    expect(snapshot.rows).toEqual([
      { amount: 20, id: 2, region: "west" },
      { amount: 10, id: 1, region: "east" },
    ]);

    const spill = memorySpillAdapter();
    const spilled = await first.lake
      .path("table")
      .window("rn", windows.rn)
      .window("running", windows.running)
      .windowWithState({ spill, spillId: "window-state" });
    expect(spilled.operatorSpill).toEqual({
      id: "window-state",
      byteSize: spilled.operatorState.byteLength,
    });
    await expect(spill.read("window-state")).resolves.toEqual(spilled.operatorState);
    const spilledSnapshot = deserializeWindowOperatorState(spilled.operatorState);
    expect(spilledSnapshot.runs).toEqual([
      { spillRef: "window-state-run-000000", rowCount: 2, byteSize: expect.any(Number) },
    ]);
    await expect(spill.read("window-state-run-000000")).resolves.toBeInstanceOf(Uint8Array);

    const chunkedSpill = memorySpillAdapter();
    const chunked = await (
      await makeLake({
        rowsByPath: {
          table: [
            { id: 1, region: "west", amount: 10 },
            { id: 2, region: "west", amount: 20 },
            { id: 3, region: "west", amount: 30 },
            { id: 4, region: "west", amount: 40 },
            { id: 5, region: "west", amount: 50 },
          ],
        },
        budget: { maxBufferedRows: 2, maxWindowPartitionRows: 10 },
      })
    ).lake
      .path("table")
      .window("running", windows.running)
      .select(["id", "running"])
      .orderBy([{ column: "id" }])
      .windowWithState({ spill: chunkedSpill, spillId: "window-chunked" });
    expect(chunked.rows).toEqual([
      { id: 1, running: 10 },
      { id: 2, running: 30 },
      { id: 3, running: 60 },
      { id: 4, running: 100 },
      { id: 5, running: 150 },
    ]);
    expect(
      deserializeWindowOperatorState(chunked.operatorState).runs?.map((run) =>
        "rows" in run ? run.rows.length : run.rowCount,
      ),
    ).toEqual([2, 2, 1]);

    const partitionSpill = await (
      await makeLake({
        rowsByPath: {
          table: [
            { id: 1, region: "a", amount: 10 },
            { id: 2, region: "a", amount: 20 },
            { id: 3, region: "b", amount: 30 },
            { id: 4, region: "b", amount: 40 },
            { id: 5, region: "c", amount: 50 },
            { id: 6, region: "c", amount: 60 },
          ],
        },
        budget: { maxBufferedRows: 2, maxWindowPartitionRows: 2, maxMemoryBytes: 170 },
      })
    ).lake
      .path("table")
      .window("running", windows.running)
      .select(["id", "running"])
      .orderBy([{ column: "id" }])
      .windowWithState({ spill: memorySpillAdapter(), spillId: "window-partitions" });
    expect(partitionSpill.rows).toEqual([
      { id: 1, running: 10 },
      { id: 2, running: 30 },
      { id: 3, running: 30 },
      { id: 4, running: 70 },
      { id: 5, running: 50 },
      { id: 6, running: 110 },
    ]);

    const second = await makeLake({
      rowsByPath: {
        table: [
          { id: 3, region: "west", amount: 30 },
          { id: 4, region: "east", amount: 40 },
        ],
      },
      budget: { maxBufferedRows: 8 },
    });
    await expect(
      second.lake
        .path("table")
        .window("rn", windows.rn)
        .window("running", windows.running)
        .select(["id", "region", "rn", "running"])
        .orderBy([{ column: "id" }])
        .windowWithState({ operatorState: partial.operatorState }),
    ).resolves.toMatchObject({
      rows: [
        { id: 1, region: "east", rn: 1, running: 10 },
        { id: 2, region: "west", rn: 1, running: 20 },
        { id: 3, region: "west", rn: 2, running: 50 },
        { id: 4, region: "east", rn: 2, running: 50 },
      ],
    });
    await expect(
      second.lake
        .path("table")
        .window("rn", windows.rn)
        .window("running", windows.running)
        .select(["id", "region", "rn", "running"])
        .orderBy([{ column: "id" }])
        .windowWithState({ operatorState: { spillRef: "window-state" }, spill }),
    ).resolves.toMatchObject({
      rows: [
        { id: 1, region: "east", rn: 1, running: 10 },
        { id: 2, region: "west", rn: 1, running: 20 },
        { id: 3, region: "west", rn: 2, running: 50 },
        { id: 4, region: "east", rn: 2, running: 50 },
      ],
    });
    await expect(
      second.lake
        .path("table")
        .window("rn", windows.rn)
        .window("running", windows.running)
        .windowWithState({ operatorState: { spillRef: "window-state" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
    await expect(
      second.lake
        .path("table")
        .window("rn", {
          ...windows.rn,
          over: { ...windows.rn.over, orderBy: [{ expr: col("id"), direction: "desc" }] },
        })
        .window("running", windows.running)
        .windowWithState({ operatorState: partial.operatorState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
    await expect(
      second.lake
        .path("table")
        .window("rn", windows.rn)
        .window("running", windows.running)
        .windowWithState({ operatorState: new TextEncoder().encode("{}") }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    await expect(
      (await makeLake({ rowsByPath: { table: [] } })).lake.path("table").windowWithState(),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
  });

  it("uses vector batch scans for window queries when the scanner exposes vector batches", async () => {
    const { lake, scanner } = await makeLake({
      vectorBatches: true,
      rowsByPath: {
        table: [
          { id: 3, region: "west", amount: 30, keep: true },
          { id: 1, region: "east", amount: 10, keep: true },
          { id: 2, region: "west", amount: 20, keep: false },
          { id: 4, region: "west", amount: 40, keep: true },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .where(eq("keep", true))
        .window("rn", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        })
        .window("running", {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        })
        .select(["id", "region", "rn", "running"])
        .orderBy([{ column: "id" }])
        .toArray(),
    ).resolves.toEqual([
      { id: 1, region: "east", rn: 1, running: 10 },
      { id: 3, region: "west", rn: 1, running: 30 },
      { id: 4, region: "west", rn: 2, running: 70 },
    ]);

    expect(scanner.requestedColumns).toEqual([]);
    expect(scanner.requestedVectorBatchColumns).toEqual([["amount", "id", "keep", "region"]]);
  });

  it("explains window operator shape and shared sort groups", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, region: "east", amount: 10 },
          { id: 2, region: "east", amount: 20 },
        ],
      },
    });
    const explain = await lake
      .path("table")
      .window("rn", {
        fn: "row_number",
        args: [],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .qualify(gt("rn", 0))
      .explain();

    expect(explain.json.windowPlan).toEqual({
      expressions: 2,
      sortGroups: 1,
      qualify: true,
      execution: "vector-window",
      workUnits: {
        topology: "window-partition-fanout",
        available: true,
        bucketCount: 2,
        partitionBy: [col("region")],
      },
    });
    expect(explain.text).toContain("window sort groups: 1");
    expect(explain.text).toContain("window qualify: yes");
  });

  it("explains prefix-compatible windows as one physical sort group", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { id: 1, region: "east", bucket: 1, amount: 10 },
          { id: 2, region: "east", bucket: 2, amount: 20 },
        ],
      },
    });
    const explain = await lake
      .path("table")
      .window("by_region_bucket", {
        fn: "row_number",
        args: [],
        over: {
          partitionBy: [col("region")],
          orderBy: [{ expr: col("bucket") }, { expr: col("id") }],
        },
      })
      .window("by_region", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: {
          partitionBy: [col("region")],
          orderBy: [{ expr: col("bucket") }],
        },
      })
      .explain();

    expect(explain.json.windowPlan?.sortGroups).toBe(1);
    expect(explain.text).toContain("window sort groups: 1");
  });

  it("plans window work-unit fan-out and explains unavailable topologies", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        "data/a.parquet": [{ id: 1, region: "east", amount: 10 }],
        "data/b.parquet": [{ id: 2, region: "west", amount: 20 }],
      },
    });
    const partitioned = lake
      .path("data/*.parquet")
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .select(["id", "running"]);
    const tasks = await partitioned.planTasks();
    expect(tasks).toHaveLength(2);
    expect(tasks[0]?.projectedColumns).toEqual(["amount", "id", "region"]);
    expect(tasks[0]?.window).toEqual({
      topology: "window-partition-fanout",
      available: true,
      bucketCount: 4,
      partitionBy: [col("region")],
    });
    await expect(partitioned.explain()).resolves.toMatchObject({
      json: {
        windowPlan: {
          workUnits: {
            topology: "window-partition-fanout",
            available: true,
            bucketCount: 4,
          },
        },
      },
    });

    const global = await lake
      .path("data/*.parquet")
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
      })
      .explain();
    expect(global.json.windowPlan?.workUnits).toEqual({
      topology: "window-partition-fanout",
      available: false,
      bucketCount: 1,
      reason: "window query has one global semantic partition",
    });
    expect(global.json.tasks[0]?.window).toEqual(global.json.windowPlan?.workUnits);
  });

  it("executes available window work-unit fan-out through the scan adapter", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        "data/a.parquet": [{ id: 1, region: "east", amount: 10 }],
        "data/b.parquet": [{ id: 2, region: "east", amount: 20 }],
        "data/c.parquet": [{ id: 3, region: "east", amount: 30 }],
        "data/d.parquet": [{ id: 4, region: "east", amount: 40 }],
      },
      windowTaskRows: [
        { id: 1, region: "east", amount: 10, rn: 1, running: 10 },
        { id: 2, region: "east", amount: 20, rn: 2, running: 30 },
        { id: 2, region: "east", amount: 20, rn: 2, running: 30 },
        { id: 3, region: "east", amount: 30, rn: 3, running: 60 },
        { id: 4, region: "east", amount: 40, rn: 4, running: 100 },
      ],
    });
    const query = lake
      .path("data/*.parquet")
      .window("rn", {
        fn: "row_number",
        args: [],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .qualify(gt("rn", 1))
      .select(["running"])
      .distinct()
      .offset(1)
      .limit(1)
      .batchSize(1)
      .orderBy([{ column: "id" }]);

    await expect(query.explain()).resolves.toMatchObject({
      json: { windowPlan: { execution: "parquet-work-unit-fanout" } },
    });
    await expect(query.toArray()).resolves.toEqual([{ running: 60 }]);
    expect(scanner.requestedWindowTasks).toHaveLength(1);
    expect(scanner.requestedWindowTasks[0]).toHaveLength(4);
    expect(scanner.requestedWindowTasks[0]?.[0]?.window).toMatchObject({
      topology: "window-partition-fanout",
      available: true,
      bucketCount: 8,
    });
    expect(scanner.requestedColumns).toEqual([]);
    expect(scanner.requestedVectorBatchColumns).toEqual([]);
  });

  it("explains vector-window execution when vector batches are available", async () => {
    const { lake } = await makeLake({
      vectorBatches: true,
      rowsByPath: {
        table: [
          { id: 1, region: "east", amount: 10 },
          { id: 2, region: "east", amount: 20 },
        ],
      },
    });
    const explain = await lake
      .path("table")
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .select(["id", "running"])
      .explain();

    expect(explain.json.projectedColumns).toEqual(["amount", "id", "region"]);
    expect(explain.json.windowPlan?.execution).toBe("vector-window");
    expect(explain.text).toContain("window execution: vector-window");
  });

  it("uses row-materialized window spill runs when no vector batch scanner is available", async () => {
    const store = memoryStore();
    await store.put("table", new Uint8Array([1, 2, 3]));
    const scanner: ScanAdapter = {
      async *scan(path, options) {
        expect(path).toBe("table");
        const rows = [
          { id: 2, region: "west", amount: 20 },
          { id: 1, region: "west", amount: 10 },
        ];
        for (const row of rows) {
          options.stats.rowsDecoded += 1;
          yield [row];
        }
      },
    };
    const lake = new Lake({
      store,
      scanner,
      budget: { maxBufferedRows: 1, maxWindowPartitionRows: 10 },
      queryId: () => "q_row_window",
    });
    const query = lake
      .path("table")
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .select(["id", "running"])
      .orderBy([{ column: "id" }]);
    await expect(query.explain()).resolves.toMatchObject({
      json: { windowPlan: { execution: "row-materialized" } },
    });

    const result = await query.windowWithState({
      spill: memorySpillAdapter(),
      spillId: "row-window",
    });
    expect(result.rows).toEqual([
      { id: 1, running: 10 },
      { id: 2, running: 30 },
    ]);
    expect(
      deserializeWindowOperatorState(result.operatorState).runs?.map((run) =>
        "rows" in run ? run.rows.length : run.rowCount,
      ),
    ).toEqual([1, 1]);
  });

  it("propagates typed spill budget failures through stateful operators", async () => {
    const rowsByPath = {
      table: [
        { id: 3, region: "west" },
        { id: 1, region: "east" },
        { id: 2, region: "west" },
      ],
    };

    await expect(
      (await makeLake({ rowsByPath })).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(2)
        .topKWithState({ spill: memorySpillAdapter({ maxBytes: 1 }) }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "spill bytes", limit: 1 },
    });

    await expect(
      (await makeLake({ rowsByPath, budget: { maxBufferedRows: 2 } })).lake
        .path("table")
        .orderBy([{ column: "id" }])
        .sortWithState({ spill: memorySpillAdapter({ maxBytes: 1 }) }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "spill bytes", limit: 1 },
    });

    await expect(
      (await makeLake({ rowsByPath })).lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(
          { rows: { op: "count" } },
          { spill: memorySpillAdapter({ maxBytes: 1 }) },
        ),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "spill bytes", limit: 1 },
    });

    await expect(
      (await makeLake({ rowsByPath })).lake
        .path("table")
        .window("rn", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        })
        .windowWithState({ spill: memorySpillAdapter({ maxBytes: 1 }) }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "spill bytes", limit: 1 },
    });
  });

  it("enforces buffered-row budgets for retained aggregate state", async () => {
    const rowsByPath = {
      table: [
        { id: 1, region: "west", amount: 10 },
        { id: 2, region: "east", amount: 20 },
      ],
    };

    for (const spec of [
      { amountMedian: { op: "median", column: "amount" } },
      { amountP50: { op: "quantile", column: "amount", quantile: 0.5 } },
      { regionMode: { op: "mode", column: "region" } },
      { ids: { op: "count_distinct", column: "id" } },
    ] satisfies AggregateSpec[]) {
      await expect(
        (await makeLake({ rowsByPath, budget: { maxBufferedRows: 1 } })).lake
          .path("table")
          .groupBy([])
          .aggregate(spec),
      ).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { metric: "buffered rows", limit: 1 },
      });
    }
  });

  it("accounts aggregate state with bigint group keys", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { distance: 3000n, delay: 10 },
          { distance: 3000n, delay: 20 },
          { distance: 3200n, delay: 5 },
        ],
      },
      budget: { maxBufferedRows: 10 },
    });

    await expect(
      lake
        .path("table")
        .groupBy(["distance"])
        .aggregate({
          flights: { op: "count" },
          avgDelay: { op: "avg", column: "delay" },
        }),
    ).resolves.toEqual([
      { distance: 3000n, flights: 2, avgDelay: 15 },
      { distance: 3200n, flights: 1, avgDelay: 5 },
    ]);
  });

  it("rejects stale or invalid slice bookmarks", async () => {
    const { lake } = await makeLake({ rowsByPath: { table: [{ id: 1 }] } });
    const query = lake.path("table");

    await expect(query.run({ slice: { maxRows: 0 } })).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await expect(
      query.run({
        slice: {
          maxRows: 1,
          bookmark: createBookmark({
            planFingerprint: "fp_stale",
            snapshot: "snapshot",
            position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
          }),
        },
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
    expect(() =>
      lake
        .resume(
          createBookmark({
            planFingerprint: "fp_without_query",
            snapshot: "snapshot",
            position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
          }),
        )
        .run({ slice: { maxRows: 1 } }),
    ).toThrowError(LakeqlError);
  });

  it("aggregates grouped rows with bounded group counts", async () => {
    const rowsByPath = {
      table: [
        { region: "west", amount: 10, id: 1, label: "a" },
        { region: "west", amount: 20, id: 2, label: "b" },
        { region: "east", amount: 7, id: 2, label: "c" },
      ],
    };
    const { lake } = await makeLake({
      rowsByPath,
    });

    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate(
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
            average: { op: "avg", column: "amount" },
            minId: { op: "min", column: "id" },
            maxId: { op: "max", column: "id" },
            distinctIds: { op: "count_distinct", column: "id" },
            approximateIds: { op: "approx_count_distinct", column: "id" },
            firstLabel: { op: "first", column: "label" },
            lastLabel: { op: "last", column: "label" },
            anyLabel: { op: "any", column: "label" },
          },
          { maxGroups: 2 },
        ),
    ).resolves.toEqual([
      {
        region: "west",
        rows: 2,
        total: 30,
        average: 15,
        minId: 1,
        maxId: 2,
        distinctIds: 2,
        approximateIds: 2,
        firstLabel: "a",
        lastLabel: "b",
        anyLabel: "a",
      },
      {
        region: "east",
        rows: 1,
        total: 7,
        average: 7,
        minId: 2,
        maxId: 2,
        distinctIds: 1,
        approximateIds: 1,
        firstLabel: "c",
        lastLabel: "c",
        anyLabel: "c",
      },
    ]);

    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ rows: { op: "count" } }, { maxGroups: 1 }),
    ).rejects.toMatchObject({ code: "LAKEQL_GROUP_LIMIT_EXCEEDED" });

    await expect(
      (await makeLake({ rowsByPath, budget: { maxMemoryBytes: 1 } })).lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "operator memory bytes", limit: 1 },
    });

    await expect(
      (
        await makeLake({
          rowsByPath: { table: [{ region: "west" }, { region: "west" }] },
          budget: { maxOutputRows: 1 },
        })
      ).lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ rows: { op: "count" } }),
    ).resolves.toEqual([{ region: "west", rows: 2 }]);

    await expect(
      (
        await makeLake({
          rowsByPath: { table: [{ region: "west" }, { region: "east" }] },
          budget: { maxOutputRows: 1 },
        })
      ).lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { metric: "output rows", limit: 1, actual: 2 },
    });
  });

  it("late materializes sparse grouped aggregates without skipping aggregate forms", async () => {
    const rows = Array.from({ length: 100 }, (_, index) => ({
      keep: index % 20 === 0,
      region: index % 40 === 0 ? "west" : "east",
      amount: index + 10,
      bonus: 1,
      id: index % 30,
      label: index % 40 === 0 ? (index === 40 ? "c" : "a") : "b",
    }));
    const { lake, scanner } = await makeLake({ rowsByPath: { table: rows } });

    await expect(
      lake
        .path("table")
        .where(eq("keep", true))
        .groupBy(["region"])
        .aggregate(
          {
            rows: { op: "count" },
            totalExpr: { op: "sum", expr: add(col("amount"), col("bonus")) },
            average: { op: "avg", column: "amount" },
            medianAmount: { op: "median", column: "amount" },
            p50Amount: { op: "quantile", column: "amount", quantile: 0.5 },
            modeLabel: { op: "mode", column: "label" },
            distinctIds: { op: "count_distinct", column: "id" },
            approximateIds: { op: "approx_count_distinct", column: "id" },
            firstLabel: { op: "first", column: "label" },
            lastLabel: { op: "last", column: "label" },
          },
          { orderBy: [{ column: "region" }] },
        ),
    ).resolves.toEqual([
      {
        region: "east",
        rows: 2,
        totalExpr: 102,
        average: 50,
        medianAmount: 50,
        p50Amount: 50,
        modeLabel: "b",
        distinctIds: 2,
        approximateIds: 2,
        firstLabel: "b",
        lastLabel: "b",
      },
      {
        region: "west",
        rows: 3,
        totalExpr: 153,
        average: 50,
        medianAmount: 50,
        p50Amount: 50,
        modeLabel: "a",
        distinctIds: 3,
        approximateIds: 3,
        firstLabel: "a",
        lastLabel: "a",
      },
    ]);
    expect(scanner.requestedColumnBatchColumns[0]).toEqual(["keep", "region"]);
    expect(scanner.requestedColumnBatchColumns).toContainEqual(["amount", "bonus", "id", "label"]);
    expect(
      scanner.requestedColumnBatchWindows.some((window) => window.rowStart !== undefined),
    ).toBe(true);
  });

  it("aggregates expressions and counts only non-null column values", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", amount: 10, id: 1 },
          { region: "west", amount: null, id: 2 },
          { region: "west", amount: 20, id: 1 },
          { region: "east", amount: 7, id: 3 },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate({
          rows: { op: "count" },
          amountRows: { op: "count", column: "amount" },
          doubledTotal: {
            op: "sum",
            expr: {
              kind: "arithmetic",
              op: "mul",
              left: { kind: "column", name: "amount" },
              right: { kind: "literal", value: 2 },
            },
          },
          nonZeroIdBuckets: {
            op: "count_distinct",
            expr: {
              kind: "case",
              whens: [
                {
                  when: {
                    kind: "compare",
                    op: "gt",
                    left: { kind: "column", name: "amount" },
                    right: { kind: "literal", value: 0 },
                  },
                  value: { kind: "column", name: "id" },
                },
              ],
            },
          },
        }),
    ).resolves.toEqual([
      { region: "west", rows: 3, amountRows: 2, doubledTotal: 60, nonZeroIdBuckets: 1 },
      { region: "east", rows: 1, amountRows: 1, doubledTotal: 14, nonZeroIdBuckets: 1 },
    ]);
  });

  it("aggregates broad expression families through vector and scalar execution paths", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", amount: 10, id: 1, label: "alpha", fallback: null },
          { region: "west", amount: null, id: 2, label: "beta", fallback: 5 },
          { region: "east", amount: 30, id: 3, label: "alpine", fallback: 7 },
          { region: "east", amount: 40, id: 4, label: "bravo", fallback: null },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .where(or(isNull("amount"), gt("amount", 15)))
        .groupBy(["region"])
        .aggregate(
          {
            matchedRows: { op: "count" },
            inRows: { op: "sum", expr: fn("cast", isIn("label", ["alpha", "alpine"]), "number") },
            betweenRows: { op: "sum", expr: fn("cast", between("id", 2, 4), "number") },
            nullRows: { op: "sum", expr: fn("cast", isNull("fallback"), "number") },
            logicalRows: {
              op: "sum",
              expr: fn("cast", and(not(isNull("amount")), gt("amount", 20)), "number"),
            },
            normalizedLabels: { op: "count_distinct", expr: fn("lower", col("label")) },
            adjustedTotal: { op: "sum", expr: add(fn("coalesce", col("amount"), 0), col("id")) },
            caseTotal: {
              op: "sum",
              expr: {
                kind: "case",
                whens: [{ when: like("label", "a%"), value: col("id") }],
                else: lit(0),
              },
            },
            likeRows: { op: "sum", expr: fn("cast", like("label", "a%"), "number") },
          },
          { orderBy: [{ column: "region" }] },
        ),
    ).resolves.toEqual([
      {
        region: "east",
        matchedRows: 2,
        inRows: 1,
        betweenRows: 2,
        nullRows: 1,
        logicalRows: 2,
        normalizedLabels: 2,
        adjustedTotal: 77,
        caseTotal: 3,
        likeRows: 1,
      },
      {
        region: "west",
        matchedRows: 1,
        inRows: 0,
        betweenRows: 1,
        nullRows: 0,
        logicalRows: 0,
        normalizedLabels: 1,
        adjustedTotal: 2,
        caseTotal: 0,
        likeRows: 0,
      },
    ]);
    expect(scanner.requestedColumns).toContainEqual([
      "amount",
      "fallback",
      "id",
      "label",
      "region",
    ]);
  });

  it("applies having, ordering, offset, and limit inside aggregate execution", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", amount: 10 },
          { region: "west", amount: 20 },
          { region: "east", amount: 7 },
          { region: "north", amount: 12 },
          { region: "north", amount: 8 },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate(
          {
            rows: { op: "count" },
            total: { op: "sum", column: "amount" },
          },
          {
            having: gt("total", 10),
            orderBy: [{ column: "total", direction: "desc" }],
            offset: 1,
            limit: 1,
          },
        ),
    ).resolves.toEqual([{ region: "north", rows: 2, total: 20 }]);
  });

  it("projects predicate, group, and aggregate columns for grouped aggregates", async () => {
    const { lake, scanner } = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", amount: 10, keep: true, ignored: "x" },
          { region: "west", amount: 20, keep: false, ignored: "x" },
          { region: "east", amount: 7, keep: true, ignored: "x" },
        ],
      },
    });

    await expect(
      lake
        .path("table")
        .where(eq("keep", true))
        .groupBy(["region"])
        .aggregate({ total: { op: "sum", column: "amount" } }),
    ).resolves.toEqual([
      { region: "west", total: 10 },
      { region: "east", total: 7 },
    ]);
    expect(scanner.requestedColumns[0]).toEqual(["amount", "keep", "region"]);
  });

  it("serializes and resumes aggregate operator state", async () => {
    const first = await makeLake({
      rowsByPath: {
        table: [
          { region: "west", amount: 10, id: 1, label: "a" },
          { region: "west", amount: 20, id: 2, label: "b" },
        ],
      },
    });
    const spec = {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
      average: { op: "avg", column: "amount" },
      sampleVariance: { op: "var_samp", column: "amount" },
      sampleStddev: { op: "stddev_samp", column: "amount" },
      popVariance: { op: "var_pop", column: "amount" },
      popStddev: { op: "stddev_pop", column: "amount" },
      medianAmount: { op: "median", column: "amount" },
      p75Amount: { op: "quantile", column: "amount", quantile: 0.75 },
      minId: { op: "min", column: "id" },
      maxId: { op: "max", column: "id" },
      distinctIds: { op: "count_distinct", column: "id" },
      approximateIds: { op: "approx_count_distinct", column: "id" },
      modeLabel: { op: "mode", column: "label" },
      firstLabel: { op: "first", column: "label" },
      lastLabel: { op: "last", column: "label" },
      anyLabel: { op: "any", column: "label" },
    } satisfies AggregateSpec;

    const partial = await first.lake.path("table").groupBy(["region"]).aggregateWithState(spec);
    const snapshot = deserializeAggregateOperatorState(partial.operatorState);
    expect(deserializeAggregateOperatorState(serializeAggregateOperatorState(snapshot))).toEqual(
      snapshot,
    );
    const spill = memorySpillAdapter();
    const spilled = await first.lake
      .path("table")
      .groupBy(["region"])
      .aggregateWithState(spec, { spill, spillId: "agg-state" });
    expect(spilled.operatorSpill).toEqual({
      id: "agg-state",
      byteSize: spilled.operatorState.byteLength,
    });
    await expect(spill.read("agg-state")).resolves.toEqual(spilled.operatorState);

    const second = await makeLake({
      rowsByPath: {
        table: [
          { region: "east", amount: 7, id: 2, label: "c" },
          { region: "west", amount: 5, id: 1, label: "d" },
        ],
      },
    });
    await expect(
      second.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(spec, { operatorState: partial.operatorState }),
    ).resolves.toMatchObject({
      rows: [
        {
          region: "west",
          rows: 3,
          total: 35,
          average: 35 / 3,
          sampleVariance: 58.33333333333333,
          sampleStddev: Math.sqrt(175 / 3),
          popVariance: 350 / 9,
          popStddev: Math.sqrt(350 / 9),
          medianAmount: 10,
          p75Amount: 15,
          minId: 1,
          maxId: 2,
          distinctIds: 2,
          approximateIds: 2,
          modeLabel: "a",
          firstLabel: "a",
          lastLabel: "d",
          anyLabel: "a",
        },
        {
          region: "east",
          rows: 1,
          total: 7,
          average: 7,
          sampleVariance: null,
          sampleStddev: null,
          popVariance: 0,
          popStddev: 0,
          medianAmount: 7,
          p75Amount: 7,
          minId: 2,
          maxId: 2,
          distinctIds: 1,
          approximateIds: 1,
          modeLabel: "c",
          firstLabel: "c",
          lastLabel: "c",
          anyLabel: "c",
        },
      ],
    });
    await expect(
      second.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(spec, { operatorState: { spillRef: "agg-state" }, spill }),
    ).resolves.toMatchObject({
      rows: [
        {
          region: "west",
          rows: 3,
          total: 35,
        },
        {
          region: "east",
          rows: 1,
          total: 7,
        },
      ],
    });
    await expect(
      second.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(spec, { operatorState: { spillRef: "agg-state" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    await expect(
      second.lake
        .path("table")
        .groupBy(["tenant"])
        .aggregateWithState(spec, { operatorState: partial.operatorState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_STALE" });
    await expect(
      second.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(spec, { operatorState: new TextEncoder().encode("{}") }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    const missingState = deserializeAggregateOperatorState(partial.operatorState);
    delete missingState.groups[0]?.states.rows;
    await expect(
      second.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(spec, { operatorState: missingState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    const mismatchedState = deserializeAggregateOperatorState(partial.operatorState);
    const rowsState = mismatchedState.groups[0]?.states.rows;
    if (rowsState) rowsState.op = "sum";
    await expect(
      second.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState(spec, { operatorState: mismatchedState }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });

    expect(() =>
      deserializeAggregateOperatorState({ version: 1, groupColumns: [], spec: {}, groups: [{}] }),
    ).toThrow(/invalid/u);
    const objectValue = await makeLake({
      rowsByPath: { table: [{ region: "west", payload: { nested: true } }] },
    });
    await expect(
      objectValue.lake
        .path("table")
        .groupBy(["region"])
        .aggregateWithState({ firstPayload: { op: "first", column: "payload" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("validates serialized operator state contracts for every aggregate shape", () => {
    const aggregateStates = [
      { op: "count", count: 1 },
      { op: "sum", sum: 1 },
      { op: "avg", sum: 3, count: 2 },
      { op: "var_samp", count: 2, mean: 1.5, m2: 0.5 },
      { op: "var_pop", count: 2, mean: 1.5, m2: 0.5 },
      { op: "stddev_samp", count: 2, mean: 1.5, m2: 0.5 },
      { op: "stddev_pop", count: 2, mean: 1.5, m2: 0.5 },
      { op: "median", values: [1, "two"] },
      { op: "quantile", quantile: 0.5, values: [1, 2] },
      { op: "min", value: 1 },
      { op: "max", value: "z" },
      { op: "count_distinct", values: ["1", "2"] },
      { op: "approx_count_distinct", values: ["1", "2"] },
      { op: "mode", values: [{ key: "n:1", value: 1, count: 2 }] },
      { op: "first", value: "a", seen: true },
      { op: "last", value: null, seen: false },
      { op: "any", value: true, seen: true },
    ];
    const spec = Object.fromEntries(
      aggregateStates.map((state, index) => {
        const alias = `a${index}`;
        switch (state.op) {
          case "quantile":
            return [alias, { op: state.op, column: "value", quantile: state.quantile }];
          default:
            return [alias, { op: state.op, column: "value" }];
        }
      }),
    ) as AggregateSpec;
    const states = Object.fromEntries(aggregateStates.map((state, index) => [`a${index}`, state]));
    const state = {
      version: 1,
      groupColumns: ["region"],
      spec,
      groups: [{ key: "region\u001fwest", keys: { region: "west" }, states }],
    };

    expect(deserializeAggregateOperatorState(state)).toEqual(state);

    const invalidAggregateStates = [
      { op: "count", count: "1" },
      { op: "sum", sum: "1" },
      { op: "avg", sum: 1, count: "2" },
      { op: "var_samp", count: 2, mean: 1, m2: "bad" },
      { op: "median", values: [true] },
      { op: "median", values: "1" },
      { op: "quantile", quantile: 1.5, values: [1] },
      { op: "quantile", quantile: 0.5, values: [null] },
      { op: "min", value: { nested: true } },
      { op: "count_distinct", values: [1] },
      { op: "approx_count_distinct", values: [1] },
      { op: "mode", values: [{ key: "x", value: 1, count: -1 }] },
      { op: "mode", values: [{ key: "x", value: { nested: true }, count: 1 }] },
      { op: "first", value: "a", seen: "yes" },
      { op: "last", value: { nested: true }, seen: true },
      { op: "unknown" },
    ];
    for (const invalidState of invalidAggregateStates) {
      expect(() =>
        deserializeAggregateOperatorState({
          ...state,
          groups: [
            { key: "region\u001fwest", keys: { region: "west" }, states: { a0: invalidState } },
          ],
        }),
      ).toThrowError(LakeqlError);
    }

    const invalidTopKStates = [
      null,
      { version: 2 },
      { version: 1, orderBy: [], offset: 0, limit: 1, rows: [] },
      { version: 1, orderBy: [{ column: "" }], offset: 0, limit: 1, rows: [] },
      { version: 1, orderBy: [{ column: "id" }], offset: -1, limit: 1, rows: [] },
      { version: 1, orderBy: [{ column: "id" }], offset: 0, limit: -1, rows: [] },
      { version: 1, orderBy: [{ column: "id" }], offset: 0.5, limit: 1, rows: [] },
      { version: 1, orderBy: [{ column: "id" }], offset: 0, limit: 1.5, rows: [] },
      { version: 1, orderBy: [{ column: "id" }], offset: 0, limit: 1, rows: [{ x: {} }] },
    ];
    for (const invalidState of invalidTopKStates) {
      expect(() => deserializeTopKOperatorState(invalidState)).toThrowError(LakeqlError);
    }

    const invalidSortStates = [
      null,
      {
        version: 1,
        orderBy: [{ column: "id" }],
        runs: [{ spillRef: "", rowCount: 1, byteSize: 1 }],
      },
      {
        version: 1,
        orderBy: [{ column: "id" }],
        runs: [{ spillRef: "run", rowCount: 1.5, byteSize: 1 }],
      },
      {
        version: 1,
        orderBy: [{ column: "id" }],
        runs: [{ spillRef: "run", rowCount: 1, byteSize: -1 }],
      },
      { version: 1, orderBy: [{ column: "id" }], runs: [{ rows: [{ nested: {} }] }] },
    ];
    for (const invalidState of invalidSortStates) {
      expect(() => deserializeSortOperatorState(invalidState)).toThrowError(LakeqlError);
    }

    const richWindowState = {
      version: 1,
      windows: {
        rich: {
          fn: { aggregate: "sum" },
          args: [
            { kind: "arithmetic", op: "add", left: col("amount"), right: lit(1) },
            {
              kind: "case",
              whens: [{ when: gt("amount", 0), value: lit(1) }],
              else: lit(0),
            },
          ],
          over: {
            partitionBy: [
              and(
                eq("region", "west"),
                isIn("id", [1, 2]),
                between("amount", 1, 100),
                isNull("missing"),
                not(like("region", "x%")),
                fn("floor", col("amount")),
              ),
            ],
            orderBy: [{ expr: col("id"), direction: "desc", nulls: "first" }],
            frame: {
              mode: "rows",
              start: { kind: "preceding", offset: lit(1) },
              end: { kind: "current-row" },
              exclude: "ties",
            },
          },
          filter: or(eq("region", "east"), {
            kind: "null-check",
            negated: true,
            target: col("id"),
          }),
          ignoreNulls: false,
          distinct: true,
        },
      },
      rows: [{ id: 1, region: "west", amount: 10 }],
    };
    expect(deserializeWindowOperatorState(richWindowState)).toEqual(richWindowState);
    const runWindowState = {
      version: 1,
      windows: richWindowState.windows,
      runs: [{ rows: [{ id: 1, region: "west", amount: 10 }] }],
    };
    expect(deserializeWindowOperatorState(runWindowState)).toEqual(runWindowState);

    const invalidWindowStates = [
      null,
      { version: 2, windows: {}, rows: [] },
      { version: 1, windows: [], rows: [] },
      { version: 1, windows: richWindowState.windows },
      { version: 1, windows: richWindowState.windows, rows: [], runs: [] },
      {
        version: 1,
        windows: richWindowState.windows,
        runs: [{ spillRef: "", rowCount: 1, byteSize: 1 }],
      },
      {
        version: 1,
        windows: richWindowState.windows,
        runs: [{ spillRef: "run", rowCount: 1.5, byteSize: 1 }],
      },
      {
        version: 1,
        windows: richWindowState.windows,
        runs: [{ spillRef: "run", rowCount: 1, byteSize: -1 }],
      },
      { version: 1, windows: richWindowState.windows, runs: [{ rows: [{ nested: {} }] }] },
      {
        version: 1,
        windows: { rn: { fn: 1, args: [], over: { partitionBy: [], orderBy: [] } } },
        rows: [],
      },
      {
        version: 1,
        windows: { rn: { fn: "row_number", args: "bad", over: { partitionBy: [], orderBy: [] } } },
        rows: [],
      },
      {
        version: 1,
        windows: { rn: { fn: "row_number", args: [], over: { partitionBy: "bad", orderBy: [] } } },
        rows: [],
      },
      {
        version: 1,
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: { partitionBy: [], orderBy: [{ expr: col("id"), direction: "sideways" }] },
          },
        },
        rows: [],
      },
      {
        version: 1,
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: { partitionBy: [], orderBy: [], frame: { mode: "bad" } },
          },
        },
        rows: [],
      },
      {
        version: 1,
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: { partitionBy: [], orderBy: [] },
            filter: { kind: "unknown" },
          },
        },
        rows: [],
      },
      {
        version: 1,
        windows: { rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [] } } },
        rows: [{ nested: {} }],
      },
    ];
    for (const invalidState of invalidWindowStates) {
      expect(() => deserializeWindowOperatorState(invalidState)).toThrowError(LakeqlError);
    }
  });

  it("requires a spill adapter when resuming spilled window run state", async () => {
    const windowExpr = {
      fn: "row_number",
      args: [],
      over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
    } as const;
    await expect(
      (await makeLake({ rowsByPath: { table: [] } })).lake
        .path("table")
        .window("rn", windowExpr)
        .windowWithState({
          operatorState: {
            version: 1,
            windows: { rn: windowExpr },
            runs: [{ spillRef: "missing-run", rowCount: 1, byteSize: 1 }],
          },
        }),
    ).rejects.toMatchObject({ code: "LAKEQL_BOOKMARK_INVALID" });
  });

  it("validates aggregate requests and value types", async () => {
    const { lake } = await makeLake({
      rowsByPath: {
        table: [{ region: "west", amount: "not numeric" }],
      },
    });

    await expect(
      lake
        .path("table")
        .groupBy([""])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(lake.path("table").groupBy(["region"]).aggregate({})).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
    });
    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ total: { op: "sum", column: "amount" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      lake
        .path("table")
        .groupBy(["region"])
        .aggregate({ amountP50: { op: "quantile", column: "amount", quantile: 1.5 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      lake
        .path("table")
        .groupBy(["missing"])
        .aggregate({ rows: { op: "count" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNKNOWN_COLUMN" });
  });

  it("rejects mixed timestamp ordering and non-scalar window state snapshots", async () => {
    const timestamp = timestampValueFromIso("2026-01-01T00:00:00.000Z");
    if (timestamp === undefined) throw new Error("missing timestamp fixture");
    await expect(
      (
        await makeLake({
          rowsByPath: {
            table: [
              { id: 1, ts: timestamp },
              { id: 2, ts: "2026-01-02T00:00:00.000Z" },
            ],
          },
        })
      ).lake
        .path("table")
        .orderBy([{ column: "ts" }])
        .toArray(),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });

    await expect(
      (
        await makeLake({
          rowsByPath: {
            table: [{ id: 1, region: "west", payload: { nested: true } }],
          },
        })
      ).lake
        .path("table")
        .window("rn", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        })
        .window("first_payload", {
          fn: "first_value",
          args: [col("payload")],
          over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
        })
        .windowWithState({ spill: memorySpillAdapter() }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });
});
