import { parquetReadObjects, parquetSchema } from "hyparquet";
import {
  type Batch,
  batchFromVectors,
  type CacheAdapter,
  LakeqlError,
  type MemoryBudget,
  MemoryScope,
  type ObjectInfo,
  type ObjectStore,
  type OwnedMemory,
  type Row,
  type ScanAdapter,
  type ScanColumnBatch,
  type ScanObjectPlanOptions,
  type ScanOptions,
  type ScanTaskPlan,
  type ScanTaskPlanOptions,
  type ScanVectorBatch,
  type TaskInput,
  throwIfAborted,
  vectorFromValues,
  type WindowExpr,
  type WindowTaskExecutionOptions,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { lakeqlParquetCompressors } from "./compressors.js";
import { normalizeDecimalRows, rawDecimalMetadata } from "./decimal.js";
import type { DecodedColumnCache } from "./decoded-column-cache.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { iterateIndexedGroups } from "./indexed-row-group-plan.js";
import { acquireIndexedParquetSource } from "./indexed-source.js";
import type { OwnedMetadataCache } from "./metadata-cache.js";
import { readCachedParquetMetadata, readOwnedParquetMetadata } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { cachedRangeBuffer, type RangeCacheOptions } from "./range-cache.js";
import { recordReadColumns } from "./read-metrics.js";
import { ParquetReadScope } from "./read-scope.js";
import { iteratePlannedRowGroups, summarizePlannedRowGroups } from "./row-group-plan.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromObjectInfo, asyncBufferFromStore } from "./store-buffer.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";
import { canReadParquetVectorBatches, readParquetVectorBatchesFromFile } from "./vector-batches.js";
import { windowParquetTasks } from "./window-task.js";

export class ParquetScanAdapter implements ScanAdapter {
  readonly scanOwnedVectorBatches?: NonNullable<ScanAdapter["scanOwnedVectorBatches"]>;
  private readonly store: ObjectStore;
  private readonly defaultBatchSize: number;
  private readonly scanMetadataMode: "indexed" | "cached";
  private readonly taskMetadataMode: "indexed" | "cached";
  private readonly metadataCache: CacheAdapter<ParquetMetadata> | undefined;
  private readonly decodedColumnCache: DecodedColumnCache | undefined;
  private readonly scanRangeCache: RangeCacheOptions | undefined;

  constructor(
    store: ObjectStore,
    options: {
      batchSize?: number;
      metadataCache?: CacheAdapter<ParquetMetadata>;
      taskMetadataMode?: "auto" | "indexed" | "cached";
      scanMetadataMode?: "auto" | "indexed" | "cached";
      decodedColumnCache?: DecodedColumnCache;
      scanRangeCache?: RangeCacheOptions;
    } = {},
  ) {
    this.store = store;
    const cache = options.metadataCache as OwnedMetadataCache | undefined;
    this.scanMetadataMode =
      options.scanMetadataMode === undefined || options.scanMetadataMode === "auto"
        ? cache === undefined
          ? "indexed"
          : "cached"
        : options.scanMetadataMode;
    // Legacy cache adapters expose borrowed values, not independently pinnable backing.
    if (
      store.getRangeOwned !== undefined &&
      (this.scanMetadataMode === "indexed" ||
        cache === undefined ||
        (cache.acquireOwned !== undefined && cache.setOwned !== undefined))
    ) {
      this.scanOwnedVectorBatches = this.readOwnedVectorBatches.bind(this);
    }
    this.defaultBatchSize = options.batchSize ?? 4096;
    this.metadataCache = options.metadataCache;
    this.taskMetadataMode =
      options.taskMetadataMode === undefined || options.taskMetadataMode === "auto"
        ? options.metadataCache === undefined
          ? "indexed"
          : "cached"
        : options.taskMetadataMode;
    this.decodedColumnCache = options.decodedColumnCache;
    this.scanRangeCache = options.scanRangeCache;
  }

  async planObjects(objects: ObjectInfo[], options: ScanObjectPlanOptions): Promise<ObjectInfo[]> {
    if (objects.length <= 1) return objects;
    const unified = new Map<string, ParquetColumnDescriptor>();
    const perFile = new Map<string, Map<string, ParquetColumnDescriptor>>();

    for (const object of objects) {
      const file = await asyncBufferFromObjectInfo(this.store, object);
      const columns = await this.objectColumnDescriptors(object.path, file, options);
      perFile.set(object.path, columns);
      for (const [column, descriptor] of columns) {
        const existing = unified.get(column);
        if (existing === undefined) {
          unified.set(column, descriptor);
        } else if (!parquetColumnCompatible(existing, descriptor)) {
          throw new LakeqlError(
            "LAKEQL_SCHEMA_CONFLICT",
            `Parquet schema conflict for column ${column}`,
            {
              column,
              leftPath: existing.path,
              leftType: existing.signature,
              rightPath: object.path,
              rightType: descriptor.signature,
            },
          );
        }
      }
    }

    const unifiedColumns = [...unified.keys()].sort();
    return objects.map((object) => {
      const fileColumns = perFile.get(object.path) ?? new Map();
      return {
        ...object,
        parquetSchemaPlan: {
          columns: unifiedColumns,
          present: new Set(fileColumns.keys()),
        },
      };
    });
  }

  private async objectColumnDescriptors(
    path: string,
    file: StoreAsyncBuffer,
    options: ScanObjectPlanOptions,
  ): Promise<Map<string, ParquetColumnDescriptor>> {
    if (
      this.taskMetadataMode === "indexed" &&
      options.memoryBudget !== undefined &&
      file.sliceOwned !== undefined
    ) {
      const source = await acquireIndexedParquetSource(
        this.scanBuffer(path, file),
        options.memoryBudget,
      );
      try {
        rejectUnsupportedParquetSchema(source.metadata, { allowValueCheckedScalarDecimals: true });
        return parquetColumnDescriptors({ ...source.metadata, row_groups: [] }, path);
      } finally {
        source.dispose();
      }
    }
    const metadata = await this.metadata(path, file);
    rejectUnsupportedParquetSchema(metadata, { allowValueCheckedScalarDecimals: true });
    return parquetColumnDescriptors(metadata, path);
  }

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = this.scanBuffer(path, await this.scanFile(path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata, {
      columns: options.columns,
      allowValueCheckedScalarDecimals: true,
    });
    const planned = plannedParquetSchema(options.object);
    const requestedColumns = options.columns ?? planned?.columns;
    const readColumns = presentColumns(requestedColumns, planned, metadata);
    const decodedColumns =
      readColumns ?? parquetSchema(metadata).children.map((child) => child.element.name);
    const missingColumns = missingColumnsFor(requestedColumns, planned);
    if (readColumns) {
      recordReadColumns(options.stats, readColumns);
    }

    let rowGroupStart = 0;
    for (const rowGroup of metadata.row_groups) {
      throwIfAborted(options.budget.signal);
      const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
      if (!rowGroupMayMatch(rowGroup, options.where)) {
        options.stats.rowGroupsSkipped += 1;
        rowGroupStart = rowGroupEnd;
        continue;
      }
      options.stats.rowGroupsRead += 1;
      for (let rowStart = rowGroupStart; rowStart < rowGroupEnd; rowStart += batchSize) {
        throwIfAborted(options.budget.signal);
        const rowEnd = Math.min(rowStart + batchSize, rowGroupEnd);
        const readOptions: Parameters<typeof parquetReadObjects>[0] = {
          file,
          metadata: rawDecimalMetadata(metadata, decodedColumns),
          rowFormat: "object",
          rowStart,
          rowEnd,
          compressors: lakeqlParquetCompressors,
          parsers: lakeqlParquetParsers,
          utf8: false,
        };
        if (readColumns) readOptions.columns = readColumns;
        try {
          if (readColumns !== undefined && readColumns.length === 0) {
            yield fillMissingRowColumns(
              Array.from({ length: rowEnd - rowStart }, () => ({})),
              missingColumns,
            );
            continue;
          }
          yield fillMissingRowColumns(
            normalizeDecodedRows(
              normalizeDecimalRows(metadata, decodedColumns, await parquetReadObjects(readOptions)),
            ),
            missingColumns,
          );
        } catch (cause) {
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, {
            path,
            cause,
          });
        }
      }
      rowGroupStart = rowGroupEnd;
    }
  }

  async *scanColumns(path: string, options: ScanOptions): AsyncIterable<Batch> {
    for await (const vectorBatch of this.scanVectorBatches(path, options)) {
      yield vectorBatch.batch;
    }
  }

  async *scanColumnBatches(path: string, options: ScanOptions): AsyncIterable<ScanColumnBatch> {
    yield* this.scanVectorBatches(path, options);
  }

  async *scanVectorBatches(path: string, options: ScanOptions): AsyncIterable<ScanVectorBatch> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = this.scanBuffer(path, await this.scanFile(path, options));
    const metadata = await this.metadata(path, file, options);
    const planned = plannedParquetSchema(options.object);
    const requestedColumns = options.columns ?? planned?.columns;
    const present = presentColumns(requestedColumns, planned, metadata);
    const missing = missingColumnsFor(requestedColumns, planned);
    try {
      const vectorOptions = {
        batchSize,
        ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
        ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        ...(present === undefined ? {} : { columns: present }),
        ...(options.where === undefined ? {} : { where: options.where }),
        ...(options.canStopEarly === undefined ? {} : { canStopEarly: options.canStopEarly }),
        ...(options.budget.maxConcurrentReads === undefined
          ? {}
          : { maxConcurrentReads: options.budget.maxConcurrentReads }),
        now: options.now,
        ...(this.decodedColumnCache === undefined
          ? {}
          : {
              decodedColumnCache: this.decodedColumnCache,
              decodedColumnCacheKey: path,
            }),
        stats: options.stats,
      };
      const useVectorReader =
        present !== undefined &&
        present.length > 0 &&
        canReadParquetVectorBatches(metadata, vectorOptions);
      rejectUnsupportedParquetSchema(metadata, {
        columns: options.columns,
        allowValueCheckedScalarDecimals: true,
      });
      if (useVectorReader) {
        for await (const vectorBatch of readParquetVectorBatchesFromFile(
          file,
          metadata,
          vectorOptions,
        )) {
          throwIfAborted(options.budget.signal);
          yield {
            ...vectorBatch,
            batch: fillMissingBatchColumns(vectorBatch.batch, missing),
          };
        }
        return;
      }
      if (present !== undefined && present.length === 0) {
        yield* nullOnlyVectorBatches(metadata, options, missing, batchSize);
        return;
      }
      for await (const columnBatch of readParquetColumnBatchesFromFile(file, metadata, {
        batchSize,
        ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
        ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        ...(present === undefined ? {} : { columns: present }),
        ...(options.where === undefined ? {} : { where: options.where }),
        ...(options.canStopEarly === undefined ? {} : { canStopEarly: options.canStopEarly }),
        ...(options.budget.maxConcurrentReads === undefined
          ? {}
          : { maxConcurrentReads: options.budget.maxConcurrentReads }),
        now: options.now,
        ...(this.decodedColumnCache === undefined
          ? {}
          : {
              decodedColumnCache: this.decodedColumnCache,
              decodedColumnCacheKey: path,
            }),
        stats: options.stats,
      })) {
        throwIfAborted(options.budget.signal);
        yield {
          ...columnBatch,
          batch: fillMissingBatchColumns(columnBatch.batch, missing),
        };
      }
    } catch (cause) {
      if (cause instanceof LakeqlError) throw cause;
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, {
        path,
        cause,
      });
    }
  }

  private async *readOwnedVectorBatches(
    path: string,
    options: ScanOptions & { memoryBudget: MemoryBudget },
  ): AsyncIterable<OwnedMemory<ScanVectorBatch>> {
    try {
      yield* this.executeOwnedVectorScan(path, options);
    } catch (cause) {
      if (cause instanceof LakeqlError) throw cause;
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
    }
  }

  private async *executeOwnedVectorScan(
    path: string,
    options: ScanOptions & { memoryBudget: MemoryBudget },
  ): AsyncIterable<OwnedMemory<ScanVectorBatch>> {
    throwIfAborted(options.budget.signal);
    const signals = options.budget.signal === undefined ? [] : [options.budget.signal];
    if (options.budget.maxElapsedMs !== undefined) {
      const remaining = Math.max(
        0,
        Math.ceil(options.budget.maxElapsedMs - (options.now() - options.startedAt)),
      );
      signals.push(AbortSignal.timeout(remaining));
    }
    const rawFile = await this.scanFile(path, options);
    const ownedRead = rawFile.sliceOwned?.bind(rawFile);
    let metadataReads = 0;
    const file = this.scanBuffer(
      path,
      ownedRead === undefined
        ? rawFile
        : {
            ...rawFile,
            sliceOwned: (...args) => {
              metadataReads++;
              return ownedRead(...args);
            },
          },
    );
    const metadataStarted = options.now();
    const indexed =
      this.scanMetadataMode === "indexed"
        ? await acquireIndexedParquetSource(file, options.memoryBudget, signals)
        : undefined;
    const loaded =
      indexed === undefined
        ? await readOwnedParquetMetadata(
            path,
            file,
            options.memoryBudget,
            this.metadataCache,
            signals,
          )
        : undefined;
    if (loaded?.cached || (indexed !== undefined && metadataReads === 0))
      options.stats.cacheHits += 1;
    else {
      options.stats.cacheMisses += 1;
      options.stats.footerFetchMs =
        (options.stats.footerFetchMs ?? 0) + options.now() - metadataStarted;
    }
    try {
      const metadata =
        indexed === undefined ? loaded?.owner.value : { ...indexed.metadata, row_groups: [] };
      if (metadata === undefined)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing scan metadata source");
      const planned = plannedParquetSchema(options.object);
      // Omitted projection means all schema columns, not a request to bypass
      // owned vector decoding. Physical shape checks still choose fallback for
      // unsupported columns; packed text stays undecoded until output admission.
      const requested =
        options.columns ??
        planned?.columns ??
        parquetSchema(metadata).children.map((child) => child.element.name);
      const present = presentColumns(requested, planned, metadata);
      const missing = missingColumnsFor(requested, planned);
      const readOptions = {
        ...(options.object?.rangeReadMode === undefined
          ? {}
          : { rangeReadMode: options.object.rangeReadMode }),
        batchSize: options.batchSize ?? this.defaultBatchSize,
        ...(present === undefined ? {} : { columns: present }),
        ...(options.rowStart === undefined ? {} : { rowStart: options.rowStart }),
        ...(options.rowEnd === undefined ? {} : { rowEnd: options.rowEnd }),
        ...(options.where === undefined ? {} : { where: options.where }),
        ...(options.canStopEarly === undefined ? {} : { canStopEarly: options.canStopEarly }),
        ...(options.budget.maxConcurrentReads === undefined
          ? {}
          : { maxConcurrentReads: options.budget.maxConcurrentReads }),
        ...(this.decodedColumnCache === undefined
          ? {}
          : { decodedColumnCache: this.decodedColumnCache, decodedColumnCacheKey: path }),
        stats: options.stats,
        now: options.now,
      };
      rejectUnsupportedParquetSchema(metadata, {
        columns: options.columns,
        allowValueCheckedScalarDecimals: true,
      });
      if (present === undefined || present.length > 0) {
        for await (const owner of readOwnedParquetVectorBatches(
          file,
          indexed ?? metadata,
          options.memoryBudget,
          readOptions,
          signals,
        )) {
          const handoff = new MemoryScope(options.memoryBudget, signals);
          try {
            handoff.adopt(owner);
            const value = {
              ...owner.value,
              batch: fillMissingBatchColumns(owner.value.batch, missing),
            };
            yield handoff.retainAllocations(value);
          } finally {
            owner.release();
            handoff.dispose();
          }
        }
        return;
      }
      const stage = new ParquetReadScope(file, options.memoryBudget, signals);
      try {
        const batches = nullOnlyVectorBatches(
          metadata,
          options,
          missing,
          readOptions.batchSize,
          indexed === undefined ? metadata.row_groups : iterateIndexedGroups(indexed, signals),
        );
        for await (const output of batches) {
          throwIfAborted(options.budget.signal);
          yield stage.retainInputBacking({
            ...output,
            batch: fillMissingBatchColumns(output.batch, missing),
          });
        }
      } finally {
        stage.dispose();
      }
    } finally {
      indexed?.dispose();
      loaded?.owner.release();
    }
  }

  async planTask(path: string, options: ScanTaskPlanOptions): Promise<ScanTaskPlan> {
    const planningReadOptions: ScanOptions | undefined =
      options.stats === undefined || options.budget === undefined || options.now === undefined
        ? undefined
        : {
            ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
            batchSize: this.defaultBatchSize,
            stats: options.stats,
            budget: options.budget,
            now: options.now,
            startedAt: options.now(),
          };
    const file =
      options.object === undefined
        ? await asyncBufferFromStore(this.store, path, planningReadOptions)
        : asyncBufferFromObjectInfo(this.store, options.object, planningReadOptions);
    if (
      options.memoryBudget !== undefined &&
      this.taskMetadataMode === "indexed" &&
      file.sliceOwned !== undefined
    ) {
      const signals = options.budget?.signal === undefined ? [] : [options.budget.signal];
      const started = options.now?.();
      const source = await acquireIndexedParquetSource(
        this.scanBuffer(path, file),
        options.memoryBudget,
        signals,
      );
      try {
        const plan = summarizePlannedRowGroups(
          iteratePlannedRowGroups(
            source.metadata,
            iterateIndexedGroups(source, signals),
            options.where,
            { includeByteRanges: false },
          ),
        );
        if (options.stats !== undefined) {
          options.stats.cacheMisses++;
          options.stats.rowGroupsTotal = (options.stats.rowGroupsTotal ?? 0) + source.rowGroupCount;
          options.stats.rowGroupsPlanned =
            (options.stats.rowGroupsPlanned ?? 0) + plan.plannedCount;
          if (started !== undefined && options.now !== undefined)
            options.stats.footerFetchMs =
              (options.stats.footerFetchMs ?? 0) + options.now() - started;
        }
        return { rowGroupCount: source.rowGroupCount, rowGroupRanges: plan.rowGroupRanges };
      } finally {
        source.dispose();
      }
    }
    const metadata = await this.metadata(path, file, planningReadOptions);
    const plan = summarizePlannedRowGroups(
      iteratePlannedRowGroups(metadata, metadata.row_groups, options.where, {
        includeByteRanges: false,
      }),
    );
    if (options.stats !== undefined) {
      options.stats.rowGroupsTotal =
        (options.stats.rowGroupsTotal ?? 0) + metadata.row_groups.length;
      options.stats.rowGroupsPlanned = (options.stats.rowGroupsPlanned ?? 0) + plan.plannedCount;
    }
    return {
      rowGroupCount: metadata.row_groups.length,
      rowGroupRanges: plan.rowGroupRanges,
    };
  }

  async executeWindowTasks(
    tasks: TaskInput[],
    windows: Record<string, WindowExpr>,
    options: WindowTaskExecutionOptions,
  ): Promise<Row[]> {
    return windowParquetTasks(this.store, tasks, windows, {
      ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
      batchSize: options.batchSize,
      budget: options.budget,
      stats: options.stats,
      now: options.now,
      startedAt: options.startedAt,
      ...(this.metadataCache === undefined ? {} : { metadataCache: this.metadataCache }),
      ...(options.maxConcurrentTasks === undefined
        ? {}
        : { maxConcurrentTasks: options.maxConcurrentTasks }),
      ...(options.maxBufferedPartials === undefined
        ? {}
        : { maxBufferedPartials: options.maxBufferedPartials }),
    });
  }

  private async metadata(
    path: string,
    file: StoreAsyncBuffer,
    options?: ScanOptions,
  ): Promise<ParquetMetadata> {
    const startedAt = options?.now();
    const { metadata, cached } = await readCachedParquetMetadata(path, file, this.metadataCache);
    if (cached) {
      if (options !== undefined) options.stats.cacheHits += 1;
    } else if (options !== undefined) {
      options.stats.cacheMisses += 1;
      if (startedAt !== undefined) {
        options.stats.footerFetchMs =
          (options.stats.footerFetchMs ?? 0) + (options.now() - startedAt);
      }
    }
    return metadata;
  }

  private async scanFile(path: string, options: ScanOptions): Promise<StoreAsyncBuffer> {
    throwIfAborted(options.budget.signal);
    return options.object?.path === path
      ? asyncBufferFromObjectInfo(this.store, options.object, options)
      : await asyncBufferFromStore(this.store, path, options);
  }

  private scanBuffer(path: string, file: StoreAsyncBuffer): StoreAsyncBuffer {
    return this.scanRangeCache === undefined
      ? file
      : cachedRangeBuffer(file, this.scanRangeCache, path);
  }
}

interface PlannedParquetSchema {
  columns: string[];
  present: Set<string>;
}

interface PlannedParquetObjectInfo extends ObjectInfo {
  parquetSchemaPlan?: PlannedParquetSchema;
}

function plannedParquetSchema(object: ObjectInfo | undefined): PlannedParquetSchema | undefined {
  return (object as PlannedParquetObjectInfo | undefined)?.parquetSchemaPlan;
}

interface ParquetColumnDescriptor {
  path: string;
  signature: string;
  physicalType?: string;
}

function parquetColumnDescriptors(
  metadata: ParquetMetadata,
  path: string,
): Map<string, ParquetColumnDescriptor> {
  const columns = new Map<string, ParquetColumnDescriptor>();
  for (const child of parquetSchema(metadata).children) {
    const element = child.element as unknown as Record<string, unknown>;
    const name = String(element.name ?? "");
    if (name.length === 0) continue;
    columns.set(name, {
      path,
      signature: stableSchemaSignature(parquetColumnShape(child)),
      ...(typeof element.type === "string" ? { physicalType: element.type } : {}),
    });
  }
  return columns;
}

function parquetColumnShape(node: ReturnType<typeof parquetSchema>): Record<string, unknown> {
  const element = node.element as unknown as Record<string, unknown>;
  return {
    type: element.type,
    converted_type: element.converted_type,
    logical_type: element.logical_type,
    repetition_type: element.repetition_type,
    type_length: element.type_length,
    precision: element.precision,
    scale: element.scale,
    children: node.children.map((child) => ({
      name: child.element.name,
      ...parquetColumnShape(child),
    })),
  };
}

function stableSchemaSignature(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableSchemaSignature).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter((key) => record[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${stableSchemaSignature(record[key])}`)
    .join(",")}}`;
}

function parquetColumnCompatible(
  left: ParquetColumnDescriptor,
  right: ParquetColumnDescriptor,
): boolean {
  return left.signature === right.signature || (isNumericColumn(left) && isNumericColumn(right));
}

function isNumericColumn(descriptor: ParquetColumnDescriptor): boolean {
  const physicalType = descriptor.physicalType;
  return (
    physicalType === "INT32" ||
    physicalType === "INT64" ||
    physicalType === "FLOAT" ||
    physicalType === "DOUBLE"
  );
}

function presentColumns(
  requested: readonly string[] | undefined,
  planned: PlannedParquetSchema | undefined,
  metadata: ParquetMetadata,
): string[] | undefined {
  if (requested === undefined) return undefined;
  const present = planned?.present ?? new Set(parquetColumnDescriptors(metadata, "").keys());
  return requested.filter((column) => present.has(column));
}

function missingColumnsFor(
  requested: readonly string[] | undefined,
  planned: PlannedParquetSchema | undefined,
): string[] {
  if (requested === undefined || planned === undefined) return [];
  return requested.filter((column) => !planned.present.has(column));
}

function fillMissingRowColumns(rows: Row[], missingColumns: readonly string[]): Row[] {
  if (missingColumns.length === 0) return rows;
  return rows.map((row) => {
    const filled: Row = { ...row };
    for (const column of missingColumns) filled[column] = null;
    return filled;
  });
}

function fillMissingBatchColumns(batch: Batch, missingColumns: readonly string[]): Batch {
  if (missingColumns.length === 0) return batch;
  const columns = { ...batch.columns };
  for (const column of missingColumns) {
    columns[column] = vectorFromValues(Array.from({ length: batch.rowCount }, () => null));
  }
  return batchFromVectors(columns);
}

async function* nullOnlyVectorBatches(
  metadata: ParquetMetadata,
  options: ScanOptions,
  columns: readonly string[],
  batchSize: number,
  groups: Iterable<ParquetMetadata["row_groups"][number]> = metadata.row_groups,
): AsyncIterable<ScanVectorBatch> {
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  let rowGroupStart = 0;
  for (const rowGroup of groups) {
    const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
    if (
      rowGroupEnd <= requestedStart ||
      rowGroupStart >= requestedEnd ||
      !rowGroupMayMatch(rowGroup, options.where)
    ) {
      options.stats.rowGroupsSkipped += 1;
      rowGroupStart = rowGroupEnd;
      continue;
    }
    options.stats.rowGroupsRead += 1;
    const start = Math.max(rowGroupStart, requestedStart);
    const end = Math.min(rowGroupEnd, requestedEnd);
    for (let rowStart = start; rowStart < end; rowStart += batchSize) {
      const rowEnd = Math.min(rowStart + batchSize, end);
      yield {
        rowOffset: rowStart,
        batch: nullColumnsBatch(columns, rowEnd - rowStart),
      };
    }
    rowGroupStart = rowGroupEnd;
  }
}

function nullColumnsBatch(columns: readonly string[], rowCount: number): Batch {
  return batchFromVectors(
    Object.fromEntries(
      columns.map((column) => [
        column,
        vectorFromValues(Array.from({ length: rowCount }, () => null)),
      ]),
    ),
  );
}
