import { parquetReadObjects, parquetSchema } from "hyparquet";
import {
  type Batch,
  batchFromVectors,
  type CacheAdapter,
  LakeqlError,
  type ObjectInfo,
  type ObjectStore,
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
import type { DecodedColumnCache } from "./decoded-column-cache.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { readCachedParquetMetadata } from "./metadata-cache.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { cachedRangeBuffer, type RangeCacheOptions } from "./range-cache.js";
import { recordReadColumns } from "./read-metrics.js";
import { planRowGroupsFromMetadata } from "./row-group-plan.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromObjectInfo, asyncBufferFromStore } from "./store-buffer.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";
import { canReadParquetVectorBatches, readParquetVectorBatchesFromFile } from "./vector-batches.js";
import { windowParquetTasks } from "./window-task.js";

export class ParquetScanAdapter implements ScanAdapter {
  private readonly store: ObjectStore;
  private readonly defaultBatchSize: number;
  private readonly metadataCache: CacheAdapter<ParquetMetadata> | undefined;
  private readonly decodedColumnCache: DecodedColumnCache | undefined;
  private readonly scanRangeCache: RangeCacheOptions | undefined;

  constructor(
    store: ObjectStore,
    options: {
      batchSize?: number;
      metadataCache?: CacheAdapter<ParquetMetadata>;
      decodedColumnCache?: DecodedColumnCache;
      scanRangeCache?: RangeCacheOptions;
    } = {},
  ) {
    this.store = store;
    this.defaultBatchSize = options.batchSize ?? 4096;
    this.metadataCache = options.metadataCache;
    this.decodedColumnCache = options.decodedColumnCache;
    this.scanRangeCache = options.scanRangeCache;
  }

  async planObjects(objects: ObjectInfo[], _options: ScanObjectPlanOptions): Promise<ObjectInfo[]> {
    if (objects.length <= 1) return objects;
    const unified = new Map<string, ParquetColumnDescriptor>();
    const perFile = new Map<string, Map<string, ParquetColumnDescriptor>>();

    for (const object of objects) {
      const file = await asyncBufferFromObjectInfo(this.store, object);
      const metadata = await this.metadata(object.path, file);
      rejectUnsupportedParquetSchema(metadata);
      const columns = parquetColumnDescriptors(metadata, object.path);
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

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = this.scanBuffer(path, await asyncBufferFromStore(this.store, path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata, { columns: options.columns });
    const planned = plannedParquetSchema(options.object);
    const requestedColumns = options.columns ?? planned?.columns;
    const readColumns = presentColumns(requestedColumns, planned, metadata);
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
          metadata,
          rowFormat: "object",
          rowStart,
          rowEnd,
          compressors: lakeqlParquetCompressors,
          parsers: lakeqlParquetParsers,
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
            normalizeDecodedRows(await parquetReadObjects(readOptions)),
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
    const file = this.scanBuffer(path, await asyncBufferFromStore(this.store, path, options));
    const metadata = await this.metadata(path, file, options);
    rejectUnsupportedParquetSchema(metadata, { columns: options.columns });
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
      if (
        present !== undefined &&
        present.length > 0 &&
        canReadParquetVectorBatches(metadata, vectorOptions)
      ) {
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

  async planTask(path: string, options: ScanTaskPlanOptions): Promise<ScanTaskPlan> {
    const planningReadOptions: ScanOptions | undefined =
      options.stats === undefined || options.budget === undefined || options.now === undefined
        ? undefined
        : {
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
    const metadata = await this.metadata(path, file, planningReadOptions);
    const plan = planRowGroupsFromMetadata(metadata, options.where);
    if (options.stats !== undefined) {
      options.stats.rowGroupsTotal =
        (options.stats.rowGroupsTotal ?? 0) + metadata.row_groups.length;
      options.stats.rowGroupsPlanned =
        (options.stats.rowGroupsPlanned ?? 0) + plan.rowGroups.length;
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
      signature: parquetColumnSignature(element),
      ...(typeof element.type === "string" ? { physicalType: element.type } : {}),
    });
  }
  return columns;
}

function parquetColumnSignature(element: Record<string, unknown>): string {
  return stableSchemaSignature({
    type: element.type,
    converted_type: element.converted_type,
    logical_type: element.logical_type,
    repetition_type: element.repetition_type,
    type_length: element.type_length,
    precision: element.precision,
    scale: element.scale,
  });
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
  const physicalType =
    descriptor.physicalType ?? /"type":"([^"]+)"/u.exec(descriptor.signature)?.[1];
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
): AsyncIterable<ScanVectorBatch> {
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  let rowGroupStart = 0;
  for (const rowGroup of metadata.row_groups) {
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
