import {
  advanceTaskCheckpoint,
  type CacheAdapter,
  type CheckpointAdapter,
  createOutputManifest,
  type Expr,
  Lake,
  type LakeConfig,
  LaQLError,
  type ObjectStore,
  type OutputManifest,
  type OutputManifestEntry,
  type Row,
  type ScanAdapter,
  type ScanOptions,
  type ScanTaskPlan,
  type ScanTaskPlanOptions,
  type TaskCheckpoint,
} from "@laql/core";
import type { RowGroup } from "hyparquet";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import type { BasicType, ColumnSource, ParquetWriteOptions } from "hyparquet-writer";
import { parquetWriteBuffer } from "hyparquet-writer";

export interface ReadParquetOptions {
  /** Columns to project; all columns when omitted. */
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
}

export type IcebergParquetDeleteFileContent =
  | "position-delete"
  | "equality-delete"
  | "deletion-vector";

export interface IcebergParquetDeleteFile {
  content: IcebergParquetDeleteFileContent;
  path: string;
}

export interface DecodedIcebergParquetDeletes {
  positionDeletes?: { path: string; position: number }[];
  equalityDeletes?: { columns: string[]; row: Row }[];
}

export interface WriteParquetOptions extends Omit<ParquetWriteOptions, "writer" | "columnData"> {
  columnData: ColumnSource[];
  contentType?: string;
  writeMode?: "overwrite" | "create";
}

export interface WriteParquetRowsOptions
  extends Omit<WriteParquetOptions, "columnData" | "schema"> {
  rows: Row[];
  partitionBy?: string[];
  maxRowsPerFile?: number;
  maxBytesPerFile?: number;
  jobId?: string;
  taskId?: string;
  idempotencyKey?: string;
  columnTypes?: Record<string, BasicType>;
  validation?: InsertValidationRules;
}

export interface InsertValidationRules {
  required?: string[];
  unique?: string[][];
  ranges?: Record<string, { min?: ComparableInsertValue; max?: ComparableInsertValue }>;
  enums?: Record<string, InsertValue[]>;
}

export interface WritePartitionedParquetFile {
  path: string;
  byteSize: number;
  contentHash: string;
  etag?: string;
  rowCount: number;
  partitionValues: Record<string, string>;
}

export interface WritePartitionedParquetResult {
  files: WritePartitionedParquetFile[];
}

export interface WritePartitionedParquetTaskOptions extends WriteParquetRowsOptions {
  checkpoints: CheckpointAdapter;
  taskId: string;
  idempotencyKey: string;
  nowMs?: number;
  staleTimeoutMs?: number;
  iceberg?: boolean;
}

export interface WritePartitionedParquetTaskResult {
  result: WritePartitionedParquetResult;
  entries: OutputManifestEntry[];
}

export interface CreateParquetTableAsQuery {
  toArray(): Promise<Row[]>;
}

export interface CreateParquetTableAsOptions
  extends Omit<WritePartitionedParquetTaskOptions, "rows" | "jobId" | "taskId" | "idempotencyKey"> {
  query: CreateParquetTableAsQuery;
  jobId: string;
  planFingerprint: string;
  taskId?: string;
  idempotencyKey: string;
}

export interface CreateParquetTableAsResult extends WritePartitionedParquetTaskResult {
  manifest: OutputManifest;
  rowsRead: number;
}

export interface PartitionedParquetOutputEntryOptions {
  taskId: string | ((file: WritePartitionedParquetFile, index: number) => string);
  iceberg?: boolean;
}

export interface ParquetLakeConfig extends Omit<LakeConfig, "scanner"> {
  batchSize?: number;
  metadataCache?: CacheAdapter<ParquetMetadata>;
}

export type ParquetMetadata = Awaited<ReturnType<typeof parquetMetadataAsync>>;

interface StoreAsyncBuffer {
  byteLength: number;
  etag?: string;
  slice(start: number, end?: number): Promise<ArrayBuffer>;
}

/**
 * Bridge an ObjectStore path to hyparquet's AsyncBuffer (length + ranged slice).
 */
export async function asyncBufferFromStore(
  store: ObjectStore,
  path: string,
  options: ScanOptions | undefined = undefined,
) {
  const head = await store.head(path);
  if (!head) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
  }
  const buffer: StoreAsyncBuffer = {
    byteLength: head.size,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const length = (end ?? head.size) - start;
      if (options) {
        options.stats.rangeRequests += 1;
        options.stats.bytesRequested += length;
      }
      const bytes = await store.getRange(path, { offset: start, length });
      const out = new ArrayBuffer(bytes.byteLength);
      new Uint8Array(out).set(bytes);
      return out;
    },
  };
  if (head.etag !== undefined) buffer.etag = head.etag;
  return buffer;
}

/**
 * Read rows from a Parquet object. Early scaffold: full planner-driven
 * row-group pruning and batch streaming land in phase 1-2 (see BUILD_PLAN.md).
 */
export async function readParquetObjects(
  store: ObjectStore,
  path: string,
  options: ReadParquetOptions = {},
): Promise<Record<string, unknown>[]> {
  const file = await asyncBufferFromStore(store, path);
  try {
    const readOptions: Parameters<typeof parquetReadObjects>[0] = { file };
    if (options.columns) readOptions.columns = options.columns;
    if (options.rowStart !== undefined) readOptions.rowStart = options.rowStart;
    if (options.rowEnd !== undefined) readOptions.rowEnd = options.rowEnd;
    return await parquetReadObjects(readOptions);
  } catch (cause) {
    throw new LaQLError("LAQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
  }
}

/** Read Parquet footer metadata (row groups, schema, stats). */
export async function readParquetMetadata(store: ObjectStore, path: string) {
  const file = await asyncBufferFromStore(store, path);
  return parquetMetadataAsync(file);
}

export async function readIcebergParquetDeletes(
  store: ObjectStore,
  deleteFile: IcebergParquetDeleteFile,
): Promise<DecodedIcebergParquetDeletes> {
  switch (deleteFile.content) {
    case "position-delete":
      return {
        positionDeletes: decodePositionDeleteRows(await readParquetObjects(store, deleteFile.path)),
      };
    case "equality-delete":
      return {
        equalityDeletes: decodeEqualityDeleteRows(await readParquetObjects(store, deleteFile.path)),
      };
    case "deletion-vector":
      throw new LaQLError(
        "LAQL_UNSUPPORTED_DELETE_FILES",
        "Iceberg deletion vectors are not Parquet delete files",
        { path: deleteFile.path, content: deleteFile.content },
      );
  }
}

export async function writeParquet(
  store: ObjectStore,
  path: string,
  options: WriteParquetOptions,
): Promise<{ path: string; byteSize: number; etag?: string }> {
  try {
    const { contentType, writeMode, ...writeOptions } = options;
    validateWriteMode(writeMode);
    return await putParquetBytes(
      store,
      path,
      encodeParquetBytes(writeOptions),
      contentType,
      writeMode,
    );
  } catch (cause) {
    if (cause instanceof LaQLError) throw cause;
    throw new LaQLError("LAQL_PARQUET_WRITE_ERROR", `Failed to write ${path}`, { path, cause });
  }
}

export async function writePartitionedParquet(
  store: ObjectStore,
  prefix: string,
  options: WriteParquetRowsOptions,
): Promise<WritePartitionedParquetResult> {
  const maxRowsPerFile = options.maxRowsPerFile ?? options.rows.length;
  validatePartitionedWriteOptions(prefix, options, maxRowsPerFile);

  const normalizedPrefix = prefix.replace(/\/+$/u, "");
  const partitionBy = options.partitionBy ?? [];
  validateWriteMode(options.writeMode);
  validateInsertRows(options.rows, options.validation);
  const {
    rows: _rows,
    partitionBy: _partitionBy,
    maxRowsPerFile: _maxRowsPerFile,
    maxBytesPerFile,
    jobId,
    taskId,
    idempotencyKey,
    columnTypes,
    validation: _validation,
    contentType,
    writeMode,
    ...writeOptions
  } = options;
  const partitions = partitionRows(options.rows, partitionBy);
  const files: WritePartitionedParquetFile[] = [];
  let ordinal = 0;

  for (const partition of partitions) {
    for (let start = 0; start < partition.rows.length; start += maxRowsPerFile) {
      const chunk = partition.rows.slice(start, start + maxRowsPerFile);
      const encodedChunks = splitRowsForFileSize(
        chunk,
        partitionBy,
        columnTypes ?? {},
        writeOptions,
        maxBytesPerFile,
      );
      for (const encodedChunk of encodedChunks) {
        const path = partitionOutputPath(
          normalizedPrefix,
          partition.values,
          partitionBy,
          jobId,
          taskId,
          idempotencyKey,
          ordinal,
        );
        const written = await writeEncodedParquet(
          store,
          path,
          encodedChunk.bytes,
          contentType,
          writeMode,
        );
        const result: WritePartitionedParquetFile = {
          path: written.path,
          byteSize: written.byteSize,
          contentHash: await contentHash(encodedChunk.bytes),
          rowCount: encodedChunk.rows.length,
          partitionValues: partition.values,
        };
        if (written.etag !== undefined) result.etag = written.etag;
        files.push(result);
        ordinal += 1;
      }
    }
  }

  return { files };
}

export async function writePartitionedParquetTask(
  store: ObjectStore,
  prefix: string,
  options: WritePartitionedParquetTaskOptions,
): Promise<WritePartitionedParquetTaskResult> {
  const existing = await options.checkpoints.get(options.taskId);
  const existingOutputs =
    existing?.idempotencyKey === options.idempotencyKey ? checkpointOutputs(existing) : undefined;
  if (existing?.state === "complete" && existingOutputs !== undefined) {
    return { result: outputEntriesToPartitionedResult(existingOutputs), entries: existingOutputs };
  }

  const nowMs = options.nowMs ?? Date.now();
  let result: WritePartitionedParquetResult;
  let entries: OutputManifestEntry[];
  if (
    (existing?.state === "output-written" || existing?.state === "manifest-recorded") &&
    existingOutputs !== undefined
  ) {
    entries = existingOutputs;
    result = outputEntriesToPartitionedResult(entries);
  } else {
    if (existing?.state !== "running") {
      await advanceTaskCheckpoint(options.checkpoints, {
        taskId: options.taskId,
        nextState: "planned",
        idempotencyKey: options.idempotencyKey,
        nowMs,
        ...(options.staleTimeoutMs !== undefined ? { staleTimeoutMs: options.staleTimeoutMs } : {}),
      });
    }
    await advanceTaskCheckpoint(options.checkpoints, {
      taskId: options.taskId,
      nextState: "running",
      idempotencyKey: options.idempotencyKey,
      nowMs: nowMs + 1,
      ...(options.staleTimeoutMs !== undefined ? { staleTimeoutMs: options.staleTimeoutMs } : {}),
    });

    const {
      checkpoints: _checkpoints,
      nowMs: _nowMs,
      staleTimeoutMs: _staleTimeoutMs,
      iceberg,
      ...writeOptions
    } = options;
    result = await writePartitionedParquet(store, prefix, writeOptions);
    entries = partitionedParquetOutputEntries(result, {
      taskId: options.taskId,
      ...(iceberg !== undefined ? { iceberg } : {}),
    });

    await advanceTaskCheckpoint(options.checkpoints, {
      taskId: options.taskId,
      nextState: "output-written",
      idempotencyKey: options.idempotencyKey,
      nowMs: nowMs + 2,
      outputs: entries,
    });
  }

  if (existing?.state !== "manifest-recorded") {
    await advanceTaskCheckpoint(options.checkpoints, {
      taskId: options.taskId,
      nextState: "manifest-recorded",
      idempotencyKey: options.idempotencyKey,
      nowMs: nowMs + 3,
    });
  }
  await advanceTaskCheckpoint(options.checkpoints, {
    taskId: options.taskId,
    nextState: "complete",
    idempotencyKey: options.idempotencyKey,
    nowMs: nowMs + 4,
  });

  return { result, entries };
}

function checkpointOutputs(checkpoint: TaskCheckpoint): OutputManifestEntry[] | undefined {
  if (checkpoint.outputs !== undefined) return checkpoint.outputs;
  if (checkpoint.output !== undefined) return [checkpoint.output];
  return undefined;
}

export async function createParquetTableAs(
  store: ObjectStore,
  prefix: string,
  options: CreateParquetTableAsOptions,
): Promise<CreateParquetTableAsResult> {
  const rows = await options.query.toArray();
  const taskId = options.taskId ?? `${options.jobId}-ctas-000000`;
  const {
    query: _query,
    planFingerprint,
    taskId: _taskId,
    idempotencyKey,
    jobId,
    ...writeOptions
  } = options;
  const task = await writePartitionedParquetTask(store, prefix, {
    ...writeOptions,
    rows,
    jobId,
    taskId,
    idempotencyKey,
  });
  return {
    ...task,
    rowsRead: rows.length,
    manifest: createOutputManifest({
      jobId,
      planFingerprint,
      entries: task.entries,
    }),
  };
}

export function partitionedParquetOutputEntries(
  result: WritePartitionedParquetResult,
  options: PartitionedParquetOutputEntryOptions,
): OutputManifestEntry[] {
  return result.files.map((file, index) => {
    const entry: OutputManifestEntry = {
      taskId: typeof options.taskId === "function" ? options.taskId(file, index) : options.taskId,
      outputPath: file.path,
      partitionValues: sortStringRecord(file.partitionValues),
      rowCount: file.rowCount,
      byteSize: file.byteSize,
      contentHash: file.contentHash,
    };
    if (file.etag !== undefined) entry.etag = file.etag;
    if (options.iceberg === true) {
      entry.iceberg = {
        recordCount: file.rowCount,
        fileSizeInBytes: file.byteSize,
        partitionValues: sortStringRecord(file.partitionValues),
      };
    }
    return entry;
  });
}

function outputEntriesToPartitionedResult(
  entries: OutputManifestEntry[],
): WritePartitionedParquetResult {
  return {
    files: entries.map((entry) => {
      const file: WritePartitionedParquetFile = {
        path: entry.outputPath,
        byteSize: entry.byteSize,
        contentHash: entry.contentHash ?? "",
        rowCount: entry.rowCount,
        partitionValues: sortStringRecord(entry.partitionValues),
      };
      if (entry.etag !== undefined) file.etag = entry.etag;
      return file;
    }),
  };
}

export class ParquetScanAdapter implements ScanAdapter {
  private readonly store: ObjectStore;
  private readonly defaultBatchSize: number;
  private readonly metadataCache: CacheAdapter<ParquetMetadata> | undefined;

  constructor(
    store: ObjectStore,
    options: { batchSize?: number; metadataCache?: CacheAdapter<ParquetMetadata> } = {},
  ) {
    this.store = store;
    this.defaultBatchSize = options.batchSize ?? 4096;
    this.metadataCache = options.metadataCache;
  }

  async *scan(path: string, options: ScanOptions): AsyncIterable<Row[]> {
    const batchSize = options.batchSize || this.defaultBatchSize;
    const file = await asyncBufferFromStore(this.store, path, options);
    const metadata = await this.metadata(path, file, options);
    const readColumns = options.columns;
    if (readColumns) {
      const known = new Set(options.stats.columnsRead);
      for (const column of readColumns) {
        if (!known.has(column)) {
          known.add(column);
          options.stats.columnsRead.push(column);
        }
      }
      options.stats.columnsRead.sort();
    }

    let rowGroupStart = 0;
    for (const rowGroup of metadata.row_groups) {
      const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
      if (!rowGroupMayMatch(rowGroup, options.where)) {
        options.stats.rowGroupsSkipped += 1;
        rowGroupStart = rowGroupEnd;
        continue;
      }
      options.stats.rowGroupsRead += 1;
      for (let rowStart = rowGroupStart; rowStart < rowGroupEnd; rowStart += batchSize) {
        const rowEnd = Math.min(rowStart + batchSize, rowGroupEnd);
        const readOptions: Parameters<typeof parquetReadObjects>[0] = {
          file,
          metadata,
          rowFormat: "object",
          rowStart,
          rowEnd,
        };
        if (readColumns) readOptions.columns = readColumns;
        try {
          yield await parquetReadObjects(readOptions);
        } catch (cause) {
          throw new LaQLError("LAQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
        }
      }
      rowGroupStart = rowGroupEnd;
    }
  }

  async planTask(path: string, options: ScanTaskPlanOptions): Promise<ScanTaskPlan> {
    const file = await asyncBufferFromStore(this.store, path);
    const metadata = await parquetMetadataAsync(file);
    return { rowGroupRanges: matchingRowGroupRanges(metadata.row_groups, options.where) };
  }

  private async metadata(
    path: string,
    file: StoreAsyncBuffer,
    options: ScanOptions,
  ): Promise<ParquetMetadata> {
    if (!this.metadataCache) return parquetMetadataAsync(file);
    const key = metadataCacheKey(path, file.byteLength, file.etag);
    const cached = await this.metadataCache.get(key);
    if (cached) {
      options.stats.cacheHits += 1;
      return cached.value;
    }
    options.stats.cacheMisses += 1;
    const metadata = await parquetMetadataAsync(file);
    await this.metadataCache.set(key, { value: metadata });
    return metadata;
  }
}

function matchingRowGroupRanges(
  rowGroups: RowGroup[],
  where: Expr | undefined,
): { start: number; end: number }[] {
  const ranges: { start: number; end: number }[] = [];
  for (let index = 0; index < rowGroups.length; index += 1) {
    const rowGroup = rowGroups[index];
    if (rowGroup === undefined || !rowGroupMayMatch(rowGroup, where)) continue;
    const previous = ranges.at(-1);
    if (previous && previous.end === index) previous.end = index + 1;
    else ranges.push({ start: index, end: index + 1 });
  }
  return ranges;
}

function metadataCacheKey(path: string, byteLength: number, etag: string | undefined): string {
  return `parquet-metadata:${path}:${byteLength}:${etag ?? "no-etag"}`;
}

interface RowPartition {
  key: string;
  values: Record<string, string>;
  rows: Row[];
}

type ColumnValue = string | number | boolean | bigint | null;
type InsertValue = string | number | boolean | bigint | null;
type ComparableInsertValue = string | number | bigint;

interface EncodedRowChunk {
  rows: Row[];
  bytes: Uint8Array;
}

function validatePartitionedWriteOptions(
  prefix: string,
  options: WriteParquetRowsOptions,
  maxRowsPerFile: number,
): void {
  if (!prefix || prefix.replace(/\/+$/u, "") === "") {
    throw new LaQLError("LAQL_TYPE_ERROR", "Parquet output prefix must be non-empty");
  }
  if (options.rows.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Cannot write an empty row set");
  }
  if (!Number.isInteger(maxRowsPerFile) || maxRowsPerFile < 1) {
    throw new LaQLError("LAQL_TYPE_ERROR", "maxRowsPerFile must be a positive integer", {
      maxRowsPerFile,
    });
  }
  if (
    options.maxBytesPerFile !== undefined &&
    (!Number.isInteger(options.maxBytesPerFile) || options.maxBytesPerFile < 1)
  ) {
    throw new LaQLError("LAQL_TYPE_ERROR", "maxBytesPerFile must be a positive integer", {
      maxBytesPerFile: options.maxBytesPerFile,
    });
  }
  validateOptionalOutputPathComponent("taskId", options.taskId);
  validateOptionalOutputPathComponent("idempotencyKey", options.idempotencyKey);
  const partitionBy = options.partitionBy ?? [];
  const uniquePartitions = new Set(partitionBy);
  if (uniquePartitions.size !== partitionBy.length) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "partitionBy columns must be unique", {
      partitionBy,
    });
  }
}

function partitionRows(rows: Row[], partitionBy: string[]): RowPartition[] {
  const byKey = new Map<string, RowPartition>();
  for (const row of rows) {
    const values: Record<string, string> = {};
    for (const column of partitionBy) {
      const raw = row[column];
      if (!isPartitionValue(raw)) {
        throw new LaQLError("LAQL_VALIDATION_ERROR", "Partition values must be scalar", {
          column,
        });
      }
      values[column] = String(raw);
    }
    const key = partitionBy.map((column) => `${column}=${values[column]}`).join("/");
    const existing = byKey.get(key);
    if (existing) {
      existing.rows.push(row);
    } else {
      byKey.set(key, { key, values, rows: [row] });
    }
  }
  return [...byKey.values()].sort((left, right) => left.key.localeCompare(right.key));
}

function partitionOutputPath(
  prefix: string,
  partitionValues: Record<string, string>,
  partitionBy: string[],
  jobId: string | undefined,
  taskId: string | undefined,
  idempotencyKey: string | undefined,
  ordinal: number,
): string {
  const segments = [prefix];
  for (const column of partitionBy) {
    segments.push(`${column}=${encodeURIComponent(partitionValues[column] ?? "")}`);
  }
  const safeJobId = jobId ?? "data";
  const filenameParts = [safeJobId];
  if (taskId !== undefined) filenameParts.push(encodeURIComponent(taskId));
  if (idempotencyKey !== undefined) filenameParts.push(encodeURIComponent(idempotencyKey));
  filenameParts.push(String(ordinal).padStart(5, "0"));
  segments.push(`part-${filenameParts.join("-")}.parquet`);
  return segments.join("/");
}

function validateOptionalOutputPathComponent(name: string, value: string | undefined): void {
  if (value === undefined) return;
  if (typeof value !== "string" || value.trim() === "") {
    throw new LaQLError("LAQL_TYPE_ERROR", `${name} must be a non-empty string`, { [name]: value });
  }
}

function validateInsertRows(rows: Row[], validation: InsertValidationRules | undefined): void {
  if (!validation) return;
  for (const column of validation.required ?? []) {
    const missingIndex = rows.findIndex((row) => row[column] === null || row[column] === undefined);
    if (missingIndex !== -1) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Required column is missing", {
        column,
        rowIndex: missingIndex,
      });
    }
  }
  for (const uniqueColumns of validation.unique ?? []) validateUniqueRows(rows, uniqueColumns);
  for (const [column, range] of Object.entries(validation.ranges ?? {})) {
    validateRange(rows, column, range);
  }
  for (const [column, values] of Object.entries(validation.enums ?? {})) {
    validateEnum(rows, column, values);
  }
}

function validateUniqueRows(rows: Row[], columns: string[]): void {
  if (columns.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Unique constraints must name columns");
  }
  const seen = new Map<string, number>();
  for (const [rowIndex, row] of rows.entries()) {
    const key = columns
      .map((column) => insertValueKey(normalizeInsertValue(row[column], column)))
      .join("|");
    const existing = seen.get(key);
    if (existing !== undefined) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Unique constraint violation", {
        columns,
        firstRowIndex: existing,
        rowIndex,
      });
    }
    seen.set(key, rowIndex);
  }
}

function validateRange(
  rows: Row[],
  column: string,
  range: { min?: ComparableInsertValue; max?: ComparableInsertValue },
): void {
  for (const [rowIndex, row] of rows.entries()) {
    const value = row[column];
    if (value === null || value === undefined) continue;
    if (!isComparableInsertValue(value)) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Range constraints require comparable values", {
        column,
        rowIndex,
      });
    }
    if (
      (range.min !== undefined && compareInsertValues(value, range.min, column, rowIndex) < 0) ||
      (range.max !== undefined && compareInsertValues(value, range.max, column, rowIndex) > 0)
    ) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Range constraint violation", {
        column,
        rowIndex,
      });
    }
  }
}

function validateEnum(rows: Row[], column: string, values: InsertValue[]): void {
  const allowed = new Set(values.map((value) => insertValueKey(value)));
  for (const [rowIndex, row] of rows.entries()) {
    const raw = row[column];
    if (raw === undefined) continue;
    const value = normalizeInsertValue(raw, column);
    if (!allowed.has(insertValueKey(value))) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Enum constraint violation", {
        column,
        rowIndex,
      });
    }
  }
}

function normalizeInsertValue(value: unknown, column: string): InsertValue {
  if (value === null) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  throw new LaQLError("LAQL_VALIDATION_ERROR", "Insert constraints require scalar values", {
    column,
  });
}

function insertValueKey(value: InsertValue): string {
  return `${typeof value}:${String(value)}`;
}

function isComparableInsertValue(value: unknown): value is ComparableInsertValue {
  return typeof value === "string" || typeof value === "number" || typeof value === "bigint";
}

function compareInsertValues(
  left: ComparableInsertValue,
  right: ComparableInsertValue,
  column: string,
  rowIndex: number,
): number {
  if (typeof left !== typeof right) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Range constraint types must match row values", {
      column,
      rowIndex,
    });
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function sortStringRecord(record: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of Object.keys(record).sort()) out[key] = record[key] ?? "";
  return out;
}

function splitRowsForFileSize(
  rows: Row[],
  partitionBy: string[],
  columnTypes: Record<string, BasicType>,
  writeOptions: Omit<WriteParquetOptions, "columnData" | "contentType">,
  maxBytesPerFile: number | undefined,
): EncodedRowChunk[] {
  const bytes = encodeRows(rows, partitionBy, columnTypes, writeOptions);
  if (maxBytesPerFile === undefined || bytes.byteLength <= maxBytesPerFile || rows.length === 1) {
    return [{ rows, bytes }];
  }
  const mid = Math.ceil(rows.length / 2);
  return [
    ...splitRowsForFileSize(
      rows.slice(0, mid),
      partitionBy,
      columnTypes,
      writeOptions,
      maxBytesPerFile,
    ),
    ...splitRowsForFileSize(
      rows.slice(mid),
      partitionBy,
      columnTypes,
      writeOptions,
      maxBytesPerFile,
    ),
  ];
}

function encodeRows(
  rows: Row[],
  partitionBy: string[],
  columnTypes: Record<string, BasicType>,
  writeOptions: Omit<WriteParquetOptions, "columnData" | "contentType">,
): Uint8Array {
  return encodeParquetBytes({
    ...writeOptions,
    columnData: rowsToColumnData(rows, partitionBy, columnTypes),
  });
}

function rowsToColumnData(
  rows: Row[],
  partitionBy: string[],
  columnTypes: Record<string, BasicType>,
): ColumnSource[] {
  const partitionColumns = new Set(partitionBy);
  const columns = [
    ...new Set(rows.flatMap((row) => Object.keys(row).filter((key) => !partitionColumns.has(key)))),
  ].sort();
  if (columns.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "At least one non-partition column is required");
  }

  return columns.map((name) => {
    const data = rows.map((row) => normalizeColumnValue(row[name], name));
    return {
      name,
      data,
      type: columnTypes[name] ?? inferColumnType(name, data),
      nullable: data.some((value) => value === null),
    };
  });
}

function normalizeColumnValue(value: unknown, column: string): ColumnValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Numeric column values must be finite", {
        column,
      });
    }
    return value;
  }
  throw new LaQLError("LAQL_VALIDATION_ERROR", "Column values must be scalar", { column });
}

function inferColumnType(column: string, data: ColumnValue[]): BasicType {
  const values = data.filter((value) => value !== null);
  if (values.length === 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Cannot infer type for all-null column", {
      column,
    });
  }
  const kinds = new Set(values.map((value) => typeof value));
  if (kinds.size !== 1) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Column values must have one scalar type", {
      column,
    });
  }
  const kind = kinds.values().next().value;
  switch (kind) {
    case "boolean":
      return "BOOLEAN";
    case "bigint":
      return "INT64";
    case "string":
      return "STRING";
    case "number":
      return data.every((value) => value === null || isInt32Value(value)) ? "INT32" : "DOUBLE";
    default:
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Unsupported column value type", { column });
  }
}

function isInt32Value(value: ColumnValue): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= -2147483648 &&
    value <= 2147483647
  );
}

function isPartitionValue(value: unknown): value is string | number | boolean | bigint {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  );
}

function encodeParquetBytes(options: Omit<WriteParquetOptions, "contentType">): Uint8Array {
  return new Uint8Array(parquetWriteBuffer(options));
}

async function writeEncodedParquet(
  store: ObjectStore,
  path: string,
  bytes: Uint8Array,
  contentType: string | undefined,
  writeMode: WriteParquetOptions["writeMode"],
): Promise<{ path: string; byteSize: number; etag?: string }> {
  try {
    return await putParquetBytes(store, path, bytes, contentType, writeMode);
  } catch (cause) {
    if (cause instanceof LaQLError) throw cause;
    throw new LaQLError("LAQL_PARQUET_WRITE_ERROR", `Failed to write ${path}`, { path, cause });
  }
}

async function putParquetBytes(
  store: ObjectStore,
  path: string,
  bytes: Uint8Array,
  contentType: string | undefined,
  writeMode: WriteParquetOptions["writeMode"] = "overwrite",
): Promise<{ path: string; byteSize: number; etag?: string }> {
  if (writeMode === "create" && (await store.head(path)) !== null) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Parquet output already exists", { path });
  }
  await store.put(path, bytes, {
    contentType: contentType ?? "application/vnd.apache.parquet",
  });
  const head = await store.head(path);
  const result: { path: string; byteSize: number; etag?: string } = {
    path,
    byteSize: head?.size ?? bytes.byteLength,
  };
  if (head?.etag !== undefined) result.etag = head.etag;
  return result;
}

async function contentHash(bytes: Uint8Array): Promise<string> {
  const source = new Uint8Array(bytes.byteLength);
  source.set(bytes);
  const digest = await crypto.subtle.digest("SHA-256", source.buffer);
  const hex = [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return `sha256:${hex}`;
}

function validateWriteMode(writeMode: WriteParquetOptions["writeMode"]): void {
  if (writeMode !== undefined && writeMode !== "overwrite" && writeMode !== "create") {
    throw new LaQLError("LAQL_TYPE_ERROR", "writeMode must be overwrite or create", { writeMode });
  }
}

function decodePositionDeleteRows(
  rows: Record<string, unknown>[],
): { path: string; position: number }[] {
  return rows.map((row, rowIndex) => {
    const path = row.file_path;
    const position = row.pos;
    if (typeof path !== "string" || path.length === 0) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg position delete file_path is invalid", {
        rowIndex,
        path,
      });
    }
    const numericPosition =
      typeof position === "bigint"
        ? Number(position)
        : typeof position === "number"
          ? position
          : Number.NaN;
    if (
      !Number.isSafeInteger(numericPosition) ||
      numericPosition < 0 ||
      (typeof position === "bigint" && BigInt(numericPosition) !== position)
    ) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg position delete pos is invalid", {
        rowIndex,
        position,
      });
    }
    return { path, position: numericPosition };
  });
}

function decodeEqualityDeleteRows(
  rows: Record<string, unknown>[],
): { columns: string[]; row: Row }[] {
  return rows.map((row, rowIndex) => {
    const equalityRow: Row = {};
    for (const [column, value] of Object.entries(row)) {
      if (column.startsWith("_")) continue;
      if (!isIcebergEqualityValue(value)) {
        throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg equality delete value is invalid", {
          rowIndex,
          column,
        });
      }
      equalityRow[column] = value;
    }
    const columns = Object.keys(equalityRow).sort();
    if (columns.length === 0) {
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg equality delete requires columns", {
        rowIndex,
      });
    }
    return { columns, row: equalityRow };
  });
}

function isIcebergEqualityValue(value: unknown): value is Row[string] {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  );
}

type StatsValue = string | number | bigint | boolean;

/** @internal Exposed for pruning tests; not part of the stable public API. */
export function rowGroupMayMatch(rowGroup: RowGroup, expr: Expr | undefined): boolean {
  if (!expr) return true;
  switch (expr.kind) {
    case "literal":
    case "column":
    case "null-check":
    case "like":
      return true;
    case "call":
      return callMayMatch(rowGroup, expr);
    case "not":
      return true;
    case "logical":
      if (expr.op === "and")
        return expr.operands.every((operand) => rowGroupMayMatch(rowGroup, operand));
      return expr.operands.some((operand) => rowGroupMayMatch(rowGroup, operand));
    case "compare":
      return compareMayMatch(rowGroup, expr);
    case "in":
      return inMayMatch(rowGroup, expr);
    case "between":
      return betweenMayMatch(rowGroup, expr);
  }
}

function compareMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "compare" }>): boolean {
  const pair = columnLiteralPair(expr.left, expr.right);
  if (!pair) return true;
  const stats = columnStats(rowGroup, pair.column);
  if (!stats) return true;
  const { min, max } = stats;
  const value = pair.value;
  if (!sameComparableType(min, value) || !sameComparableType(max, value)) return true;
  switch (expr.op) {
    case "eq":
      return compareValues(value, min) >= 0 && compareValues(value, max) <= 0;
    case "ne":
      return !(compareValues(min, value) === 0 && compareValues(max, value) === 0);
    case "lt":
      return compareValues(min, value) < 0;
    case "lte":
      return compareValues(min, value) <= 0;
    case "gt":
      return compareValues(max, value) > 0;
    case "gte":
      return compareValues(max, value) >= 0;
  }
}

function inMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "in" }>): boolean {
  if (expr.negated) return true;
  if (expr.target.kind !== "column") return true;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats) return true;
  return expr.values.some((valueExpr) => {
    if (valueExpr.kind !== "literal" || valueExpr.value === null) return true;
    const value = valueExpr.value;
    if (
      !isStatsValue(value) ||
      !sameComparableType(stats.min, value) ||
      !sameComparableType(stats.max, value)
    ) {
      return true;
    }
    return compareValues(value, stats.min) >= 0 && compareValues(value, stats.max) <= 0;
  });
}

function betweenMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "between" }>): boolean {
  if (
    expr.target.kind !== "column" ||
    expr.low.kind !== "literal" ||
    expr.high.kind !== "literal"
  ) {
    return true;
  }
  if (!isStatsValue(expr.low.value) || !isStatsValue(expr.high.value)) return true;
  const stats = columnStats(rowGroup, expr.target.name);
  if (!stats) return true;
  if (
    !sameComparableType(stats.min, expr.low.value) ||
    !sameComparableType(stats.max, expr.high.value)
  ) {
    return true;
  }
  return (
    compareValues(stats.max, expr.low.value) >= 0 && compareValues(stats.min, expr.high.value) <= 0
  );
}

function callMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  if (expr.fn === "st_intersects") return stIntersectsMayMatch(rowGroup, expr);
  if (expr.fn === "h3_in") return h3InMayMatch(rowGroup, expr);
  if (expr.fn === "h3_within") return h3WithinMayMatch(rowGroup, expr);
  return true;
}

function stIntersectsMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, query] = expr.args;
  if (target?.kind !== "column") return true;
  const queryBBox = bboxLiteral(query);
  if (!queryBBox) return true;
  const groupBBox = rowGroupBBox(rowGroup, target.name);
  if (!groupBBox) return true;
  return (
    groupBBox.maxx >= queryBBox.minx &&
    groupBBox.minx <= queryBBox.maxx &&
    groupBBox.maxy >= queryBBox.miny &&
    groupBBox.miny <= queryBBox.maxy
  );
}

function h3InMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, values] = expr.args;
  if (target?.kind !== "column") return true;
  const cells = h3CellList(values);
  if (!cells) return true;
  const stats = columnStats(rowGroup, target.name);
  if (!stats || typeof stats.min !== "string" || typeof stats.max !== "string") return true;
  return cells.some((cell) => cell >= stats.min && cell <= stats.max);
}

function h3WithinMayMatch(rowGroup: RowGroup, expr: Extract<Expr, { kind: "call" }>): boolean {
  const [target, origin, radius] = expr.args;
  if (
    target?.kind !== "column" ||
    origin?.kind !== "literal" ||
    typeof origin.value !== "string" ||
    radius?.kind !== "literal" ||
    radius.value !== 0
  ) {
    return true;
  }
  return h3InMayMatch(rowGroup, {
    kind: "call",
    fn: "h3_in",
    args: [target, { kind: "literal", value: JSON.stringify([origin.value]) }],
  });
}

function bboxLiteral(
  expr: Expr | undefined,
): { minx: number; miny: number; maxx: number; maxy: number } | undefined {
  if (expr?.kind === "call" && expr.fn === "st_bbox" && expr.args.length === 4) {
    const values = expr.args.map((arg) => (arg.kind === "literal" ? arg.value : undefined));
    const bbox = numberTuple4(values);
    if (bbox) {
      const [minx, miny, maxx, maxy] = bbox;
      if (minx <= maxx && miny <= maxy) return { minx, miny, maxx, maxy };
    }
  }
  if (expr?.kind === "literal" && typeof expr.value === "string") {
    try {
      const parsed = JSON.parse(expr.value) as unknown;
      const bbox = numberTuple4(parsed);
      if (bbox) {
        const [minx, miny, maxx, maxy] = bbox;
        if (minx <= maxx && miny <= maxy) return { minx, miny, maxx, maxy };
      }
    } catch {
      return undefined;
    }
  }
  return undefined;
}

function numberTuple4(value: unknown): [number, number, number, number] | undefined {
  if (!Array.isArray(value) || value.length !== 4) {
    return undefined;
  }
  const [first, second, third, fourth] = value;
  if (
    typeof first !== "number" ||
    typeof second !== "number" ||
    typeof third !== "number" ||
    typeof fourth !== "number"
  ) {
    return undefined;
  }
  return [first, second, third, fourth];
}

function h3CellList(expr: Expr | undefined): string[] | undefined {
  if (expr?.kind !== "literal" || typeof expr.value !== "string") return undefined;
  try {
    const parsed = JSON.parse(expr.value) as unknown;
    if (Array.isArray(parsed) && parsed.every((value) => typeof value === "string")) {
      return parsed;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function rowGroupBBox(
  rowGroup: RowGroup,
  geometryColumn: string,
): { minx: number; miny: number; maxx: number; maxy: number } | undefined {
  const candidates = [
    {
      minx: `${geometryColumn}_minx`,
      miny: `${geometryColumn}_miny`,
      maxx: `${geometryColumn}_maxx`,
      maxy: `${geometryColumn}_maxy`,
    },
    { minx: "minx", miny: "miny", maxx: "maxx", maxy: "maxy" },
  ];
  for (const columns of candidates) {
    const minx = numericColumnStats(rowGroup, columns.minx)?.min;
    const miny = numericColumnStats(rowGroup, columns.miny)?.min;
    const maxx = numericColumnStats(rowGroup, columns.maxx)?.max;
    const maxy = numericColumnStats(rowGroup, columns.maxy)?.max;
    if (minx !== undefined && miny !== undefined && maxx !== undefined && maxy !== undefined) {
      return { minx, miny, maxx, maxy };
    }
  }
  return undefined;
}

function numericColumnStats(
  rowGroup: RowGroup,
  column: string,
): { min: number; max: number } | undefined {
  const stats = columnStats(rowGroup, column);
  if (!stats || !isNumberLike(stats.min) || !isNumberLike(stats.max)) return undefined;
  return { min: Number(stats.min), max: Number(stats.max) };
}

function columnLiteralPair(
  left: Expr,
  right: Expr,
): { column: string; value: StatsValue } | undefined {
  if (left.kind === "column" && right.kind === "literal" && isStatsValue(right.value)) {
    return { column: left.name, value: right.value };
  }
  if (right.kind === "column" && left.kind === "literal" && isStatsValue(left.value)) {
    return { column: right.name, value: left.value };
  }
  return undefined;
}

function columnStats(
  rowGroup: RowGroup,
  column: string,
): { min: StatsValue; max: StatsValue } | undefined {
  for (const chunk of rowGroup.columns) {
    const metadata = chunk.meta_data;
    if (!metadata || metadata.path_in_schema.join(".") !== column) continue;
    const min = metadata.statistics?.min_value;
    const max = metadata.statistics?.max_value;
    if (isStatsValue(min) && isStatsValue(max)) return { min, max };
  }
  return undefined;
}

function isStatsValue(value: unknown): value is StatsValue {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  );
}

function sameComparableType(left: StatsValue, right: StatsValue): boolean {
  if (typeof left === typeof right) return true;
  return isNumberLike(left) && isNumberLike(right);
}

function isNumberLike(value: StatsValue): value is number | bigint {
  return typeof value === "number" || typeof value === "bigint";
}

function compareValues(left: StatsValue, right: StatsValue): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

export function parquetScanner(
  store: ObjectStore,
  options: { batchSize?: number; metadataCache?: CacheAdapter<ParquetMetadata> } = {},
): ScanAdapter {
  return new ParquetScanAdapter(store, options);
}

export function createParquetLake(config: ParquetLakeConfig): Lake {
  const scannerOptions: { batchSize?: number; metadataCache?: CacheAdapter<ParquetMetadata> } = {};
  if (config.batchSize !== undefined) scannerOptions.batchSize = config.batchSize;
  if (config.metadataCache !== undefined) scannerOptions.metadataCache = config.metadataCache;
  return new Lake({
    ...config,
    scanner: parquetScanner(config.store, scannerOptions),
  });
}
