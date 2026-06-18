import type { BasicType, ColumnSource, ParquetWriteOptions } from "hyparquet-writer";
import { parquetWriteBuffer } from "hyparquet-writer";
import {
  advanceTaskCheckpoint,
  type CacheAdapter,
  type CheckpointAdapter,
  cachedObjectStore,
  createOutputManifest,
  type Expr,
  Lake,
  type LakeConfig,
  LakeqlError,
  memoryCache,
  type ObjectInfo,
  type ObjectStore,
  type ObjectStoreCacheOptions,
  type OutputManifest,
  type OutputManifestEntry,
  type Row,
  type ScanAdapter,
  type TaskCheckpoint,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readParquetObjectBatchesFromFile } from "./object-batches.js";
import { type ParquetRowGroupPlan, planRowGroupsFromMetadata } from "./row-group-plan.js";
import { ParquetScanAdapter } from "./scan-adapter.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { readParquetMetadata } from "./task-scan.js";
import type {
  ParquetColumnBatch,
  ParquetMetadata,
  ParquetRowBatch,
  ReadParquetBatchOptions,
  ReadParquetOptions,
} from "./types.js";

export {
  aggregateParquetGroupTask,
  aggregateParquetGroupTasks,
  aggregateParquetGroupTasksBatch,
  aggregateParquetTask,
  aggregateParquetTasks,
} from "./aggregate-task.js";
export type { ParquetRowGroupPlan, PlannedParquetRowGroup } from "./row-group-plan.js";
export { planRowGroupsFromMetadata } from "./row-group-plan.js";
export { rowGroupMayMatch, rowGroupMustMatch } from "./row-group-pruning.js";
export { ParquetScanAdapter } from "./scan-adapter.js";
export { rejectUnsupportedParquetSchema } from "./schema.js";
export { asyncBufferFromStore } from "./store-buffer.js";
export type {
  AggregateParquetGroupTaskOptions,
  AggregateParquetGroupTasksOptions,
  AggregateParquetTaskOptions,
  AggregateParquetTasksOptions,
  PlanParquetTaskWorkUnitsOptions,
  ScanParquetTaskOptions,
} from "./task.js";
export {
  planParquetTaskWorkUnits,
  readParquetMetadata,
  scanParquetTaskBatches,
  scanParquetTaskColumnBatches,
} from "./task-scan.js";
export type {
  ParquetColumnBatch,
  ParquetMetadata,
  ParquetRowBatch,
  ReadParquetBatchOptions,
  ReadParquetOptions,
} from "./types.js";

export interface PlanParquetRowGroupsOptions {
  where?: Expr;
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
  cache?: ObjectStoreCacheOptions;
  metadataCache?: CacheAdapter<ParquetMetadata>;
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
  const rows: Row[] = [];
  for await (const batch of readParquetObjectBatches(store, path, options)) {
    rows.push(...batch.rows);
  }
  return rows;
}

export async function* readParquetObjectBatches(
  store: ObjectStore,
  path: string,
  options: ReadParquetBatchOptions = {},
): AsyncIterable<ParquetRowBatch> {
  const file = await asyncBufferFromStore(store, path);
  try {
    const metadata = await readParquetMetadataFromFile(file);
    rejectUnsupportedParquetSchema(metadata);
    yield* readParquetObjectBatchesFromFile(file, metadata, options);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
  }
}

export async function* readParquetColumnBatches(
  store: ObjectStore,
  path: string,
  options: ReadParquetBatchOptions = {},
): AsyncIterable<ParquetColumnBatch> {
  const file = await asyncBufferFromStore(store, path);
  try {
    const metadata = await readParquetMetadataFromFile(file);
    rejectUnsupportedParquetSchema(metadata);
    yield* readParquetColumnBatchesFromFile(file, metadata, options);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${path}`, { path, cause });
  }
}

export async function planRowGroups(
  store: ObjectStore,
  path: string,
  options: PlanParquetRowGroupsOptions = {},
): Promise<ParquetRowGroupPlan> {
  const metadata = await readParquetMetadata(store, path);
  return planRowGroupsFromMetadata(metadata, options.where);
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
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_DELETE_FILES",
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
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_WRITE_ERROR", `Failed to write ${path}`, { path, cause });
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
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Parquet output prefix must be non-empty");
  }
  if (options.rows.length === 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot write an empty row set");
  }
  if (!Number.isInteger(maxRowsPerFile) || maxRowsPerFile < 1) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "maxRowsPerFile must be a positive integer", {
      maxRowsPerFile,
    });
  }
  if (
    options.maxBytesPerFile !== undefined &&
    (!Number.isInteger(options.maxBytesPerFile) || options.maxBytesPerFile < 1)
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "maxBytesPerFile must be a positive integer", {
      maxBytesPerFile: options.maxBytesPerFile,
    });
  }
  validateOptionalOutputPathComponent("taskId", options.taskId);
  validateOptionalOutputPathComponent("idempotencyKey", options.idempotencyKey);
  const partitionBy = options.partitionBy ?? [];
  const uniquePartitions = new Set(partitionBy);
  if (uniquePartitions.size !== partitionBy.length) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "partitionBy columns must be unique", {
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
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Partition values must be scalar", {
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
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${name} must be a non-empty string`, {
      [name]: value,
    });
  }
}

function validateInsertRows(rows: Row[], validation: InsertValidationRules | undefined): void {
  if (!validation) return;
  for (const column of validation.required ?? []) {
    const missingIndex = rows.findIndex((row) => row[column] === null || row[column] === undefined);
    if (missingIndex !== -1) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Required column is missing", {
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
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Unique constraints must name columns");
  }
  const seen = new Map<string, number>();
  for (const [rowIndex, row] of rows.entries()) {
    const key = columns
      .map((column) => insertValueKey(normalizeInsertValue(row[column], column)))
      .join("|");
    const existing = seen.get(key);
    if (existing !== undefined) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Unique constraint violation", {
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
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Range constraints require comparable values",
        {
          column,
          rowIndex,
        },
      );
    }
    if (
      (range.min !== undefined && compareInsertValues(value, range.min, column, rowIndex) < 0) ||
      (range.max !== undefined && compareInsertValues(value, range.max, column, rowIndex) > 0)
    ) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Range constraint violation", {
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
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Enum constraint violation", {
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
  throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Insert constraints require scalar values", {
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
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Range constraint types must match row values",
      {
        column,
        rowIndex,
      },
    );
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
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "At least one non-partition column is required",
    );
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
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Numeric column values must be finite", {
        column,
      });
    }
    return value;
  }
  throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Column values must be scalar", { column });
}

function inferColumnType(column: string, data: ColumnValue[]): BasicType {
  const values = data.filter((value) => value !== null);
  if (values.length === 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot infer type for all-null column", {
      column,
    });
  }
  const kinds = new Set(values.map((value) => typeof value));
  if (kinds.size !== 1) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Column values must have one scalar type", {
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
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Unsupported column value type", { column });
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
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_WRITE_ERROR", `Failed to write ${path}`, { path, cause });
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
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Parquet output already exists", { path });
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
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "writeMode must be overwrite or create", {
      writeMode,
    });
  }
}

function decodePositionDeleteRows(
  rows: Record<string, unknown>[],
): { path: string; position: number }[] {
  return rows.map((row, rowIndex) => {
    const path = row.file_path;
    const position = row.pos;
    if (typeof path !== "string" || path.length === 0) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Iceberg position delete file_path is invalid",
        {
          rowIndex,
          path,
        },
      );
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
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Iceberg position delete pos is invalid", {
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
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Iceberg equality delete value is invalid",
          {
            rowIndex,
            column,
          },
        );
      }
      equalityRow[column] = value;
    }
    const columns = Object.keys(equalityRow).sort();
    if (columns.length === 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Iceberg equality delete requires columns", {
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
  else if (config.cache !== undefined)
    scannerOptions.metadataCache = memoryCache<ParquetMetadata>();
  const store =
    config.cache === undefined ? config.store : cachedObjectStore(config.store, config.cache);
  const planningCache =
    config.planningCache ?? (config.cache === undefined ? undefined : memoryCache<ObjectInfo[]>());
  return new Lake({
    ...config,
    store,
    ...(planningCache !== undefined ? { planningCache } : {}),
    scanner: parquetScanner(store, scannerOptions),
  });
}
