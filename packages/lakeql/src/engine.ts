import {
  collectExprColumns,
  type ObjectStore,
  type ObjectStoreReadControls,
  type Row,
} from "lakeql-core";
import {
  type DecodedIcebergDeletes,
  type IcebergPlan,
  type IcebergTable,
  type LoadIcebergTableOptions,
  loadIcebergTable,
  type PlanIcebergFilesOptions,
  planFiles as planIcebergFiles,
  scanPlannedIcebergRows,
} from "lakeql-iceberg";
import {
  type ParquetRowBatch,
  type ReadParquetBatchOptions,
  readIcebergParquetDeletes,
  readParquetObjectBatches,
} from "lakeql-parquet";

export interface LoadIcebergEngineTableOptions extends LoadIcebergTableOptions {
  format: "iceberg";
}

export interface LoadParquetEngineTableOptions {
  format: "parquet";
  store: ObjectStore;
  path: string;
}

export type LoadTableOptions = LoadIcebergEngineTableOptions | LoadParquetEngineTableOptions;

export interface IcebergEngineTable {
  format: "iceberg";
  store: ObjectStore;
  table: IcebergTable;
}

export interface ParquetEngineTable {
  format: "parquet";
  store: ObjectStore;
  path: string;
}

export type EngineTable = IcebergEngineTable | ParquetEngineTable;

export interface IcebergEnginePlan {
  format: "iceberg";
  store: ObjectStore;
  table: IcebergTable;
  plan: IcebergPlan;
  options: PlanIcebergFilesOptions;
}

export interface ParquetEnginePlan {
  format: "parquet";
  store: ObjectStore;
  files: { path: string }[];
}

export type EngineFilePlan = IcebergEnginePlan | ParquetEnginePlan;

export interface ScanEngineOptions extends ObjectStoreReadControls {
  batchSize?: number;
}

export type ScanBatch = Row[];

export async function loadTable(options: LoadTableOptions): Promise<EngineTable> {
  if (options.format === "parquet") {
    return { format: "parquet", store: options.store, path: options.path };
  }

  const { format: _format, ...icebergOptions } = options;
  return {
    format: "iceberg",
    store: options.store,
    table: await loadIcebergTable(icebergOptions),
  };
}

export function planFiles(
  table: IcebergEngineTable,
  options?: PlanIcebergFilesOptions,
): IcebergEnginePlan;
export function planFiles(table: ParquetEngineTable): ParquetEnginePlan;
export function planFiles(table: EngineTable, options?: PlanIcebergFilesOptions): EngineFilePlan;
export function planFiles(
  table: EngineTable,
  options: PlanIcebergFilesOptions = {},
): EngineFilePlan {
  if (table.format === "parquet") {
    return { format: "parquet", store: table.store, files: [{ path: table.path }] };
  }

  return {
    format: "iceberg",
    store: table.store,
    table: table.table,
    plan: planIcebergFiles(table.table, options),
    options,
  };
}

export async function* scanBatches(
  plan: EngineFilePlan,
  options: ScanEngineOptions = {},
): AsyncIterable<ScanBatch> {
  if (plan.format === "parquet") {
    for (const file of plan.files) {
      const readOptions: ReadParquetBatchOptions = {};
      if (options.batchSize !== undefined) readOptions.batchSize = options.batchSize;
      for await (const batch of readParquetObjectBatches(plan.store, file.path, readOptions)) {
        yield batch.rows;
      }
    }
    return;
  }

  for await (const batch of scanPlannedIcebergRows({
    plan: plan.plan,
    ...options,
    readDataFile: async (file, deletes) =>
      projectIcebergParquetBatches(
        plan.store,
        plan.table,
        file.path,
        file.partition,
        file.snapshotId,
        plan.options,
        deletes,
        options,
      ),
    readDeleteFile: async (deleteFile) => readIcebergParquetDeletes(plan.store, deleteFile),
  })) {
    yield batch;
  }
}

export async function* scanRows(
  plan: EngineFilePlan,
  options: ScanEngineOptions = {},
): AsyncIterable<Row> {
  for await (const batch of scanBatches(plan, options)) {
    for (const row of batch) {
      yield row;
    }
  }
}

async function* projectIcebergParquetBatches(
  store: ObjectStore,
  table: IcebergTable,
  path: string,
  partition: Record<string, string>,
  snapshotId: number,
  planOptions: PlanIcebergFilesOptions,
  deletes: DecodedIcebergDeletes,
  scanOptions: ScanEngineOptions,
): AsyncIterable<ParquetRowBatch> {
  const readOptions: ReadParquetBatchOptions = {};
  if (scanOptions.batchSize !== undefined) readOptions.batchSize = scanOptions.batchSize;
  const retainedColumns = retainedFilterColumns(deletes, planOptions);
  for await (const batch of readParquetObjectBatches(store, path, readOptions)) {
    yield {
      rowOffset: batch.rowOffset,
      rows: batch.rows.map((row) => {
        const sourceRow = { ...partition, ...row };
        const projectOptions: Parameters<IcebergTable["projectRow"]>[1] = { snapshotId };
        if (planOptions.select !== undefined) projectOptions.select = planOptions.select;
        const projected = table.projectRow(sourceRow, projectOptions);
        for (const column of retainedColumns) {
          if (column in sourceRow && !(column in projected)) projected[column] = sourceRow[column];
        }
        return projected;
      }),
    };
  }
}

function retainedFilterColumns(
  deletes: DecodedIcebergDeletes,
  planOptions: PlanIcebergFilesOptions,
): Set<string> {
  const columns = new Set<string>();
  if (planOptions.select === undefined) collectExprColumns(planOptions.where, columns);
  for (const deletion of deletes.equalityDeletes ?? []) {
    for (const column of deletion.columns) columns.add(column);
  }
  return columns;
}
