import { LaQLError, type ObjectStore } from "@laql/core";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";

export interface ReadParquetOptions {
  /** Columns to project; all columns when omitted. */
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
}

/**
 * Bridge an ObjectStore path to hyparquet's AsyncBuffer (length + ranged slice).
 */
export async function asyncBufferFromStore(store: ObjectStore, path: string) {
  const head = await store.head(path);
  if (!head) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
  }
  return {
    byteLength: head.size,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const length = (end ?? head.size) - start;
      const bytes = await store.getRange(path, { offset: start, length });
      const out = new ArrayBuffer(bytes.byteLength);
      new Uint8Array(out).set(bytes);
      return out;
    },
  };
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
