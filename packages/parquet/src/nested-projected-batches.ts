import type { ColumnDecoder, ColumnMetaData, SchemaTree } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type OwnedMemory,
  type Vector,
} from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import { canReadNestedColumn } from "./nested-column-capability.js";
import {
  type NestedPageCacheOptions,
  readNestedCompactColumnPages,
} from "./nested-column-pages.js";
import { readNestedColumnRows } from "./nested-column-rows.js";
import { readAlignedNestedRows } from "./nested-row-alignment.js";
import { validateNestedRowWindow } from "./nested-row-window.js";
import { readNestedSchemaBatches } from "./nested-schema-batches.js";
import { compileNestedProjection } from "./nested-schema-plan.js";
import { validateParquetReadConcurrency } from "./read-concurrency.js";
import type { StoreAsyncBuffer } from "./types.js";

export interface NestedProjectedColumn {
  metadata: ColumnMetaData;
  decoder: ColumnDecoder;
}

/** One projected row group. Metadata/schema are caller-owned; only selected
 * columns open streams. Output leases follow the nested batch iterator contract.
 */
export async function* readNestedProjectedBatches(
  file: StoreAsyncBuffer,
  root: SchemaTree,
  columns: readonly NestedProjectedColumn[],
  names: readonly string[],
  scope: MemoryScope,
  options: NestedPageCacheOptions & {
    rowStart: number;
    rowEnd: number;
    batchSize: number;
    maxConcurrentReads?: number;
  },
): AsyncGenerator<OwnedMemory<Vector>> {
  parquetBatchSize(options.batchSize);
  validateParquetReadConcurrency(options);
  validateNestedRowWindow(options.rowStart, options.rowEnd);
  if (options.rowStart === options.rowEnd) return;
  const lifetime = scope.childScope();
  try {
    let count = 0;
    for (const column of columns) {
      if (!names.includes(column.metadata.path_in_schema[0] ?? "")) continue;
      if (!canReadNestedColumn(column.metadata, column.decoder))
        throw new LakeqlError(
          "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
          "Unsupported nested projected column",
        );
      count++;
    }
    const paths = lifetime.allocate(
      c.array + count * c.arraySlot,
      "projected leaf paths",
      () => [] as ColumnDecoder["schemaPath"][],
    ).value;
    const sources = lifetime.allocate(
      c.array + count * c.arraySlot,
      "projected column streams",
      () => [] as ReturnType<typeof readNestedColumnRows>[],
    ).value;
    for (const column of columns) {
      if (!names.includes(column.metadata.path_in_schema[0] ?? "")) continue;
      paths.push(column.decoder.schemaPath);
      sources.push(
        readNestedColumnRows(
          readNestedCompactColumnPages(file, column.metadata, column.decoder, lifetime, options),
          lifetime,
          options.batchSize,
          options,
        ),
      );
    }
    const plan = compileNestedProjection(root, paths, names, lifetime);
    const aligned = readAlignedNestedRows(sources, lifetime, options.maxConcurrentReads ?? 4);
    yield* readNestedSchemaBatches(aligned, plan, paths, lifetime, options);
  } finally {
    lifetime.dispose();
  }
}
