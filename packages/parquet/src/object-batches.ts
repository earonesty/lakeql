import { parquetReadObjects, parquetSchema } from "hyparquet";
import { normalizeIntegerDecimalRows, rawIntegerDecimalMetadata } from "./decimal.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { lakeqlParquetParsers } from "./parsers.js";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import type {
  ParquetMetadata,
  ParquetRowBatch,
  ReadParquetBatchOptions,
  StoreAsyncBuffer,
} from "./types.js";

export async function* readParquetObjectBatchesFromFile(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetRowBatch> {
  const batchSize = options.batchSize ?? 4096;
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  const columns =
    options.columns ?? parquetSchema(metadata).children.map((child) => child.element.name);
  if (options.columns !== undefined) recordReadColumns(options.stats, options.columns);
  let rowGroupStart = 0;
  for (const rowGroup of metadata.row_groups) {
    const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
    if (
      rowGroupEnd <= requestedStart ||
      rowGroupStart >= requestedEnd ||
      !rowGroupMayMatch(rowGroup, options.where)
    ) {
      recordRowGroupSkipped(options.stats);
      rowGroupStart = rowGroupEnd;
      continue;
    }
    recordRowGroupRead(options.stats);
    const start = Math.max(rowGroupStart, requestedStart);
    const end = Math.min(rowGroupEnd, requestedEnd);
    for (let rowStart = start; rowStart < end; rowStart += batchSize) {
      const rowEnd = Math.min(rowStart + batchSize, end);
      const readOptions: Parameters<typeof parquetReadObjects>[0] = {
        file,
        metadata: rawIntegerDecimalMetadata(metadata, columns),
        rowFormat: "object",
        rowStart,
        rowEnd,
        parsers: lakeqlParquetParsers,
      };
      if (options.columns) readOptions.columns = options.columns;
      const rows = normalizeDecodedRows(
        normalizeIntegerDecimalRows(metadata, columns, await parquetReadObjects(readOptions)),
      );
      recordRowsDecoded(options.stats, rows.length);
      yield {
        rowOffset: rowStart,
        rows,
      };
    }
    rowGroupStart = rowGroupEnd;
  }
}
