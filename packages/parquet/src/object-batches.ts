import { parquetReadObjects, parquetSchema } from "hyparquet";
import { scopedParquetCompressors } from "./compressors.js";
import { normalizeDecimalRows, rawDecimalMetadata } from "./decimal.js";
import { normalizeDecodedRows } from "./decoded-rows.js";
import { parquetParsersForMetadata } from "./parsers.js";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { recoverColumnExtents } from "./recover-column-extents.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import type {
  ParquetMetadata,
  ParquetRowBatch,
  ReadParquetBatchOptions,
  StoreAsyncBuffer,
} from "./types.js";
import { parquetValueDecoders } from "./value-decoders.js";

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
  for (let groupIndex = 0; groupIndex < metadata.row_groups.length; groupIndex++) {
    const rowGroup = metadata.row_groups[groupIndex];
    if (rowGroup === undefined) continue;
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
    const recovered = await recoverColumnExtents(
      file,
      metadata,
      { columns },
      options.allocationScope ?? options.decompressionScope,
      () => metadata.row_groups,
      groupIndex,
    );
    for (let rowStart = start; rowStart < end; rowStart += batchSize) {
      const rowEnd = Math.min(rowStart + batchSize, end);
      const readOptions: Parameters<typeof parquetReadObjects>[0] = {
        file,
        compressors: scopedParquetCompressors(
          options.allocationScope ?? options.decompressionScope,
        ),
        metadata: rawDecimalMetadata(recovered, columns),
        rowFormat: "object",
        rowStart,
        rowEnd,
        parsers: parquetParsersForMetadata(
          metadata,
          options.allocationScope ?? options.decompressionScope,
        ),
        valueDecoders: parquetValueDecoders(options.allocationScope ?? options.decompressionScope),
        utf8: false,
      };
      if (options.columns) readOptions.columns = options.columns;
      const rows = normalizeDecodedRows(
        normalizeDecimalRows(metadata, columns, await parquetReadObjects(readOptions)),
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
