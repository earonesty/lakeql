import type { RowGroup } from "hyparquet";
import type { Expr } from "lakeql-core";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import type { ParquetMetadata } from "./types.js";

export interface PlannedParquetRowGroup {
  index: number;
  rowStart: number;
  rowCount: number;
  byteRange?: { offset: number; length: number };
}

export interface ParquetRowGroupPlan {
  rowGroups: PlannedParquetRowGroup[];
  rowGroupRanges: { start: number; end: number }[];
}

export function planRowGroupsFromMetadata(
  metadata: ParquetMetadata,
  where: Expr | undefined,
): ParquetRowGroupPlan {
  rejectUnsupportedParquetSchema(metadata);
  const rowGroups: PlannedParquetRowGroup[] = [];
  const ranges: { start: number; end: number }[] = [];
  let rowStart = 0;
  for (let index = 0; index < metadata.row_groups.length; index += 1) {
    const rowGroup = metadata.row_groups[index];
    const rowCount = rowGroup === undefined ? 0 : Number(rowGroup.num_rows);
    const nextRowStart = rowStart + rowCount;
    if (rowGroup === undefined || !rowGroupMayMatch(rowGroup, where)) {
      rowStart = nextRowStart;
      continue;
    }
    const planned: PlannedParquetRowGroup = { index, rowStart, rowCount };
    const byteRange = rowGroupByteRange(rowGroup);
    if (byteRange !== undefined) planned.byteRange = byteRange;
    rowGroups.push(planned);
    const previous = ranges.at(-1);
    if (previous && previous.end === index) previous.end = index + 1;
    else ranges.push({ start: index, end: index + 1 });
    rowStart = nextRowStart;
  }
  return { rowGroups, rowGroupRanges: ranges };
}

function rowGroupByteRange(rowGroup: RowGroup): { offset: number; length: number } | undefined {
  const groupOffset = safeNumber(rowGroup.file_offset);
  const groupCompressedSize = safeNumber(rowGroup.total_compressed_size);
  if (groupOffset !== undefined && groupCompressedSize !== undefined && groupCompressedSize > 0) {
    return { offset: groupOffset, length: groupCompressedSize };
  }

  let start: number | undefined;
  let end: number | undefined;
  for (const column of rowGroup.columns) {
    const metadata = column.meta_data;
    if (!metadata) continue;
    const pageOffset = safeNumber(metadata.dictionary_page_offset ?? metadata.data_page_offset);
    const compressedSize = safeNumber(metadata.total_compressed_size);
    if (pageOffset === undefined || compressedSize === undefined || compressedSize < 0) continue;
    start = start === undefined ? pageOffset : Math.min(start, pageOffset);
    end =
      end === undefined ? pageOffset + compressedSize : Math.max(end, pageOffset + compressedSize);
  }
  if (start === undefined || end === undefined || end < start) return undefined;
  return { offset: start, length: end - start };
}

function safeNumber(value: bigint | number | undefined): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (typeof value === "bigint") {
    const numberValue = Number(value);
    if (Number.isSafeInteger(numberValue) && BigInt(numberValue) === value) return numberValue;
  }
  return undefined;
}
