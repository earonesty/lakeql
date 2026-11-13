import type { RowGroup } from "hyparquet";
import type { Expr, MemoryBudget } from "lakeql-core";
import { dictionaryPageOffset } from "./dictionary-offset.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import type { ParquetMetadata } from "./types.js";

interface RowGroupPlanOptions {
  includeByteRanges?: boolean;
  memoryBudget?: MemoryBudget | undefined;
}

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
  return planRowGroupsFromSource(metadata, metadata.row_groups, where);
}

/** Consume one decoded group at a time; retained output contains plan entries only. */
export function planRowGroupsFromSource(
  metadata: Pick<ParquetMetadata, "schema">,
  groups: Iterable<RowGroup | undefined>,
  where: Expr | undefined,
  options: RowGroupPlanOptions = {},
): ParquetRowGroupPlan {
  const rowGroups: PlannedParquetRowGroup[] = [];
  const ranges: { start: number; end: number }[] = [];
  for (const planned of iteratePlannedRowGroups(metadata, groups, where, options)) {
    rowGroups.push(planned);
    appendRowGroupRange(ranges, planned.index);
  }
  return { rowGroups, rowGroupRanges: ranges };
}

/** Task planning needs merged group ranges and a count, not retained per-group entries. */
export function summarizePlannedRowGroups(groups: Iterable<PlannedParquetRowGroup>) {
  const rowGroupRanges: { start: number; end: number }[] = [];
  let plannedCount = 0;
  for (const group of groups) {
    plannedCount++;
    appendRowGroupRange(rowGroupRanges, group.index);
  }
  return { plannedCount, rowGroupRanges };
}

function appendRowGroupRange(ranges: { start: number; end: number }[], index: number): void {
  const previous = ranges.at(-1);
  if (previous && previous.end === index) previous.end = index + 1;
  else ranges.push({ start: index, end: index + 1 });
}

/** Demand-driven planning; consumers determine whether output entries are retained. */
export function* iteratePlannedRowGroups(
  metadata: Pick<ParquetMetadata, "schema">,
  groups: Iterable<RowGroup | undefined>,
  where: Expr | undefined,
  options: RowGroupPlanOptions = {},
): Generator<PlannedParquetRowGroup> {
  rejectUnsupportedParquetSchema(metadata, {
    allowValueCheckedScalarDecimals: true,
    memoryBudget: options.memoryBudget,
  });
  let rowStart = 0;
  let nextIndex = 0;
  for (const rowGroup of groups) {
    const index = nextIndex++;
    const rowCount = rowGroup === undefined ? 0 : Number(rowGroup.num_rows);
    const nextRowStart = rowStart + rowCount;
    if (rowGroup !== undefined && rowGroupMayMatch(rowGroup, where)) {
      const planned: PlannedParquetRowGroup = { index, rowStart, rowCount };
      const byteRange =
        options.includeByteRanges === false ? undefined : rowGroupByteRange(rowGroup);
      if (byteRange !== undefined) planned.byteRange = byteRange;
      yield planned;
    }
    rowStart = nextRowStart;
  }
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
    const pageOffset = safeNumber(dictionaryPageOffset(metadata) ?? metadata.data_page_offset);
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
