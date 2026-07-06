import {
  type Batch,
  batchExprValues,
  materializeBatchRow,
  type Selection,
  selectedRowIndices,
} from "./batch.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import type { WindowExecutionOptions } from "./window-backend.js";
import { partitionSpans, sortWindowRows, type WindowRow } from "./window-utils.js";

export interface VectorWindowBatch {
  batch: Batch;
  selection?: Selection;
}

interface VectorWindowEntry {
  batchIndex: number;
  rowIndex: number;
  originalIndex: number;
  row?: Row;
}

export async function applyWindowsToVectorBatches(
  inputs: readonly VectorWindowBatch[],
  windows: Record<string, WindowExpr>,
  options: WindowExecutionOptions = {},
): Promise<Row[]> {
  const { evaluateWindowRows, windowRowsForExpr, windowSortGroups } = await import(
    "./window-backend.js"
  );
  const entries = vectorEntries(inputs);
  options.enforceBufferedRows?.(entries.length);
  const rows = entries.map((entry) => rowForEntry(entry, inputs));
  if (options.estimateMemoryBytes !== undefined) {
    options.enforceMemoryBytes?.(options.estimateMemoryBytes(rows));
  }

  for (const group of windowSortGroups(windows)) {
    const sorted = vectorWindowRows(inputs, entries, group.sortExpr);
    sortWindowRows(sorted, group.sortExpr.over.orderBy);
    for (const { alias, expr } of group.items) {
      const windowRows = windowRowsForExpr(sorted, expr, group.sortExpr);
      const spans = partitionSpans(windowRows);
      const values = evaluateWindowRows(rows.length, windowRows, spans, expr, options);
      for (let index = 0; index < rows.length; index += 1) {
        const row = rows[index] as Row;
        row[alias] = values[index] ?? null;
      }
    }
  }

  return rows;
}

function vectorEntries(inputs: readonly VectorWindowBatch[]): VectorWindowEntry[] {
  const entries: VectorWindowEntry[] = [];
  for (let batchIndex = 0; batchIndex < inputs.length; batchIndex += 1) {
    const input = inputs[batchIndex] as VectorWindowBatch;
    for (const rowIndex of selectedRowIndices(input.batch.rowCount, input.selection)) {
      entries.push({ batchIndex, rowIndex, originalIndex: entries.length });
    }
  }
  return entries;
}

function vectorWindowRows(
  inputs: readonly VectorWindowBatch[],
  entries: readonly VectorWindowEntry[],
  expr: WindowExpr,
): WindowRow[] {
  const partitionValues = inputs.map((input) =>
    expr.over.partitionBy.map((part) => batchExprValues(input.batch, part)),
  );
  const orderValues = inputs.map((input) =>
    expr.over.orderBy.map((term) => batchExprValues(input.batch, term.expr)),
  );
  return entries.map((entry) => {
    const partition = partitionValues[entry.batchIndex] as ReturnType<typeof batchExprValues>[];
    const order = orderValues[entry.batchIndex] as ReturnType<typeof batchExprValues>[];
    return {
      row: rowForEntry(entry, inputs),
      originalIndex: entry.originalIndex,
      partitionKey: partition.map((values) => values.valueAt(entry.rowIndex)),
      orderKey: order.map((values) => values.valueAt(entry.rowIndex)),
    };
  });
}

function rowForEntry(entry: VectorWindowEntry, inputs: readonly VectorWindowBatch[]): Row {
  if (entry.row === undefined) {
    const input = inputs[entry.batchIndex] as VectorWindowBatch;
    entry.row = materializeBatchRow(input.batch, entry.rowIndex);
  }
  return entry.row;
}
