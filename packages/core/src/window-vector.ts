import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import {
  type Batch,
  batchExprValues,
  materializeBatchRow,
  type Selection,
  selectedRowIndices,
} from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
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
  const indexScope = new MemoryScope(
    options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER),
  );
  try {
    const entries = vectorEntries(inputs, indexScope);
    options.enforceBufferedRows?.(entries.length);
    const rows = entries.map((entry) => rowForEntry(entry, inputs));
    if (options.estimateMemoryBytes !== undefined)
      options.enforceMemoryBytes?.(options.estimateMemoryBytes(rows));
    for (const group of windowSortGroups(windows)) {
      const groupScope = indexScope.childScope();
      try {
        const sorted = vectorWindowRows(inputs, entries, group.sortExpr, groupScope);
        sortWindowRows(sorted, group.sortExpr.over.orderBy, groupScope);
        for (const { alias, expr } of group.items) {
          const expressionScope = groupScope.childScope();
          try {
            const windowRows = windowRowsForExpr(sorted, expr, group.sortExpr, expressionScope);
            const spans = partitionSpans(windowRows, expressionScope);
            const values = evaluateWindowRows(rows.length, windowRows, spans, expr, {
              ...options,
              allocationScope: expressionScope,
            });
            for (let index = 0; index < rows.length; index++) {
              const row = rows[index] as Row;
              row[alias] = values[index] ?? null;
            }
          } finally {
            expressionScope.dispose();
          }
        }
      } finally {
        groupScope.dispose();
      }
    }
    return rows;
  } finally {
    indexScope.dispose();
  }
}

function vectorEntries(
  inputs: readonly VectorWindowBatch[],
  scope: MemoryScope,
): VectorWindowEntry[] {
  const entries = scope.allocate(
    c.array,
    "vector window entries",
    () => [] as VectorWindowEntry[],
  ).value;
  for (let batchIndex = 0; batchIndex < inputs.length; batchIndex += 1) {
    const input = inputs[batchIndex] as VectorWindowBatch;
    for (const rowIndex of selectedRowIndices(input.batch.rowCount, input.selection)) {
      scope.allocate(c.arraySlot + c.object + 4 * c.property, "vector window entry", () => {
        entries.push({ batchIndex, rowIndex, originalIndex: entries.length });
      });
    }
  }
  return entries;
}

function vectorWindowRows(
  inputs: readonly VectorWindowBatch[],
  entries: readonly VectorWindowEntry[],
  expr: WindowExpr,
  scope: MemoryScope,
): WindowRow[] {
  const orderExpressions = scope.allocate(
    c.array + expr.over.orderBy.length * c.arraySlot,
    "vector window order expressions",
    () => expr.over.orderBy.map((term) => term.expr),
  ).value;
  const valuesFor = (expressions: readonly import("./expr.js").Expr[]) =>
    scope.allocate(c.array + inputs.length * c.arraySlot, "vector window key batches", () =>
      inputs.map(
        (input) =>
          scope.allocate(
            c.array + expressions.length * (c.arraySlot + c.object + 4 * c.property),
            "vector window key expressions",
            () => expressions.map((expression) => batchExprValues(input.batch, expression, scope)),
          ).value,
      ),
    ).value;
  const partitionValues = valuesFor(expr.over.partitionBy);
  const orderValues = valuesFor(orderExpressions);
  return scope.allocate(c.array + entries.length * c.arraySlot, "vector window row indexes", () =>
    entries.map((entry) => {
      const partition = partitionValues[entry.batchIndex] as ReturnType<typeof batchExprValues>[];
      const order = orderValues[entry.batchIndex] as ReturnType<typeof batchExprValues>[];
      const keys = (values: ReturnType<typeof batchExprValues>[]) =>
        scope.allocate(c.array + values.length * c.arraySlot, "vector window keys", () =>
          values.map((value) => value.valueAt(entry.rowIndex, scope)),
        ).value;
      return scope.allocate(c.object + 4 * c.property, "vector window row index", () => ({
        row: rowForEntry(entry, inputs),
        originalIndex: entry.originalIndex,
        partitionKey: keys(partition),
        orderKey: keys(order),
      })).value;
    }),
  ).value;
}

function rowForEntry(entry: VectorWindowEntry, inputs: readonly VectorWindowBatch[]): Row {
  if (entry.row === undefined) {
    const input = inputs[entry.batchIndex] as VectorWindowBatch;
    entry.row = materializeBatchRow(input.batch, entry.rowIndex);
  }
  return entry.row;
}
