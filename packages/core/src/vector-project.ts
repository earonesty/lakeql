import { type Batch, batchExprValues, type Vector, vectorFromSource } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";

import { MemoryScope } from "./memory-scope.js";

export type VectorProjectionSpec = Record<string, Expr>;

export function vectorProjectBatch(
  batch: Batch,
  select?: readonly string[],
  projections: VectorProjectionSpec = {},
  scope?: MemoryScope,
): Batch {
  const columns: Record<string, Vector> = Object.create(null);
  for (const column of projectionColumns(batch, select)) {
    const vector = batch.columns[column];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${column}`, { column });
    }
    columns[column] = vector;
  }
  for (const [alias, expr] of Object.entries(projections)) {
    const evaluation =
      scope === undefined ? undefined : new MemoryScope(scope.budget, [scope.signal]);
    try {
      const values = batchExprValues(batch, expr, evaluation);
      // Direct vector expressions preserve shape and borrowed backing just as
      // selected columns do; the execution stage retains input ownership.
      columns[alias] =
        values.vector ??
        vectorFromSource({ length: batch.rowCount, get: (index) => values.valueAt(index) }, scope);
    } finally {
      evaluation?.dispose();
    }
  }
  return { rowCount: batch.rowCount, columns };
}

function projectionColumns(batch: Batch, select: readonly string[] | undefined): string[] {
  if (select === undefined || select.length === 0) return Object.keys(batch.columns);
  if (!select.includes("*")) return [...select];
  const columns = new Set(Object.keys(batch.columns));
  for (const column of select) {
    if (column !== "*") columns.add(column);
  }
  return [...columns];
}
