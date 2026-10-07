import { type Batch, batchExprValues, type Vector, vectorFromValues } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Expr, Scalar } from "./expr.js";

export type VectorProjectionSpec = Record<string, Expr>;

export function vectorProjectBatch(
  batch: Batch,
  select?: readonly string[],
  projections: VectorProjectionSpec = {},
): Batch {
  const columns: Record<string, Vector> = {};
  for (const column of projectionColumns(batch, select)) {
    const vector = batch.columns[column];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${column}`, { column });
    }
    columns[column] = vector;
  }
  for (const [alias, expr] of Object.entries(projections)) {
    columns[alias] = vectorFromValues(exprValues(batch, expr));
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

function exprValues(batch: Batch, expr: Expr): Scalar[] {
  const values = batchExprValues(batch, expr);
  const out: Scalar[] = [];
  for (let index = 0; index < batch.rowCount; index += 1) out.push(values.valueAt(index));
  return out;
}
