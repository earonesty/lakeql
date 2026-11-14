import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, batchExprValues, type Vector, vectorFromSource } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Expr } from "./expr.js";
import type { MemoryScope } from "./memory-scope.js";

export type VectorProjectionSpec = Record<string, Expr>;

/** A physical projection may borrow its input when every output mapping and
 * column order is unchanged. Public vectorProjectBatch still creates a directory.
 */
export function isIdentityVectorProjection(
  batch: Batch,
  select?: readonly string[],
  projections?: VectorProjectionSpec,
): boolean {
  if (projections !== undefined)
    for (const name in projections) if (Object.hasOwn(projections, name)) return false;
  if (select === undefined || select.length === 0 || select.includes("*")) {
    if (select !== undefined)
      for (const name of select)
        if (
          name !== "*" &&
          (!Object.hasOwn(batch.columns, name) || batch.columns[name] === undefined)
        )
          return false;
    for (const name in batch.columns)
      if (Object.hasOwn(batch.columns, name) && batch.columns[name] === undefined) return false;
    return true;
  }
  let index = 0;
  for (const name in batch.columns) {
    if (!Object.hasOwn(batch.columns, name)) continue;
    if (select[index++] !== name || batch.columns[name] === undefined) return false;
  }
  return index === select.length;
}

export function vectorProjectBatch(
  batch: Batch,
  select?: readonly string[],
  projections: VectorProjectionSpec = {},
  scope?: MemoryScope,
): Batch {
  const output = scope?.childScope();
  try {
    const createColumns = (): Record<string, Vector> => Object.create(null);
    const columns = output
      ? output.allocate(c.object, "projection column directory", createColumns).value
      : createColumns();
    const createBatch = () => ({ rowCount: batch.rowCount, columns });
    const result = output
      ? output.allocate(c.object + 2 * c.property, "projected batch descriptor", createBatch).value
      : createBatch();
    const admitName = (name: string) => {
      if (!Object.hasOwn(columns, name))
        output?.allocate(
          c.property + c.string + name.length * c.stringCodeUnit,
          "projection column entry",
          () => undefined,
        );
    };
    for (const column of projectionColumns(batch, select)) {
      const vector = Object.hasOwn(batch.columns, column) ? batch.columns[column] : undefined;
      if (vector === undefined)
        throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${column}`, { column });
      admitName(column);
      columns[column] = vector;
    }
    for (const alias in projections) {
      if (!Object.hasOwn(projections, alias)) continue;
      const expr = projections[alias];
      if (!expr) continue;
      admitName(alias);
      const evaluation = output?.childScope();
      try {
        const values = batchExprValues(batch, expr, evaluation);
        // Aliases borrow input backing; the execution-stage owner pins it.
        columns[alias] =
          values.vector ??
          vectorFromSource(
            { length: batch.rowCount, get: (index) => values.valueAt(index) },
            output,
          );
      } finally {
        evaluation?.dispose();
      }
    }
    if (!output || !scope) return result;
    const owned = output.retainAllocations(result);
    try {
      return scope.adopt(owned).value;
    } catch (error) {
      owned.release();
      throw error;
    }
  } finally {
    output?.dispose();
  }
}

function* projectionColumns(batch: Batch, select: readonly string[] | undefined): Iterable<string> {
  if (select === undefined || select.length === 0 || select.includes("*")) {
    for (const name in batch.columns) if (Object.hasOwn(batch.columns, name)) yield name;
  }
  if (select !== undefined) for (const name of select) if (name !== "*") yield name;
}
