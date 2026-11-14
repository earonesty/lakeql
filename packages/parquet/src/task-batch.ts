import {
  type Batch,
  MATERIALIZATION_CHARGES as c,
  constantVector,
  type MemoryScope,
  type Vector,
} from "lakeql-core";

export function batchWithPartitionValues(
  batch: Batch,
  partitionValues: Record<string, string>,
  scope: MemoryScope,
): Batch {
  let missing = false;
  for (const column in partitionValues) {
    if (
      Object.hasOwn(partitionValues, column) &&
      (!Object.hasOwn(batch.columns, column) || batch.columns[column] === undefined)
    ) {
      missing = true;
      break;
    }
  }
  if (!missing) return batch;
  const columns = scope.allocate(
    c.object,
    "partition column directory",
    () => Object.create(null) as Record<string, Vector>,
  ).value;
  const add = (column: string, vector: Vector) => {
    scope.allocate(
      c.property + c.string + column.length * c.stringCodeUnit,
      "partition column entry",
      () => {
        columns[column] = vector;
      },
    );
  };
  for (const column in batch.columns) {
    if (Object.hasOwn(batch.columns, column)) {
      const vector = batch.columns[column];
      if (vector !== undefined) add(column, vector);
    }
  }
  for (const column in partitionValues) {
    if (!Object.hasOwn(partitionValues, column) || columns[column] !== undefined) continue;
    const value = partitionValues[column];
    if (value !== undefined) add(column, constantVector(value, batch.rowCount, scope));
  }
  return scope.allocate(c.object + 2 * c.property, "partition batch", () => ({
    rowCount: batch.rowCount,
    columns,
  })).value;
}
export function projectTaskBatch(
  batch: Batch,
  names: string[] | undefined,
  scope: MemoryScope,
): Batch {
  if (names === undefined) return batch;
  const columns = scope.allocate(
    c.object,
    "task projected columns",
    () => Object.create(null) as Record<string, Vector>,
  ).value;
  for (const name of names) {
    if (Object.hasOwn(columns, name)) continue;
    scope.allocate(
      c.property + c.string + name.length * c.stringCodeUnit,
      "task projected column",
      () => {
        const vector = Object.hasOwn(batch.columns, name) ? batch.columns[name] : undefined;
        columns[name] = vector ?? constantVector(null, batch.rowCount, scope);
      },
    );
  }
  return scope.allocate(c.object + 2 * c.property, "task projected batch", () => ({
    rowCount: batch.rowCount,
    columns,
  })).value;
}
