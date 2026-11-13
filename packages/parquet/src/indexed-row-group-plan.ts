import { type Expr, type MemoryBudget, throwIfAborted } from "lakeql-core";
import { acquireIndexedParquetSource } from "./indexed-source.js";
import { iteratePlannedRowGroups, planRowGroupsFromSource } from "./row-group-plan.js";
import type { ParquetRowGroupSource, StoreAsyncBuffer } from "./types.js";

/** Internal compact planning route; JS envelope/group/plan allocations remain separate. */
export async function planOwnedIndexedRowGroups(
  file: StoreAsyncBuffer,
  budget: MemoryBudget,
  where?: Expr,
  signals: readonly AbortSignal[] = [],
) {
  const source = await acquireIndexedParquetSource(file, budget, signals);
  try {
    return planRowGroupsFromSource(source.metadata, iterateIndexedGroups(source, signals), where);
  } finally {
    source.dispose();
  }
}

/** Footer ownership follows iterator lifetime, including early return/consumer failure. */
export async function* iterateOwnedIndexedRowGroups(
  file: StoreAsyncBuffer,
  budget: MemoryBudget,
  where?: Expr,
  signals: readonly AbortSignal[] = [],
) {
  const source = await acquireIndexedParquetSource(file, budget, signals);
  try {
    yield* iteratePlannedRowGroups(source.metadata, iterateIndexedGroups(source, signals), where);
  } finally {
    source.dispose();
  }
}

export function* iterateIndexedGroups(
  source: ParquetRowGroupSource,
  signals: readonly AbortSignal[],
) {
  for (let index = 0; index < source.rowGroupCount; index++) {
    for (const signal of signals) throwIfAborted(signal);
    if (source.acquireRowGroup === undefined) {
      yield source.readRowGroup(index);
    } else {
      const owner = source.acquireRowGroup(index);
      try {
        yield owner.value;
      } finally {
        owner.release();
      }
    }
  }
}
