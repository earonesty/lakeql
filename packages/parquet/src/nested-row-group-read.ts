import { MATERIALIZATION_CHARGES as c, type MemoryScope, type OwnedMemory } from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import { type NestedReadOptions, resolveInt96Interpretation } from "./int96-interpretation.js";
import { readNestedBatchOutputs } from "./nested-batch-output.js";
import { nestedColumnPlan } from "./nested-column-plan.js";
import { readNestedProjectedBatches } from "./nested-projected-batches.js";
import { validateNestedRowWindow } from "./nested-row-window.js";
import { validateParquetReadConcurrency } from "./read-concurrency.js";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { rowGroupMayMatch, rowGroupMustMatch } from "./row-group-pruning.js";
import { schemaPathIndex } from "./schema-path-index.js";
import type { ParquetColumnBatch, ParquetMetadata, StoreAsyncBuffer } from "./types.js";

/** Projected nested row-group adapter. Caller selects this capability before
 * consuming results; failures must not restart a fallback after output is yielded.
 */
export async function* readNestedRowGroupBatches(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  scope: MemoryScope,
  options: NestedReadOptions,
): AsyncGenerator<OwnedMemory<ParquetColumnBatch>> {
  const batchSize = parquetBatchSize(options.batchSize);
  validateParquetReadConcurrency(options);
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Math.max(requestedStart, Number(metadata.num_rows));
  validateNestedRowWindow(requestedStart, requestedEnd);
  if (requestedStart === requestedEnd) return;
  const planning = scope.childScope();
  try {
    const lookup = schemaPathIndex(metadata.schema, planning);
    const root = planning.allocate(c.array + c.arraySlot, "schema root lookup", () => lookup([]))
      .value[0];
    if (!root) return;
    const names =
      options.columns ??
      planning.allocate(
        c.array + root.children.length * c.arraySlot,
        "nested default projection",
        () => root.children.map((child) => child.element.name),
      ).value;
    recordReadColumns(options.stats, names);
    let groupStart = 0;
    for (const group of metadata.row_groups) {
      const groupEnd = groupStart + Number(group.num_rows);
      if (
        groupEnd <= requestedStart ||
        groupStart >= requestedEnd ||
        !rowGroupMayMatch(group, options.where)
      ) {
        recordRowGroupSkipped(options.stats);
        groupStart = groupEnd;
        continue;
      }
      const stage = scope.childScope();
      try {
        const columns = nestedColumnPlan(
          group,
          lookup,
          names,
          stage,
          resolveInt96Interpretation(metadata, options.int96Interpretation),
        );
        const start = Math.max(groupStart, requestedStart),
          end = Math.min(groupEnd, requestedEnd);
        const vectors = readNestedProjectedBatches(file, root, columns, names, stage, {
          ...options,
          rowStart: start - groupStart,
          rowEnd: end - groupStart,
          batchSize,
          maxConcurrentReads: options.maxConcurrentPageReads ?? options.maxConcurrentReads ?? 4,
        });
        recordRowGroupRead(options.stats);
        for await (const owner of readNestedBatchOutputs(
          vectors,
          stage,
          start,
          rowGroupMustMatch(group, options.where),
        )) {
          recordRowsDecoded(options.stats, owner.value.batch.rowCount);
          yield owner;
        }
      } finally {
        stage.dispose();
      }
      groupStart = groupEnd;
      if (groupStart >= requestedEnd) break;
    }
  } finally {
    planning.dispose();
  }
}
