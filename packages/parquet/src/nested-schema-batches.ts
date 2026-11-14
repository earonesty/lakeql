import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type OwnedMemory,
  type Vector,
  vectorLength,
} from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import type { NestedRowFragment } from "./nested-column-rows.js";
import {
  indexNestedFragments,
  type NestedFragmentIndex,
  windowNestedFragments,
} from "./nested-fragment-window.js";
import type { AlignedNestedRow } from "./nested-row-alignment.js";
import { joinNestedFragments } from "./nested-row-fragments.js";
import { validateNestedRowWindow } from "./nested-row-window.js";
import { executeNestedSchema, type NestedSchemaInput } from "./nested-schema-execute.js";
import type { NestedSchemaPlan } from "./nested-schema-plan.js";

/** Assemble aligned rows within available memory, shrinking atomic assembly on
 * admission failure. Leases expire on advance/close;
 * input payloads remain shared and caller retention keeps them independently live.
 */
export async function* readNestedSchemaBatches(
  source: AsyncIterable<OwnedMemory<AlignedNestedRow>>,
  plan: NestedSchemaPlan,
  paths: readonly ColumnDecoder["schemaPath"][],
  scope: MemoryScope,
  options: { rowStart: number; rowEnd: number; batchSize: number },
): AsyncGenerator<OwnedMemory<Vector>> {
  const batchSize = parquetBatchSize(options.batchSize);
  validateNestedRowWindow(options.rowStart, options.rowEnd);
  if (options.rowStart === options.rowEnd) return;
  let batch: MemoryScope | undefined;
  let rows: AlignedNestedRow[] | undefined;
  let position = 0;
  let heldRows = 0;
  let measuredRows = batchSize;
  let selectionScope: MemoryScope | undefined;
  let indexes: (NestedFragmentIndex | undefined)[] | undefined;
  const columnFragments = (column: number, target: MemoryScope) => {
    if (!rows) throw Error("Missing aligned batch");
    let count = 0;
    for (const row of rows) count += row.columns[column]?.fragments.length ?? 0;
    const fragments = target.allocate(
      c.array + count * c.arraySlot,
      "nested batch fragment directory",
      () => [] as NestedRowFragment[],
    ).value;
    for (const row of rows) {
      const input = row.columns[column];
      if (!input) throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing nested batch column");
      for (const fragment of input.fragments) fragments.push(fragment);
    }
    return fragments;
  };
  const seal = (start: number, length: number) => {
    if (!batch || !rows?.length) throw Error("Missing aligned batch");
    const construction = batch.childScope();
    const scratch = construction.childScope();
    try {
      const inputs = scratch.allocate(
        c.array + paths.length * (c.arraySlot + c.object + 3 * c.property),
        "nested batch schema inputs",
        () => new Array<NestedSchemaInput>(paths.length),
      ).value;
      for (let column = 0; column < paths.length; column++) {
        const schemaPath = paths[column];
        if (!schemaPath) throw Error("Missing schema path");
        let selected: readonly NestedRowFragment[];
        if (start === 0 && length === heldRows) {
          selected = columnFragments(column, scratch);
        } else {
          selectionScope ??= batch.childScope();
          indexes ??= selectionScope.allocate(
            c.array + paths.length * c.arraySlot,
            "nested fragment index directory",
            () => new Array<NestedFragmentIndex | undefined>(paths.length),
          ).value;
          let index = indexes[column];
          if (!index) {
            const staging = selectionScope.childScope();
            try {
              index = indexNestedFragments(columnFragments(column, staging), staging);
              selectionScope.adopt(staging.retainAllocations(undefined));
              indexes[column] = index;
            } finally {
              staging.dispose();
            }
          }
          selected = windowNestedFragments(index, start, length, scratch);
        }
        const joined = joinNestedFragments(selected, length, schemaPath, construction);
        inputs[column] = { schemaPath, levels: joined.levels, values: joined.values };
      }
      const vector = executeNestedSchema(plan, inputs, construction);
      if (vectorLength(vector) !== length)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested batch row count mismatch");
      scratch.dispose();
      construction.adopt(batch.retainAllocations(undefined));
      return construction.retainAllocations(vector);
    } finally {
      scratch.dispose();
      construction.dispose();
    }
  };
  try {
    for await (const row of source) {
      if (row.value.columns.length !== paths.length)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested batch/schema column mismatch");
      const count = row.value.rowCount ?? 1;
      if (!Number.isSafeInteger(count) || count < 1 || !Number.isSafeInteger(position + count))
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested block size");
      const start = position;
      position += count;
      if (position <= options.rowStart) continue;
      if (start < options.rowStart || position > options.rowEnd || heldRows + count > batchSize)
        throw new LakeqlError(
          "LAKEQL_PARQUET_READ_ERROR",
          "Nested block crosses requested batch boundary",
        );
      if (!batch) {
        batch = scope.childScope();
        rows = batch.allocate(
          c.array,
          "nested batch row directory",
          () => [] as AlignedNestedRow[],
        ).value;
      }
      const pin = row.retainIn(batch.budget);
      try {
        batch.adopt(pin);
      } catch (error) {
        pin.release();
        throw error;
      }
      // The directory stores physical blocks, not logical rows. Admit each
      // actual block slot before growing it instead of reserving batchSize slots.
      batch.allocate(c.arraySlot, "nested batch block slot", () => undefined);
      rows?.push(pin.value);
      heldRows += count;
      if (heldRows === batchSize || position === options.rowEnd) {
        let emitted = 0;
        let target = Math.min(measuredRows, heldRows);
        while (emitted < heldRows) {
          let owner: OwnedMemory<Vector>;
          const length = Math.min(target, heldRows - emitted);
          try {
            owner = seal(emitted, length);
          } catch (error) {
            // Only retry atomic assembly of already-owned fragments. No source
            // advances, decoding, or cloud reads are repeated on this path.
            if (
              !(error instanceof LakeqlError) ||
              error.code !== "LAKEQL_BUDGET_EXCEEDED" ||
              length === 1
            )
              throw error;
            target = Math.max(1, Math.floor(length / 2));
            measuredRows = Math.min(measuredRows, target);
            continue;
          }
          try {
            yield owner;
          } finally {
            owner.release();
          }
          emitted += length;
        }
        batch.dispose();
        batch = undefined;
        rows = undefined;
        heldRows = 0;
        selectionScope = undefined;
        indexes = undefined;
      }
      if (position === options.rowEnd) return;
    }
    if (position < options.rowEnd)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested row window exceeds column rows");
  } finally {
    batch?.dispose();
  }
}
