import { MATERIALIZATION_CHARGES as c, isLakeqlError, type MemoryScope } from "lakeql-core";
import { type NestedReadOptions, resolveInt96Interpretation } from "./int96-interpretation.js";
import { canReadNestedColumn } from "./nested-column-capability.js";
import { nestedColumnPlan } from "./nested-column-plan.js";
import { compileNestedProjection } from "./nested-schema-plan.js";
import { schemaPathIndex } from "./schema-path-index.js";
import type { ParquetMetadata } from "./types.js";

/** Preflight metadata/layout capability before any output. Only unsupported
 * capabilities decline; corruption and admission failures remain real errors.
 */
export function canReadNestedRowGroupBatches(
  metadata: ParquetMetadata,
  options: NestedReadOptions,
  scope: MemoryScope,
): boolean {
  const planning = scope.childScope();
  try {
    const lookup = schemaPathIndex(metadata.schema, planning);
    const root = planning.allocate(c.array + c.arraySlot, "nested capability root", () =>
      lookup([]),
    ).value[0];
    if (!root) return false;
    const names =
      options.columns ??
      planning.allocate(
        c.array + root.children.length * c.arraySlot,
        "nested capability projection",
        () => root.children.map((child) => child.element.name),
      ).value;
    if (names.length === 0) return false;
    for (const group of metadata.row_groups) {
      const stage = planning.childScope();
      try {
        const columns = nestedColumnPlan(
          group,
          lookup,
          names,
          stage,
          resolveInt96Interpretation(metadata, options.int96Interpretation),
        );
        for (const column of columns)
          if (!canReadNestedColumn(column.metadata, column.decoder)) return false;
        const paths = stage.allocate(
          c.array + columns.length * c.arraySlot,
          "nested capability paths",
          () => columns.map((column) => column.decoder.schemaPath),
        ).value;
        compileNestedProjection(root, paths, names, stage);
      } finally {
        stage.dispose();
      }
    }
    return true;
  } catch (error) {
    if (isLakeqlError(error) && error.code === "LAKEQL_UNSUPPORTED_PARQUET_FEATURE") return false;
    throw error;
  } finally {
    planning.dispose();
  }
}
