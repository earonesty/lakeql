import { MATERIALIZATION_CHARGES as c } from "lakeql";

/** Modeled JS descriptors created by selection execution, excluding payload
 * backing (reserved separately) and opaque driver/runtime allocations.
 */
export function selectionWorkspaceMetadataBytes(fragmentId: string, columns: number): number {
  // Upload/readback views plus at most one reinterpretation and validity view per column.
  const views = (3 + 2 * columns) * c.view;
  // Buffer list, binding list, submission list and runtime error-scope list.
  const lists = 4 * c.array + 11 * c.arraySlot;
  // Four buffer descriptors, six binding/resource records, bind/encoder descriptors,
  // nine API handle wrappers, attempt metrics and transfer descriptor.
  const records = 23 * c.object + 33 * c.property;
  // Cold pipeline creation: shader, pipeline and compute descriptors plus API
  // shader/pipeline handles. Warm execution conservatively keeps this allowance.
  const compilation = 5 * c.object + 6 * c.property + 2 * c.string + 32 * c.stringCodeUnit;
  // Four buffer labels and one encoder label: shared fragment id is copied into each.
  const labels = 5 * c.string + (5 * fragmentId.length + 70) * c.stringCodeUnit;
  return views + lists + records + labels + compilation;
}

/** Mask backing/view and the returned result, output and metrics records. */
export function selectionResultBytes(rows: number): number {
  return rows + c.view + 3 * c.object + 14 * c.property;
}
