import { LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import { footerRowGroupView, indexFooterRowGroups } from "./footer-directory.js";
import { markGeoMetadata } from "./geo-metadata.js";
import { readParquetFooterEnvelope } from "./incremental-metadata.js";
import { decodeRowGroupMetadata } from "./row-group-metadata.js";

/** Copied leaf records preserve annotations before GeoParquet marking mutates schema. */
export function indexedLeafBytes(
  metadata: ReturnType<typeof readParquetFooterEnvelope>["metadata"],
): number {
  const c = MATERIALIZATION_CHARGES;
  let bytes = c.array;
  for (const element of metadata.schema) {
    if (!element.type) continue;
    bytes += c.arraySlot + c.object;
    for (const key in element) if (Object.hasOwn(element, key)) bytes += c.property;
  }
  return bytes;
}

/** Internal checked-statistic metadata source. Footer backing must outlive all reads.
 * Directory and copied leaf records are scoped. Borrowed APIs require callers
 * to manage envelope/group ownership; acquired sources admit those separately.
 */
export function openIndexedParquetFooter(footer: ArrayBuffer, scope: MemoryScope) {
  if (scope.disposed)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Indexed footer scope is closed");
  return openIndexedParquetEnvelope(readParquetFooterEnvelope(footer), scope);
}

export function openIndexedParquetEnvelope(
  envelope: ReturnType<typeof readParquetFooterEnvelope>,
  scope: MemoryScope,
) {
  const assertOpen = () => {
    if (scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Indexed footer scope is closed");
  };
  assertOpen();
  const { view, metadata, rowGroupCount } = envelope;
  // Geo marking mutates schema annotations after statistic conversion in the
  // full reader. Preserve those input annotations for deferred conversion too.
  const construction = scope.childScope();
  let leaves: typeof metadata.schema;
  let directory: Uint32Array;
  try {
    leaves = construction.allocate(indexedLeafBytes(metadata), "indexed schema leaves", () => {
      const result: typeof metadata.schema = [];
      for (const element of metadata.schema) if (element.type) result.push({ ...element });
      return result;
    }).value;
    markGeoMetadata(metadata.schema, metadata.key_value_metadata, construction);
    directory = indexFooterRowGroups(view, construction, rowGroupCount);
    scope.adopt(construction.retainAllocations(undefined));
  } finally {
    construction.dispose();
  }
  return {
    metadata,
    rowGroupCount: directory.length / 2,
    readRowGroup(index: number, decodeScope?: MemoryScope) {
      assertOpen();
      return decodeRowGroupMetadata(
        footerRowGroupView(view, directory, index),
        leaves,
        true,
        decodeScope,
      );
    },
  };
}
