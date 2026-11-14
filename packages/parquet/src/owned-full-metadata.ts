import type { SchemaElement } from "hyparquet";
import { MATERIALIZATION_CHARGES as c, type MemoryScope } from "lakeql-core";
import { footerRowGroupView, indexFooterRowGroups } from "./footer-directory.js";
import { markGeoMetadata } from "./geo-metadata.js";
import { readParquetFooterEnvelope } from "./incremental-metadata.js";
import { decodeRowGroupMetadata } from "./row-group-metadata.js";
import type { ParquetMetadata } from "./types.js";

/** Full metadata with checked statistics and transactional allocation ownership.
 * The caller owns footer backing separately. Directory and leaf references are
 * temporary; converted metadata and conservatively retained raw records survive.
 */
export function decodeOwnedFullMetadata(footer: ArrayBuffer, scope: MemoryScope): ParquetMetadata {
  const construction = scope.childScope();
  const scratch = construction.childScope();
  try {
    const { view, metadata, rowGroupCount } = readParquetFooterEnvelope(
      footer,
      undefined,
      construction,
    );
    let leafCount = 0;
    for (const element of metadata.schema) if (element.type) leafCount++;
    const leaves = scratch.allocate(
      c.array + leafCount * c.arraySlot,
      "metadata leaf references",
      () => {
        const result: SchemaElement[] = [];
        for (const element of metadata.schema) if (element.type) result.push(element);
        return result;
      },
    ).value;
    const directory = indexFooterRowGroups(view, scratch, rowGroupCount);
    const row_groups = construction.allocate<ParquetMetadata["row_groups"]>(
      c.array + rowGroupCount * c.arraySlot,
      "full metadata row groups",
      () => [],
    ).value;
    for (let index = 0; index < rowGroupCount; index++) {
      row_groups.push(
        decodeRowGroupMetadata(
          footerRowGroupView(view, directory, index),
          leaves,
          true,
          construction,
        ),
      );
    }
    markGeoMetadata(metadata.schema, metadata.key_value_metadata, construction);
    const result = construction.allocate(c.object + 7 * c.property, "full metadata result", () => ({
      ...metadata,
      row_groups,
    })).value;
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}
