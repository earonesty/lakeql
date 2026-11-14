import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import { dictionaryColumnEnd } from "./dictionary-column-extent.js";
import { dictionaryPageOffset } from "./dictionary-offset.js";
import type { ParquetMetadata, ReadParquetOptions, StoreAsyncBuffer } from "./types.js";

type RowGroup = ParquetMetadata["row_groups"][number];
// Only private recovered groups receive this proof. A projection must not certify
// uninspected columns, and no global/cache lifetime is extended by this marker.
const inspectedColumns = Symbol("inspected dictionary extents");
type InspectedGroup = RowGroup & { [inspectedColumns]?: string[] | null };

/** Correct proven legacy extents in private metadata, with no changes to cache entries.
 * physicalGroups must enumerate the complete file even when metadata is a selected
 * row-group view. Indexed callers may acquire and release groups while enumerating.
 */
export async function recoverColumnExtents(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  options: ReadParquetOptions,
  scope?: MemoryScope,
  physicalGroups: () => Iterable<RowGroup> = () => metadata.row_groups,
  selectedGroupIndex?: number,
): Promise<ParquetMetadata> {
  let result = metadata;
  let rowStart = 0;
  const copyObject = <T extends object>(value: T): T => {
    let properties = 0;
    for (const key in value) if (Object.hasOwn(value, key)) properties++;
    return scope === undefined
      ? { ...value }
      : scope.allocate(
          c.object + (properties + 1) * c.property,
          "recovered column metadata",
          () => ({ ...value }),
        ).value;
  };
  const copyArray = <T>(value: T[]): T[] =>
    scope === undefined
      ? value.slice()
      : scope.allocate(c.array + value.length * c.arraySlot, "recovered metadata directory", () =>
          value.slice(),
        ).value;
  for (
    let groupIndex = selectedGroupIndex ?? 0;
    groupIndex <
    (selectedGroupIndex === undefined ? metadata.row_groups.length : selectedGroupIndex + 1);
    groupIndex++
  ) {
    const group = metadata.row_groups[groupIndex];
    if (!group) continue;
    const rowEnd = rowStart + Number(group.num_rows);
    const selected =
      rowStart < (options.rowEnd ?? Number(metadata.num_rows)) && rowEnd > (options.rowStart ?? 0);
    rowStart = rowEnd;
    if (!selected) continue;
    const inspected = (group as InspectedGroup)[inspectedColumns];
    if (
      inspected === null ||
      (inspected !== undefined && options.columns?.every((column) => inspected.includes(column)))
    )
      continue;
    let replacement = group;
    for (let columnIndex = 0; columnIndex < group.columns.length; columnIndex++) {
      const chunk = group.columns[columnIndex];
      const column = chunk?.meta_data;
      if (
        !chunk ||
        !column ||
        column.dictionary_page_offset !== undefined ||
        (column.encodings.length > 0 &&
          !column.encodings.some(
            (encoding) => encoding === "PLAIN_DICTIONARY" || encoding === "RLE_DICTIONARY",
          )) ||
        (options.columns !== undefined && !options.columns.includes(column.path_in_schema[0] ?? ""))
      )
        continue;
      if (chunk.file_path)
        throw new LakeqlError(
          "LAKEQL_PARQUET_READ_ERROR",
          "External column extent recovery is unsupported",
        );
      const start = Number(column.data_page_offset);
      const declaredEnd = start + Number(column.total_compressed_size);
      let physicalEnd = file.byteLength - metadata.metadata_length - 8;
      if (!Number.isSafeInteger(physicalEnd) || physicalEnd < 4)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid recovery footer boundary");
      let equalStarts = 0;
      for (const physical of physicalGroups())
        for (const sibling of physical.columns) {
          const info = sibling.meta_data;
          if (!info || sibling.file_path)
            throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Unknown physical column boundary");
          const offset = Number(dictionaryPageOffset(info) ?? info.data_page_offset);
          if (
            !Number.isSafeInteger(offset) ||
            offset < 4 ||
            offset > file.byteLength - metadata.metadata_length - 8
          )
            throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid physical column boundary");
          if (offset === start) equalStarts++;
          else if (offset > start) physicalEnd = Math.min(physicalEnd, offset);
        }
      if (equalStarts !== 1)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Ambiguous physical column boundary");
      const end = await dictionaryColumnEnd(file, column, physicalEnd, scope);
      if (end === declaredEnd) continue;
      if (result === metadata) {
        result = copyObject(metadata);
        result.row_groups = copyArray(metadata.row_groups);
      }
      if (replacement === group) {
        replacement = copyObject(group);
        replacement.columns = copyArray(group.columns);
        result.row_groups[groupIndex] = replacement;
      }
      const corrected = copyObject(column);
      corrected.total_compressed_size = BigInt(end - start);
      corrected.dictionary_page_offset = BigInt(start);
      const correctedChunk = copyObject(chunk);
      correctedChunk.meta_data = corrected;
      replacement.columns[columnIndex] = correctedChunk;
    }
    if (replacement !== group) {
      (replacement as InspectedGroup)[inspectedColumns] =
        options.columns === undefined ? null : copyArray(options.columns);
    }
  }
  return result;
}
