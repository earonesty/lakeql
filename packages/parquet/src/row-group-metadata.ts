import type { RowGroup, SchemaElement } from "hyparquet";
import { CompressionCodecs, Encodings, PageTypes, ParquetTypes } from "hyparquet/src/constants.js";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { convertMetadata } from "hyparquet/src/metadata.js";
import type { ThriftObject, ThriftType } from "hyparquet/src/types.js";
import { isLakeqlError, LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import { planCompactAllocation } from "./compact-allocation.js";
import { type CompactProjection, readCompactHeader } from "./compact-header.js";
import { convertDecimalValue, isDecimal, isIntegerDecimal } from "./decimal.js";
import { columnRawAliasBytes, rowGroupRawAliasBytes } from "./metadata-aliases.js";
import { decodeMetadataText } from "./metadata-text.js";
import { lakeqlParquetParsers } from "./parsers.js";

/** Only fields consumed by conversion. Opaque crypto payloads deliberately retain
 * their wire representation; primitive lists are retained for conversion/output.
 */
export const COLUMN_CHUNK_PROJECTION: CompactProjection = {
  1: "binary",
  2: "i64",
  3: {
    1: "i32",
    2: true,
    3: true,
    4: "i32",
    5: "i64",
    6: "i64",
    7: "i64",
    8: { list: { 1: "binary", 2: "binary" } },
    9: "i64",
    10: "i64",
    11: "i64",
    12: {
      1: "binary",
      2: "binary",
      3: "i64",
      4: "i64",
      5: "binary",
      6: "binary",
      7: "bool",
      8: "bool",
    },
    13: { list: { 1: "i32", 2: "i32", 3: "i32" } },
    14: "i64",
    // Existing external files use legacy non-scalar field 15 payloads. Preserve
    // the converter's representation rather than imposing a newer wire shape.
    15: true,
    16: { 1: "i64", 2: true, 3: true },
    17: { 1: { 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true }, 2: true },
  },
  4: "i64",
  5: "i32",
  6: "i64",
  7: "i32",
  8: true,
  9: "binary",
};

export const ROW_GROUP_FIELDS_PROJECTION: CompactProjection = {
  2: "i64",
  3: "i64",
  4: { list: { 1: "i32", 2: "bool", 3: "bool" } },
  5: "i64",
  6: "i64",
  7: true,
};

const statisticsTextDecoder = new TextDecoder("utf-8", { ignoreBOM: true });
const parsers = { ...DEFAULT_PARSERS, ...lakeqlParquetParsers };
const object = (value: ThriftType | undefined) => value as ThriftObject | undefined;
const list = (value: ThriftType | undefined) => value as ThriftObject[] | undefined;
const decode = (value: ThriftType | undefined, scope?: MemoryScope) =>
  decodeMetadataText(value as Uint8Array | undefined, scope);
const enumValue = <T>(values: readonly T[], value: ThriftType | undefined) =>
  values[value as number];

/** New containers only; raw aliases and decoded strings have separate owners. */
export function columnMetadataStructureBytes(chunk: ThriftObject): number {
  const c = MATERIALIZATION_CHARGES;
  const record = (fields: number) => c.object + fields * c.property;
  const mapped = (value: ThriftType | undefined, fields = 0) => {
    if (value === undefined) return 0;
    if (!Array.isArray(value))
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid metadata list");
    return c.array + value.length * (c.arraySlot + (fields ? record(fields) : 0));
  };
  const data = object(chunk.field_3);
  let bytes = record(9);
  if (data) {
    bytes +=
      record(17) +
      mapped(data.field_2) +
      mapped(data.field_3) +
      mapped(data.field_8, 2) +
      mapped(data.field_13, 3);
    if (data.field_12) bytes += record(8);
    if (data.field_16) bytes += record(3);
    const geo = object(data.field_17);
    if (geo) bytes += record(2) + (geo.field_1 ? record(8) : 0);
  }
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Metadata allocation exceeds safe capacity");
  return bytes;
}

export function convertColumnChunkMetadata(
  chunk: ThriftObject,
  schema: SchemaElement | undefined,
  checkedDecimals = false,
  scope?: MemoryScope,
): RowGroup["columns"][number] {
  if (scope === undefined) return convertColumnChunk(chunk, schema, checkedDecimals);
  return scope.allocate(columnMetadataStructureBytes(chunk), "converted column metadata", () =>
    convertColumnChunk(chunk, schema, checkedDecimals, scope),
  ).value;
}

function convertColumnChunk(
  chunk: ThriftObject,
  schema: SchemaElement | undefined,
  checkedDecimals = false,
  scope?: MemoryScope,
): RowGroup["columns"][number] {
  const data = object(chunk.field_3);
  const stats = object(data?.field_12);
  const size = object(data?.field_16);
  const geo = object(data?.field_17);
  const bbox = object(geo?.field_1);
  const bound = (value: ThriftType | undefined) => {
    if (value === undefined) return undefined;
    if (schema === undefined)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Statistics column has no schema leaf");
    // Preserve the statistics parser's BOM policy while admitting UTF-8 output.
    // Checked decimals follow their numeric conversion path below.
    if (schema.type === "BYTE_ARRAY" && !(checkedDecimals && isDecimal(schema)))
      return decodeMetadataText(value as Uint8Array, scope, statisticsTextDecoder);
    const convertScalar = (targetScope = scope) => {
      const convert = () => convertMetadata(value as Uint8Array, schema, parsers);
      if (targetScope === undefined || schema.type === "BOOLEAN") return convert();
      const c = MATERIALIZATION_CHARGES;
      const temporal =
        schema.converted_type === "DATE" ||
        schema.converted_type === "TIMESTAMP_MILLIS" ||
        schema.converted_type === "TIMESTAMP_MICROS" ||
        schema.logical_type?.type === "TIMESTAMP";
      const numeric =
        schema.type === "INT32" ||
        schema.type === "INT64" ||
        schema.type === "FLOAT" ||
        schema.type === "DOUBLE" ||
        schema.logical_type?.type === "FLOAT16" ||
        schema.converted_type === "DECIMAL";
      const retainedBytes =
        (numeric ? c.number : 0) +
        (schema.type === "INT64" ? c.bigint : 0) +
        (temporal ? c.timestamp : 0);
      const stage = targetScope.childStage(c.view + retainedBytes, "fixed-width statistic");
      try {
        const result = stage.allocate(retainedBytes, "statistic value", convert);
        return targetScope.adopt(result.retain()).value;
      } finally {
        stage.dispose();
      }
    };
    if (checkedDecimals && isDecimal(schema)) {
      const scratch = isIntegerDecimal(schema) ? scope?.childScope() : undefined;
      try {
        const unscaled = isIntegerDecimal(schema) ? convertScalar(scratch) : value;
        return convertDecimalValue(unscaled, schema, schema.name, scope);
      } catch (error) {
        if (isLakeqlError(error) && error.code === "LAKEQL_BUDGET_EXCEEDED") throw error;
        // Unsafe/invalid bounds cannot exclude rows; selected values still validate.
        return undefined;
      } finally {
        scratch?.dispose();
      }
    }
    return convertScalar();
  };
  const result = {
    file_path: decode(chunk.field_1, scope),
    file_offset: chunk.field_2,
    meta_data: data && {
      type: enumValue(ParquetTypes, data.field_1),
      encodings: (data.field_2 as number[] | undefined)?.map((value) => Encodings[value]),
      path_in_schema: (data.field_3 as Uint8Array[]).map((value) =>
        decodeMetadataText(value, scope),
      ),
      codec: enumValue(CompressionCodecs, data.field_4),
      num_values: data.field_5,
      total_uncompressed_size: data.field_6,
      total_compressed_size: data.field_7,
      key_value_metadata: list(data.field_8)?.map((kv) => ({
        key: decode(kv.field_1, scope),
        value: decode(kv.field_2, scope),
      })),
      data_page_offset: data.field_9,
      index_page_offset: data.field_10,
      dictionary_page_offset: data.field_11,
      statistics: stats && {
        max: bound(stats.field_1),
        min: bound(stats.field_2),
        null_count: stats.field_3,
        distinct_count: stats.field_4,
        max_value: bound(stats.field_5),
        min_value: bound(stats.field_6),
        is_max_value_exact: stats.field_7,
        is_min_value_exact: stats.field_8,
      },
      encoding_stats: list(data.field_13)?.map((value) => ({
        page_type: enumValue(PageTypes, value.field_1),
        encoding: enumValue(Encodings, value.field_2),
        count: value.field_3,
      })),
      bloom_filter_offset: data.field_14,
      bloom_filter_length: data.field_15,
      size_statistics: size && {
        unencoded_byte_array_data_bytes: size.field_1,
        repetition_level_histogram: size.field_2,
        definition_level_histogram: size.field_3,
      },
      geospatial_statistics: geo && {
        bbox: bbox && {
          xmin: bbox.field_1,
          xmax: bbox.field_2,
          ymin: bbox.field_3,
          ymax: bbox.field_4,
          zmin: bbox.field_5,
          zmax: bbox.field_6,
          mmin: bbox.field_7,
          mmax: bbox.field_8,
        },
        geospatial_types: geo.field_2,
      },
    },
    offset_index_offset: chunk.field_4,
    offset_index_length: chunk.field_5,
    column_index_offset: chunk.field_6,
    column_index_length: chunk.field_7,
    crypto_metadata: chunk.field_8,
    encrypted_column_metadata: chunk.field_9,
  } as unknown as RowGroup["columns"][number];
  if (
    checkedDecimals &&
    schema !== undefined &&
    isDecimal(schema) &&
    result.meta_data?.statistics
  ) {
    const statistics = result.meta_data.statistics;
    for (const key of ["min", "max", "min_value", "max_value"] as const)
      if (statistics[key] === undefined) delete statistics[key];
  }
  return result;
}

/** Convert one complete row group. Unchanged histogram/crypto payloads retain their existing representation. */
export function convertRowGroupMetadata(
  raw: ThriftObject,
  leaves: readonly SchemaElement[],
  convertedColumns?: RowGroup["columns"],
  scope?: MemoryScope,
): RowGroup {
  if (scope === undefined) return convertRowGroup(raw, leaves, convertedColumns);
  const c = MATERIALIZATION_CHARGES;
  const sorting = list(raw.field_4);
  const columns = list(raw.field_1);
  const bytes =
    c.object +
    7 * c.property +
    (sorting === undefined
      ? 0
      : c.array + sorting.length * (c.arraySlot + c.object + 3 * c.property)) +
    (convertedColumns === undefined ? c.array + (columns?.length ?? 0) * c.arraySlot : 0);
  return scope.allocate(bytes, "converted row-group containers", () =>
    convertRowGroup(raw, leaves, convertedColumns, scope),
  ).value;
}

function convertRowGroup(
  raw: ThriftObject,
  leaves: readonly SchemaElement[],
  convertedColumns?: RowGroup["columns"],
  scope?: MemoryScope,
): RowGroup {
  if (
    raw.field_1 === undefined ||
    typeof raw.field_2 !== "bigint" ||
    raw.field_2 < 0n ||
    typeof raw.field_3 !== "bigint" ||
    raw.field_3 < 0n
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid required Parquet row-group fields");
  const columns =
    convertedColumns ??
    (list(raw.field_1) ?? []).map((chunk, index) =>
      convertColumnChunkMetadata(chunk, leaves[index], false, scope),
    );
  return {
    columns,
    total_byte_size: raw.field_2,
    num_rows: raw.field_3,
    sorting_columns: list(raw.field_4)?.map((value) => ({
      column_idx: value.field_1,
      descending: value.field_2,
      nulls_first: value.field_3,
    })),
    file_offset: raw.field_5,
    total_compressed_size: raw.field_6,
    ordinal: raw.field_7,
  } as unknown as RowGroup;
}

/** Decode one directory-selected group, converting columns as each entry completes. */
export function decodeRowGroupMetadata(
  view: DataView,
  leaves: readonly SchemaElement[],
  checkedDecimals = false,
  scope?: MemoryScope,
): RowGroup {
  const columns: RowGroup["columns"] =
    scope === undefined
      ? []
      : scope.allocate(MATERIALIZATION_CHARGES.array, "converted column list", () => []).value;
  const reader = { view, offset: 0 };
  const projection: CompactProjection = {
    1: {
      list: COLUMN_CHUNK_PROJECTION,
      visit: (value, index) => {
        const append = () => {
          if (scope !== undefined && scratch !== undefined)
            scope.allocateReserved(
              scratch.reserve(
                columnRawAliasBytes(value as ThriftObject),
                "retained raw column aliases",
              ),
              () => undefined,
            );
          return columns.push(
            convertColumnChunkMetadata(
              value as ThriftObject,
              leaves[index],
              checkedDecimals,
              scope,
            ),
          );
        };
        if (scope === undefined) append();
        else scope.allocate(MATERIALIZATION_CHARGES.arraySlot, "converted column slot", append);
      },
    },
    ...ROW_GROUP_FIELDS_PROJECTION,
  };
  // Admit the whole raw tree before parsing. Retained aliases consume partitions
  // of this reservation; they are not newly allocated copies. The remainder
  // covers transient parser records until conversion finishes.
  const scratch = scope?.childStage(
    planCompactAllocation(reader, projection),
    "raw compact metadata",
  );
  try {
    const raw = readCompactHeader(reader, projection);
    if (reader.offset !== view.byteLength)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Trailing row-group metadata bytes");
    if (scope !== undefined && scratch !== undefined)
      scope.allocateReserved(
        scratch.reserve(rowGroupRawAliasBytes(raw), "retained raw row-group aliases"),
        () => undefined,
      );
    return convertRowGroupMetadata(raw, leaves, columns, scope);
  } finally {
    scratch?.dispose();
  }
}
