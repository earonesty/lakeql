import type { SchemaElement } from "hyparquet";
import { ConvertedTypes, FieldRepetitionTypes, ParquetTypes } from "hyparquet/src/constants.js";
import type { ThriftObject, ThriftType } from "hyparquet/src/types.js";
import { LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import { readAdmittedCompactHeader } from "./compact-allocation.js";
import { type CompactAllocation, readCompactHeader } from "./compact-header.js";
import { markGeoMetadata } from "./geo-metadata.js";
import { convertLogicalMetadata } from "./logical-metadata.js";
import { decodeMetadataText } from "./metadata-text.js";
import {
  COLUMN_CHUNK_PROJECTION,
  convertColumnChunkMetadata,
  convertRowGroupMetadata,
  ROW_GROUP_FIELDS_PROJECTION,
} from "./row-group-metadata.js";
import type { ParquetMetadata } from "./types.js";

const object = (value: ThriftType | undefined) => value as ThriftObject | undefined;
const text = (value: ThriftType | undefined, scope?: MemoryScope) =>
  decodeMetadataText(value as Uint8Array | undefined, scope);

/** Convert envelope entries, then row groups/columns, without retaining their raw lists. */
export function readParquetFooterEnvelope(
  footer: ArrayBuffer,
  admitRaw?: (allocation: CompactAllocation) => void,
  scope?: MemoryScope,
) {
  const view = new DataView(footer);
  if (view.byteLength < 8 || view.getUint32(view.byteLength - 4, true) !== 0x31524150)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet footer magic");
  const length = view.getUint32(view.byteLength - 8, true);
  if (length > view.byteLength - 8)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Parquet footer exceeds buffer");
  const metadataView = new DataView(footer, view.byteLength - 8 - length, length);
  const envelopeReader = { view: metadataView, offset: 0 };
  let rowGroupCount = 0;
  const allocate = <T>(bytes: number, create: () => T): T =>
    scope === undefined
      ? create()
      : scope.allocate(bytes, "converted footer metadata", create).value;
  const cost = MATERIALIZATION_CHARGES;
  const schema = allocate<SchemaElement[]>(cost.array, () => []);
  const keyValues = allocate<NonNullable<ParquetMetadata["key_value_metadata"]>>(
    cost.array,
    () => [],
  );
  const read: typeof readCompactHeader =
    scope === undefined
      ? readCompactHeader
      : (reader, projection, counts, admit) =>
          readAdmittedCompactHeader(reader, scope, projection, counts, admit);
  const raw = read(
    envelopeReader,
    {
      1: "i32",
      2: {
        list: {
          1: "i32",
          2: "i32",
          3: "i32",
          4: "binary",
          5: "i32",
          6: "i32",
          7: "i32",
          8: "i32",
          9: "i32",
          10: true,
        },
        visit: (value) => {
          const field = value as ThriftObject;
          allocate(cost.arraySlot + cost.object + 10 * cost.property, () =>
            schema.push({
              type: ParquetTypes[field.field_1 as number],
              type_length: field.field_2,
              repetition_type: FieldRepetitionTypes[field.field_3 as number],
              name: text(field.field_4, scope),
              num_children: field.field_5,
              converted_type: ConvertedTypes[field.field_6 as number],
              scale: field.field_7,
              precision: field.field_8,
              field_id: field.field_9,
              logical_type: convertLogicalMetadata(object(field.field_10), scope),
            } as SchemaElement),
          );
        },
      },
      3: "i64",
      4: {
        list: false,
        visitRange: (_start, _end, index) => {
          if (index !== rowGroupCount++)
            throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Duplicate footer row-group list");
        },
      },
      5: {
        list: { 1: "binary", 2: "binary" },
        visit: (value) => {
          const kv = value as ThriftObject;
          allocate(cost.arraySlot + cost.object + 2 * cost.property, () =>
            keyValues.push({
              key: text(kv.field_1, scope),
              value: text(kv.field_2, scope),
            } as NonNullable<ParquetMetadata["key_value_metadata"]>[number]),
          );
        },
      },
      6: "binary",
    },
    undefined,
    admitRaw,
  );
  if (envelopeReader.offset !== length)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Trailing footer metadata bytes");
  if (raw.field_2 === undefined || schema.length === 0)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing Parquet schema");
  if (
    typeof raw.field_1 !== "number" ||
    !Number.isInteger(raw.field_1) ||
    raw.field_1 <= 0 ||
    typeof raw.field_3 !== "bigint" ||
    raw.field_3 < 0n
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid required Parquet file fields");
  if (raw.field_4 === undefined)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing required Parquet row-group list");
  const key_value_metadata = raw.field_5 === undefined ? undefined : keyValues;
  return allocate(2 * cost.object + 9 * cost.property, () => ({
    view: metadataView,
    rowGroupCount,
    metadata: {
      version: raw.field_1,
      schema,
      num_rows: raw.field_3,
      key_value_metadata,
      created_by: text(raw.field_6, scope),
      metadata_length: length,
    } as Omit<ParquetMetadata, "row_groups">,
  }));
}

export function incrementalParquetMetadata(footer: ArrayBuffer): ParquetMetadata {
  const { view: metadataView, metadata } = readParquetFooterEnvelope(footer);
  const { schema, key_value_metadata } = metadata;
  const leaves = schema.filter((element) => element.type);
  const row_groups: ParquetMetadata["row_groups"] = [];
  let columns: ParquetMetadata["row_groups"][number]["columns"] = [];
  const groupsEnvelope = readCompactHeader(
    { view: metadataView, offset: 0 },
    {
      4: {
        list: () => {
          columns = [];
          return {
            1: {
              list: COLUMN_CHUNK_PROJECTION,
              visit: (value, index) => {
                columns.push(convertColumnChunkMetadata(value as ThriftObject, leaves[index]));
              },
            },
            ...ROW_GROUP_FIELDS_PROJECTION,
          };
        },
        visit: (value) => {
          const group = value as ThriftObject;
          row_groups.push(convertRowGroupMetadata(group, leaves, columns));
        },
      },
    },
  );
  if (groupsEnvelope.field_4 === undefined)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Missing Parquet row-group list");
  markGeoMetadata(schema, key_value_metadata);
  return { ...metadata, row_groups };
}
