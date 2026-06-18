import type { ColumnMetaData, DecodedArray, Encoding, PageHeader } from "hyparquet";
import { Encodings, PageTypes } from "hyparquet/src/constants.js";
import { convert, convertWithDictionary, DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { decompressPage, readDataPage, readDataPageV2 } from "hyparquet/src/datapage.js";
import { readPlain } from "hyparquet/src/plain.js";
import { getSchemaPath, isFlatColumn } from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { type Batch, batchFromVectors, type Vector } from "lakeql-core";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import type { ParquetMetadata, ReadParquetBatchOptions, StoreAsyncBuffer } from "./types.js";

export interface ParquetVectorBatch {
  rowOffset: number;
  batch: Batch;
}

export function canReadParquetVectorBatches(
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
): boolean {
  const columns = directVectorColumns(options.columns);
  const column = columns?.length === 1 ? columns[0] : undefined;
  if (column === undefined) return false;
  for (const rowGroup of metadata.row_groups) {
    const chunk = rowGroup.columns.find(
      (candidate) => candidate.meta_data?.path_in_schema.join(".") === column,
    );
    if (chunk?.meta_data === undefined || !canDirectVector(metadata, chunk.meta_data)) {
      return false;
    }
  }
  return true;
}

export async function* readParquetVectorBatchesFromFile(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetVectorBatch> {
  const columns = directVectorColumns(options.columns);
  if (columns === undefined) return;
  const column = columns.length === 1 ? columns[0] : undefined;
  if (column === undefined) return;
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  recordReadColumns(options.stats, [column]);
  let rowGroupStart = 0;
  for (let rowGroupIndex = 0; rowGroupIndex < metadata.row_groups.length; rowGroupIndex += 1) {
    const rowGroup = metadata.row_groups[rowGroupIndex];
    if (rowGroup === undefined) return;
    const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
    if (
      rowGroupEnd <= requestedStart ||
      rowGroupStart >= requestedEnd ||
      !rowGroupMayMatch(rowGroup, options.where)
    ) {
      recordRowGroupSkipped(options.stats);
      rowGroupStart = rowGroupEnd;
      continue;
    }
    const chunk = rowGroup.columns.find(
      (candidate) => candidate.meta_data?.path_in_schema.join(".") === column,
    );
    const columnMetadata = chunk?.meta_data;
    if (columnMetadata === undefined || !canDirectVector(metadata, columnMetadata)) return;
    recordRowGroupRead(options.stats);
    const start = Math.max(rowGroupStart, requestedStart);
    const end = Math.min(rowGroupEnd, requestedEnd);
    yield* readColumnVectorBatches(
      file,
      metadata,
      columnMetadata,
      column,
      rowGroupStart,
      start,
      end,
      options,
    );
    rowGroupStart = rowGroupEnd;
  }
}

function directVectorColumns(columns: readonly string[] | undefined): string[] | undefined {
  return columns !== undefined && columns.length > 0 ? [...columns] : undefined;
}

function canDirectVector(metadata: ParquetMetadata, column: ColumnMetaData): boolean {
  const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
  return isFlatColumn(schemaPath);
}

async function* readColumnVectorBatches(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  columnMetadata: ColumnMetaData,
  column: string,
  rowGroupStart: number,
  requestedStart: number,
  requestedEnd: number,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetVectorBatch> {
  const start = safeNumber(
    columnMetadata.dictionary_page_offset ?? columnMetadata.data_page_offset,
  );
  const compressedSize = safeNumber(columnMetadata.total_compressed_size);
  if (start === undefined || compressedSize === undefined) return;
  const buffer = await file.slice(start, start + compressedSize);
  const reader = { view: new DataView(buffer), offset: 0 };
  const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema);
  const leaf = schemaPath[schemaPath.length - 1];
  if (leaf === undefined) return;
  const columnDecoder = {
    pathInSchema: columnMetadata.path_in_schema,
    element: leaf.element,
    schemaPath,
    parsers: DEFAULT_PARSERS,
    ...columnMetadata,
  } satisfies ColumnDecoder;
  let dictionary: DecodedArray | undefined;
  let pageRowStart = rowGroupStart;
  while (reader.offset < reader.view.byteLength - 1 && pageRowStart < requestedEnd) {
    const header = parquetHeader(reader);
    const compressedBytes = new Uint8Array(
      reader.view.buffer,
      reader.view.byteOffset + reader.offset,
      header.compressed_page_size,
    );
    reader.offset += header.compressed_page_size;
    if (header.type === "DICTIONARY_PAGE") {
      const dictionaryHeader = header.dictionary_page_header;
      if (dictionaryHeader === undefined) continue;
      const page = decompressPage(
        compressedBytes,
        Number(header.uncompressed_page_size),
        columnMetadata.codec,
        undefined,
      );
      const pageReader = {
        view: new DataView(page.buffer, page.byteOffset, page.byteLength),
        offset: 0,
      };
      dictionary = convert(
        readPlain(
          pageReader,
          columnMetadata.type,
          dictionaryHeader.num_values,
          columnDecoder.element.type_length,
        ),
        columnDecoder,
      );
      continue;
    }
    const rowCount = dataPageRowCount(header);
    if (rowCount === undefined) continue;
    const pageRowEnd = pageRowStart + rowCount;
    if (pageRowEnd <= requestedStart || pageRowStart >= requestedEnd) {
      pageRowStart = pageRowEnd;
      continue;
    }
    const page = dataPageValues(compressedBytes, header, columnDecoder, dictionary);
    if (page === undefined) continue;
    const start = Math.max(pageRowStart, requestedStart);
    const end = Math.min(pageRowEnd, requestedEnd);
    if (start < end) {
      const batch = batchFromVectors({
        [column]: flatPageVector(
          page.values,
          page.definitionLevels,
          start - pageRowStart,
          end - pageRowStart,
        ),
      });
      recordRowsDecoded(options.stats, batch.rowCount);
      yield {
        rowOffset: start,
        batch,
      };
    }
    pageRowStart = pageRowEnd;
  }
}

function dataPageRowCount(header: PageHeader): number | undefined {
  if (header.type === "DATA_PAGE") return header.data_page_header?.num_values;
  if (header.type === "DATA_PAGE_V2") return header.data_page_header_v2?.num_rows;
  return undefined;
}

function dataPageValues(
  compressedBytes: Uint8Array,
  header: PageHeader,
  columnDecoder: ColumnDecoder,
  dictionary: DecodedArray | undefined,
): { rowCount: number; values: DecodedArray; definitionLevels: number[] | undefined } | undefined {
  if (header.type === "DATA_PAGE") {
    const dataHeader = header.data_page_header;
    if (dataHeader === undefined) return undefined;
    const page = decompressPage(
      compressedBytes,
      Number(header.uncompressed_page_size),
      columnDecoder.codec,
      undefined,
    );
    const { definitionLevels, dataPage } = readDataPage(page, dataHeader, columnDecoder);
    return {
      rowCount: dataHeader.num_values,
      values: convertWithDictionary(dataPage, dictionary, dataHeader.encoding, columnDecoder),
      definitionLevels:
        definitionLevels === undefined || definitionLevels.length === 0
          ? undefined
          : definitionLevels,
    };
  }
  if (header.type === "DATA_PAGE_V2") {
    const dataHeader = header.data_page_header_v2;
    if (dataHeader === undefined) return undefined;
    const { definitionLevels, dataPage } = readDataPageV2(compressedBytes, header, columnDecoder);
    return {
      rowCount: dataHeader.num_rows,
      values: convertWithDictionary(dataPage, dictionary, dataHeader.encoding, columnDecoder),
      definitionLevels:
        definitionLevels === undefined || definitionLevels.length === 0
          ? undefined
          : definitionLevels,
    };
  }
  return undefined;
}

function flatPageVector(
  values: DecodedArray,
  definitionLevels: readonly number[] | undefined,
  start: number,
  end: number,
): Vector {
  if (definitionLevels === undefined) return nonNullFlatVector(values, start, end);
  return nullableFlatVector(values, definitionLevels, start, end);
}

function nonNullFlatVector(values: DecodedArray, start: number, end: number): Vector {
  const length = end - start;
  if (values instanceof Float64Array) return { type: "f64", values: values.slice(start, end) };
  if (values instanceof Float32Array) {
    const out = new Float64Array(length);
    for (let index = 0; index < length; index += 1) out[index] = values[start + index] ?? 0;
    return { type: "f64", values: out };
  }
  if (
    values instanceof Int32Array ||
    values instanceof Uint32Array ||
    values instanceof Uint8Array
  ) {
    const out = new Float64Array(length);
    for (let index = 0; index < length; index += 1) out[index] = values[start + index] ?? 0;
    return { type: "f64", values: out };
  }
  if (values instanceof BigInt64Array) return { type: "i64", values: values.slice(start, end) };
  if (values instanceof BigUint64Array) {
    const out = new BigInt64Array(length);
    for (let index = 0; index < length; index += 1)
      out[index] = BigInt(values[start + index] ?? 0n);
    return { type: "i64", values: out };
  }
  return arrayFlatVector(values, start, end);
}

function nullableFlatVector(
  values: DecodedArray,
  definitionLevels: readonly number[],
  start: number,
  end: number,
): Vector {
  const length = end - start;
  const valid = new Uint8Array(length);
  if (
    values instanceof Float64Array ||
    values instanceof Float32Array ||
    values instanceof Int32Array ||
    values instanceof Uint32Array ||
    values instanceof Uint8Array
  ) {
    const out = new Float64Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = Number(values[valueIndex] ?? 0);
    });
    return optionalVectorValidity({ type: "f64", values: out }, valid);
  }
  if (values instanceof BigInt64Array || values instanceof BigUint64Array) {
    const out = new BigInt64Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = BigInt(values[valueIndex] ?? 0n);
    });
    return optionalVectorValidity({ type: "i64", values: out }, valid);
  }
  return nullableArrayFlatVector(values, definitionLevels, start, end, valid);
}

function copyNullableValues(
  definitionLevels: readonly number[],
  start: number,
  end: number,
  valid: Uint8Array,
  copy: (outIndex: number, valueIndex: number) => void,
): void {
  let valueIndex = 0;
  for (let row = 0; row < end; row += 1) {
    const present = definitionLevels[row] !== 0;
    if (row >= start) {
      const outIndex = row - start;
      valid[outIndex] = present ? 1 : 0;
      if (present) copy(outIndex, valueIndex);
    }
    if (present) valueIndex += 1;
  }
}

function arrayFlatVector(values: unknown[], start: number, end: number): Vector {
  const first = firstPresentArrayValue(values, start, end);
  switch (typeof first) {
    case "number": {
      const out = new Float64Array(end - start);
      for (let index = 0; index < out.length; index += 1) {
        out[index] = Number(values[start + index] ?? 0);
      }
      return { type: "f64", values: out };
    }
    case "bigint": {
      const out = new BigInt64Array(end - start);
      for (let index = 0; index < out.length; index += 1) {
        out[index] = bigintArrayValue(values[start + index]);
      }
      return { type: "i64", values: out };
    }
    case "boolean": {
      const out = new Uint8Array(end - start);
      for (let index = 0; index < out.length; index += 1) {
        out[index] = values[start + index] === true ? 1 : 0;
      }
      return { type: "bool", values: out };
    }
    case "string":
      return { type: "utf8", values: values.slice(start, end).map((value) => String(value ?? "")) };
    default:
      return { type: "null", length: end - start };
  }
}

function nullableArrayFlatVector(
  values: unknown[],
  definitionLevels: readonly number[],
  start: number,
  end: number,
  valid: Uint8Array,
): Vector {
  const first = firstPresentDefinitionValue(values, definitionLevels, start, end);
  const length = end - start;
  switch (typeof first) {
    case "number": {
      const out = new Float64Array(length);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = Number(values[valueIndex] ?? 0);
      });
      return optionalVectorValidity({ type: "f64", values: out }, valid);
    }
    case "bigint": {
      const out = new BigInt64Array(length);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = bigintArrayValue(values[valueIndex]);
      });
      return optionalVectorValidity({ type: "i64", values: out }, valid);
    }
    case "boolean": {
      const out = new Uint8Array(length);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = values[valueIndex] === true ? 1 : 0;
      });
      return optionalVectorValidity({ type: "bool", values: out }, valid);
    }
    case "string": {
      const out = new Array<string>(length);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = String(values[valueIndex] ?? "");
      });
      for (let index = 0; index < out.length; index += 1) {
        if (out[index] === undefined) out[index] = "";
      }
      return optionalVectorValidity({ type: "utf8", values: out }, valid);
    }
    default:
      return { type: "null", length };
  }
}

function firstPresentArrayValue(values: readonly unknown[], start: number, end: number): unknown {
  for (let index = start; index < end; index += 1) {
    const value = values[index];
    if (value != null) return value;
  }
  return null;
}

function firstPresentDefinitionValue(
  values: readonly unknown[],
  definitionLevels: readonly number[],
  start: number,
  end: number,
): unknown {
  let valueIndex = 0;
  for (let row = 0; row < end; row += 1) {
    if (definitionLevels[row] !== 0) {
      if (row >= start) return values[valueIndex];
      valueIndex += 1;
    }
  }
  return null;
}

function optionalVectorValidity<T extends Vector>(vector: T, valid: Uint8Array | undefined): T {
  if (valid === undefined) return vector;
  for (const value of valid) if (value === 0) return { ...vector, valid };
  return vector;
}

function bigintArrayValue(value: unknown): bigint {
  return typeof value === "bigint" ? value : 0n;
}

function parquetHeader(reader: { view: DataView; offset: number }): PageHeader {
  const header = deserializeTCompactProtocol(reader);
  return {
    type: PageTypes[header.field_1] as PageHeader["type"],
    uncompressed_page_size: header.field_2,
    compressed_page_size: header.field_3,
    ...(header.field_4 === undefined ? {} : { crc: header.field_4 }),
    ...(header.field_5 === undefined
      ? {}
      : {
          data_page_header: {
            num_values: header.field_5.field_1,
            encoding: Encodings[header.field_5.field_2] as Encoding,
            definition_level_encoding: Encodings[header.field_5.field_3] as Encoding,
            repetition_level_encoding: Encodings[header.field_5.field_4] as Encoding,
          },
        }),
    ...(header.field_7 === undefined
      ? {}
      : {
          dictionary_page_header: {
            num_values: header.field_7.field_1,
            encoding: Encodings[header.field_7.field_2] as Encoding,
            is_sorted: header.field_7.field_3,
          },
        }),
    ...(header.field_8 === undefined
      ? {}
      : {
          data_page_header_v2: {
            num_values: header.field_8.field_1,
            num_nulls: header.field_8.field_2,
            num_rows: header.field_8.field_3,
            encoding: Encodings[header.field_8.field_4] as Encoding,
            definition_levels_byte_length: header.field_8.field_5,
            repetition_levels_byte_length: header.field_8.field_6,
            is_compressed: header.field_8.field_7 === undefined ? true : header.field_8.field_7,
          },
        }),
  };
}

function safeNumber(value: bigint | number | undefined): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (typeof value === "bigint") {
    const numberValue = Number(value);
    if (Number.isSafeInteger(numberValue) && BigInt(numberValue) === value) return numberValue;
  }
  return undefined;
}
