import type { ColumnMetaData, DecodedArray, Encoding, PageHeader } from "hyparquet";
import { Encodings, PageTypes } from "hyparquet/src/constants.js";
import { convert, convertWithDictionary, DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { decompressPage, readDataPage, readDataPageV2 } from "hyparquet/src/datapage.js";
import { readPlain } from "hyparquet/src/plain.js";
import { getSchemaPath, isFlatColumn } from "hyparquet/src/schema.js";
import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  type Batch,
  batchFromVectors,
  type Vector,
  vectorFromValues,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import {
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
} from "./decoded-column-cache.js";
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
  if (columns === undefined) return false;
  if (columns.length > 1 && options.rowStart !== undefined && options.rowEnd !== undefined) {
    const requestedRows = options.rowEnd - options.rowStart;
    if (requestedRows < (options.batchSize ?? 262_144)) return false;
  }
  for (const rowGroup of metadata.row_groups) {
    for (const column of columns) {
      const chunk = rowGroup.columns.find(
        (candidate) => candidate.meta_data?.path_in_schema.join(".") === column,
      );
      if (chunk?.meta_data === undefined || !canDirectVector(metadata, chunk.meta_data)) {
        return false;
      }
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
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  recordReadColumns(options.stats, columns);
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
    const columnMetadata = columns.map((column) => {
      const chunk = rowGroup.columns.find(
        (candidate) => candidate.meta_data?.path_in_schema.join(".") === column,
      );
      return chunk?.meta_data;
    });
    if (
      columnMetadata.some(
        (metadataForColumn) =>
          metadataForColumn === undefined || !canDirectVector(metadata, metadataForColumn),
      )
    ) {
      return;
    }
    recordRowGroupRead(options.stats);
    const start = Math.max(rowGroupStart, requestedStart);
    const end = Math.min(rowGroupEnd, requestedEnd);
    for await (const vectorBatch of readAlignedColumnVectorBatches(
      file,
      metadata,
      columnMetadata as ColumnMetaData[],
      columns,
      rowGroupStart,
      start,
      end,
      options,
    )) {
      recordRowsDecoded(options.stats, vectorBatch.batch.rowCount);
      yield vectorBatch;
    }
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

interface ColumnVectorCursor {
  column: string;
  iterator: AsyncIterator<ParquetVectorBatch>;
  current?: ParquetVectorBatch;
}

async function* readAlignedColumnVectorBatches(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  columnMetadata: ColumnMetaData[],
  columns: string[],
  rowGroupStart: number,
  requestedStart: number,
  requestedEnd: number,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetVectorBatch> {
  const cursors: ColumnVectorCursor[] = columnMetadata.map((metadataForColumn, index) => {
    const column = columns[index];
    if (column === undefined) throw new Error("Missing vector column");
    return {
      column,
      iterator: readColumnVectorBatches(
        file,
        metadata,
        metadataForColumn,
        column,
        rowGroupStart,
        requestedStart,
        requestedEnd,
        options,
      )[Symbol.asyncIterator](),
    };
  });
  for (const cursor of cursors) {
    const next = await cursor.iterator.next();
    if (next.done === true) return;
    cursor.current = next.value;
  }

  while (cursors.every((cursor) => cursor.current !== undefined)) {
    const rowOffset = Math.max(...cursors.map((cursor) => cursor.current?.rowOffset ?? 0));
    const rowEnd = Math.min(
      ...cursors.map((cursor) => {
        const current = cursor.current;
        return current === undefined
          ? Number.NEGATIVE_INFINITY
          : current.rowOffset + current.batch.rowCount;
      }),
    );
    if (rowOffset < rowEnd) {
      const vectors: Record<string, Vector> = {};
      for (const cursor of cursors) {
        const current = cursor.current;
        if (current === undefined) return;
        const vector = current.batch.columns[cursor.column];
        if (vector === undefined) return;
        vectors[cursor.column] = sliceVector(
          vector,
          rowOffset - current.rowOffset,
          rowEnd - current.rowOffset,
        );
      }
      yield { rowOffset, batch: batchFromVectors(vectors) };
    }

    for (const cursor of cursors) {
      const current = cursor.current;
      if (current === undefined) return;
      if (current.rowOffset + current.batch.rowCount <= rowEnd) {
        const next = await cursor.iterator.next();
        if (next.done === true) delete cursor.current;
        else cursor.current = next.value;
      }
    }
  }
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
  const chunkStart = safeNumber(
    columnMetadata.dictionary_page_offset ?? columnMetadata.data_page_offset,
  );
  const compressedSize = safeNumber(columnMetadata.total_compressed_size);
  if (chunkStart === undefined || compressedSize === undefined) return;
  const buffer = await file.slice(chunkStart, chunkStart + compressedSize);
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
    const pageOffset = chunkStart + reader.offset;
    const compressedBytes = new Uint8Array(
      reader.view.buffer,
      reader.view.byteOffset + reader.offset,
      header.compressed_page_size,
    );
    reader.offset += header.compressed_page_size;
    if (header.type === "DICTIONARY_PAGE") {
      dictionary = dictionaryPageValues(
        compressedBytes,
        header,
        columnDecoder,
        column,
        rowGroupStart,
        pageOffset,
        file,
        options,
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
    const start = Math.max(pageRowStart, requestedStart);
    const end = Math.min(pageRowEnd, requestedEnd);
    if (start < end) {
      const cache = options.decodedColumnCache;
      const key =
        cache === undefined || options.decodedColumnCacheKey === undefined
          ? undefined
          : decodedColumnPageCacheKey({
              path: options.decodedColumnCacheKey,
              byteLength: file.byteLength,
              ...(file.etag === undefined ? {} : { etag: file.etag }),
              column,
              rowGroupStart,
              pageRowStart,
              pageRowEnd,
              pageOffset,
              compressedPageSize: header.compressed_page_size,
            });
      const cached = key === undefined || cache === undefined ? undefined : cache.getVector(key);
      let vector: Vector;
      if (cached !== undefined) {
        vector = cached;
      } else {
        const page = dataPageValues(compressedBytes, header, columnDecoder, dictionary);
        if (page === undefined) continue;
        vector = flatPageVector(page.values, page.definitionLevels, 0, rowCount, page.dictionary);
        if (key !== undefined && cache !== undefined) cache.setVector(key, vector);
      }
      if (key !== undefined && options.stats !== undefined) {
        if (cached === undefined) options.stats.cacheMisses += 1;
        else options.stats.cacheHits += 1;
      }
      yield {
        rowOffset: start,
        batch: batchFromVectors({
          [column]: sliceVector(vector, start - pageRowStart, end - pageRowStart),
        }),
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

function dictionaryPageValues(
  compressedBytes: Uint8Array,
  header: PageHeader,
  columnDecoder: ColumnDecoder,
  column: string,
  rowGroupStart: number,
  pageOffset: number,
  file: StoreAsyncBuffer,
  options: ReadParquetBatchOptions,
): DecodedArray | undefined {
  const dictionaryHeader = header.dictionary_page_header;
  if (dictionaryHeader === undefined) return undefined;
  const cache = options.decodedColumnCache;
  const key =
    cache === undefined || options.decodedColumnCacheKey === undefined
      ? undefined
      : decodedDictionaryPageCacheKey({
          path: options.decodedColumnCacheKey,
          byteLength: file.byteLength,
          ...(file.etag === undefined ? {} : { etag: file.etag }),
          column,
          rowGroupStart,
          pageOffset,
          compressedPageSize: header.compressed_page_size,
          values: dictionaryHeader.num_values,
        });
  const cached =
    key === undefined || cache === undefined ? undefined : cache.getValue<DecodedArray>(key);
  if (cached !== undefined) {
    if (options.stats !== undefined) options.stats.cacheHits += 1;
    return cached;
  }
  const page = decompressPage(
    compressedBytes,
    Number(header.uncompressed_page_size),
    columnDecoder.codec,
    undefined,
  );
  const pageReader = {
    view: new DataView(page.buffer, page.byteOffset, page.byteLength),
    offset: 0,
  };
  const dictionary = convert(
    readPlain(
      pageReader,
      columnDecoder.type,
      dictionaryHeader.num_values,
      columnDecoder.element.type_length,
    ),
    columnDecoder,
  );
  if (key !== undefined && cache !== undefined) {
    cache.setValue(key, dictionary, estimateDecodedArrayBytes(dictionary));
    if (options.stats !== undefined) options.stats.cacheMisses += 1;
  }
  return dictionary;
}

function dataPageValues(
  compressedBytes: Uint8Array,
  header: PageHeader,
  columnDecoder: ColumnDecoder,
  dictionary: DecodedArray | undefined,
):
  | {
      rowCount: number;
      values: DecodedArray;
      definitionLevels: number[] | undefined;
      dictionary?: DecodedArray;
    }
  | undefined {
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
    const pageDictionary =
      dictionary !== undefined && isDictionaryEncoding(dataHeader.encoding)
        ? dictionary
        : undefined;
    return {
      rowCount: dataHeader.num_values,
      values:
        pageDictionary === undefined
          ? convertWithDictionary(dataPage, dictionary, dataHeader.encoding, columnDecoder)
          : dataPage,
      definitionLevels:
        definitionLevels === undefined || definitionLevels.length === 0
          ? undefined
          : definitionLevels,
      ...(pageDictionary === undefined ? {} : { dictionary: pageDictionary }),
    };
  }
  if (header.type === "DATA_PAGE_V2") {
    const dataHeader = header.data_page_header_v2;
    if (dataHeader === undefined) return undefined;
    const { definitionLevels, dataPage } = readDataPageV2(compressedBytes, header, columnDecoder);
    const pageDictionary =
      dictionary !== undefined && isDictionaryEncoding(dataHeader.encoding)
        ? dictionary
        : undefined;
    return {
      rowCount: dataHeader.num_rows,
      values:
        pageDictionary === undefined
          ? convertWithDictionary(dataPage, dictionary, dataHeader.encoding, columnDecoder)
          : dataPage,
      definitionLevels:
        definitionLevels === undefined || definitionLevels.length === 0
          ? undefined
          : definitionLevels,
      ...(pageDictionary === undefined ? {} : { dictionary: pageDictionary }),
    };
  }
  return undefined;
}

function estimateDecodedArrayBytes(values: DecodedArray): number {
  if (ArrayBuffer.isView(values)) return values.byteLength;
  let bytes = 0;
  for (const value of values) {
    if (typeof value === "string") bytes += value.length * 2;
    else if (typeof value === "bigint") bytes += 8;
    else if (typeof value === "number") bytes += 8;
    else if (typeof value === "boolean") bytes += 1;
  }
  return bytes;
}

function isDictionaryEncoding(encoding: Encoding): boolean {
  return encoding === "PLAIN_DICTIONARY" || encoding === "RLE_DICTIONARY";
}

function flatPageVector(
  values: DecodedArray,
  definitionLevels: readonly number[] | undefined,
  start: number,
  end: number,
  dictionary?: DecodedArray,
): Vector {
  if (dictionary !== undefined)
    return dictionaryPageVector(values, dictionary, definitionLevels, start, end);
  if (definitionLevels === undefined) return nonNullFlatVector(values, start, end);
  return nullableFlatVector(values, definitionLevels, start, end);
}

function sliceVector(vector: Vector, start: number, end: number): Vector {
  if (start === 0 && end === vectorLength(vector)) return vector;
  const valid =
    "valid" in vector && vector.valid !== undefined ? vector.valid.subarray(start, end) : undefined;
  switch (vector.type) {
    case "null":
      return { type: "null", length: end - start };
    case "f64":
      return optionalVectorValidity(
        { type: "f64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "i64":
      return optionalVectorValidity(
        { type: "i64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "bool":
      return optionalVectorValidity(
        { type: "bool", values: vector.values.subarray(start, end) },
        valid,
      );
    case "utf8":
      return optionalVectorValidity(
        { type: "utf8", values: vector.values.slice(start, end) },
        valid,
      );
    case "dict":
      return optionalVectorValidity(
        {
          type: "dict",
          indices: vector.indices.subarray(start, end),
          dictionary: vector.dictionary,
        },
        valid,
      );
    case "list":
    case "struct":
    case "map": {
      const values = [];
      for (let index = start; index < end; index += 1) values.push(vectorValue(vector, index));
      return vectorFromValues(values);
    }
  }
}

function dictionaryPageVector(
  values: DecodedArray,
  dictionary: DecodedArray,
  definitionLevels: readonly number[] | undefined,
  start: number,
  end: number,
): Vector {
  const dictionaryVector = nonNullFlatVector(dictionary, 0, dictionary.length);
  const length = end - start;
  const indices = new Uint32Array(length);
  if (definitionLevels === undefined) {
    for (let index = 0; index < length; index += 1) {
      indices[index] = Number(values[start + index] ?? 0);
    }
    return { type: "dict", indices, dictionary: dictionaryVector };
  }
  const valid = new Uint8Array(length);
  copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
    indices[outIndex] = Number(values[valueIndex] ?? 0);
  });
  return optionalVectorValidity({ type: "dict", indices, dictionary: dictionaryVector }, valid);
}

function nonNullFlatVector(values: DecodedArray, start: number, end: number): Vector {
  const length = end - start;
  if (values instanceof Float64Array) return { type: "f64", values: values.subarray(start, end) };
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
  if (values instanceof BigInt64Array) return { type: "i64", values: values.subarray(start, end) };
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
