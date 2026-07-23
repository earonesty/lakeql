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
  isTimestampValue,
  type Vector,
  vectorFromValues,
  vectorLength,
  vectorValue,
} from "lakeql-core";
import { readParquetColumnBatch } from "./column-batches.js";
import { lakeqlParquetCompressors } from "./compressors.js";
import {
  decodedColumnCacheKey,
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
} from "./decoded-column-cache.js";
import { lakeqlParquetParsers } from "./parsers.js";
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
  for (const rowGroup of metadata.row_groups) {
    for (const column of columns) {
      if (!canReadVectorColumn(metadata, rowGroup, column)) {
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
  const prefetchWindowSize =
    options.maxConcurrentReads !== undefined && options.maxConcurrentReads > 1
      ? options.maxConcurrentReads
      : undefined;
  const prefetchGroups =
    prefetchWindowSize !== undefined
      ? selectedDirectVectorRanges(metadata, columns, requestedStart, requestedEnd, options.where)
      : undefined;
  const prefetchedRowGroupIndexes =
    prefetchGroups === undefined
      ? undefined
      : new Set(prefetchGroups.map((group) => group.rowGroupIndex));
  let selectedGroupIndex = 0;
  let rowGroupStart = 0;
  for (let rowGroupIndex = 0; rowGroupIndex < metadata.row_groups.length; rowGroupIndex += 1) {
    const rowGroup = metadata.row_groups[rowGroupIndex];
    if (rowGroup === undefined) return;
    const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
    if (
      rowGroupEnd <= requestedStart ||
      rowGroupStart >= requestedEnd ||
      (prefetchedRowGroupIndexes === undefined
        ? !rowGroupMayMatch(rowGroup, options.where)
        : !prefetchedRowGroupIndexes.has(rowGroupIndex))
    ) {
      recordRowGroupSkipped(options.stats);
      rowGroupStart = rowGroupEnd;
      continue;
    }
    if (
      prefetchGroups !== undefined &&
      file.prefetch !== undefined &&
      prefetchWindowSize !== undefined &&
      selectedGroupIndex % prefetchWindowSize === 0
    ) {
      const window = prefetchGroups.slice(
        selectedGroupIndex,
        selectedGroupIndex + prefetchWindowSize,
      );
      await file.prefetch(window.flatMap((group) => group.ranges));
    }
    selectedGroupIndex += 1;
    const vectorSources = columnVectorSources(
      file,
      metadata,
      rowGroup,
      columns,
      rowGroupStart,
      rowGroupEnd,
      Math.max(rowGroupStart, requestedStart),
      Math.min(rowGroupEnd, requestedEnd),
      options,
    );
    if (vectorSources === undefined) return;
    recordRowGroupRead(options.stats);
    for await (const vectorBatch of readAlignedColumnVectorBatches(vectorSources)) {
      recordRowsDecoded(options.stats, vectorBatch.batch.rowCount);
      yield vectorBatch;
    }
    rowGroupStart = rowGroupEnd;
  }
}

function selectedDirectVectorRanges(
  metadata: ParquetMetadata,
  columns: readonly string[],
  requestedStart: number,
  requestedEnd: number,
  where: ReadParquetBatchOptions["where"],
): { rowGroupIndex: number; ranges: { start: number; end: number }[] }[] {
  const selected: { rowGroupIndex: number; ranges: { start: number; end: number }[] }[] = [];
  let rowGroupStart = 0;
  for (let rowGroupIndex = 0; rowGroupIndex < metadata.row_groups.length; rowGroupIndex += 1) {
    const rowGroup = metadata.row_groups[rowGroupIndex];
    if (rowGroup === undefined) break;
    const rowGroupEnd = rowGroupStart + Number(rowGroup.num_rows);
    if (
      rowGroupEnd > requestedStart &&
      rowGroupStart < requestedEnd &&
      rowGroupMayMatch(rowGroup, where)
    ) {
      const ranges = columns.flatMap((column) => {
        const columnMetadata = directLeafColumnMetadata(rowGroup, column);
        if (columnMetadata === undefined || !canDirectVector(metadata, columnMetadata)) return [];
        const start = safeNumber(
          columnMetadata.dictionary_page_offset ?? columnMetadata.data_page_offset,
        );
        const length = safeNumber(columnMetadata.total_compressed_size);
        return start === undefined || length === undefined || length <= 0
          ? []
          : [{ start, end: start + length }];
      });
      selected.push({ rowGroupIndex, ranges });
    }
    rowGroupStart = rowGroupEnd;
  }
  return selected;
}

function directVectorColumns(columns: readonly string[] | undefined): string[] | undefined {
  return columns !== undefined && columns.length > 0 ? [...columns] : undefined;
}

type RowGroupMetadata = ParquetMetadata["row_groups"][number];

function canReadVectorColumn(
  metadata: ParquetMetadata,
  rowGroup: RowGroupMetadata,
  column: string,
): boolean {
  const leaf = directLeafColumnMetadata(rowGroup, column);
  if (leaf !== undefined) return canDirectVector(metadata, leaf);
  if (!canRepresentNestedVectorColumn(metadata, column)) return false;
  return rowGroup.columns.some((candidate) => candidate.meta_data?.path_in_schema[0] === column);
}

function directLeafColumnMetadata(
  rowGroup: RowGroupMetadata,
  column: string,
): ColumnMetaData | undefined {
  return rowGroup.columns.find(
    (candidate) => candidate.meta_data?.path_in_schema.join(".") === column,
  )?.meta_data;
}

function canRepresentNestedVectorColumn(metadata: ParquetMetadata, column: string): boolean {
  let schemaPath: ReturnType<typeof getSchemaPath>;
  try {
    schemaPath = getSchemaPath(metadata.schema, [column]);
  } catch {
    return false;
  }
  const node = schemaPath.at(-1);
  if (node === undefined || isFlatColumn(schemaPath)) return false;
  const element = node.element;
  const logicalType = element.logical_type;
  return (
    element.converted_type === "LIST" ||
    element.converted_type === "MAP" ||
    element.converted_type === "MAP_KEY_VALUE" ||
    logicalTypeName(logicalType) === "LIST" ||
    logicalTypeName(logicalType) === "MAP"
  );
}

function logicalTypeName(logicalType: unknown): string | undefined {
  if (typeof logicalType === "string") return logicalType;
  if (typeof logicalType !== "object" || logicalType === null) return undefined;
  if ("type" in logicalType && typeof logicalType.type === "string") return logicalType.type;
  for (const key of ["LIST", "MAP"]) {
    if (key in logicalType) return key;
  }
  return undefined;
}

function canDirectVector(metadata: ParquetMetadata, column: ColumnMetaData): boolean {
  if (usesDictionaryEncoding(column) && column.dictionary_page_offset === undefined) return false;
  const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
  if (!isFlatColumn(schemaPath)) return false;
  const leaf = schemaPath[schemaPath.length - 1]?.element;
  if (leaf === undefined) return false;
  return canRepresentDirectVectorLeaf(leaf);
}

function canRepresentDirectVectorLeaf(leaf: ColumnDecoder["element"]): boolean {
  const logicalType = leaf.logical_type;
  if (
    leaf.converted_type === "TIMESTAMP_MILLIS" ||
    leaf.converted_type === "TIMESTAMP_MICROS" ||
    logicalType?.type === "TIMESTAMP"
  ) {
    return true;
  }
  if (
    leaf.converted_type === "UTF8" ||
    leaf.converted_type === "JSON" ||
    logicalType?.type === "STRING"
  ) {
    return true;
  }
  if (
    leaf.converted_type === "UINT_64" ||
    (logicalType?.type === "INTEGER" && logicalType.bitWidth === 64 && !logicalType.isSigned)
  ) {
    return false;
  }
  if (
    leaf.converted_type === "DATE" ||
    leaf.converted_type === "BSON" ||
    leaf.converted_type === "INTERVAL" ||
    logicalType?.type === "DATE" ||
    logicalType?.type === "UUID"
  ) {
    return false;
  }
  switch (leaf.type) {
    case "BOOLEAN":
    case "DOUBLE":
    case "FLOAT":
    case "INT32":
    case "INT64":
      return true;
    case "BYTE_ARRAY":
      return (
        (leaf.converted_type === undefined && logicalType === undefined) ||
        logicalType?.type === "GEOMETRY" ||
        logicalType?.type === "GEOGRAPHY"
      );
    default:
      return false;
  }
}

function usesDictionaryEncoding(column: ColumnMetaData): boolean {
  return (
    column.encodings?.some(isDictionaryEncoding) === true ||
    column.encoding_stats?.some((stats) => isDictionaryEncoding(stats.encoding)) === true
  );
}

interface VectorBatchSource {
  columns: readonly string[];
  iterator: AsyncIterator<ParquetVectorBatch>;
}

interface ColumnVectorCursor extends VectorBatchSource {
  current?: ParquetVectorBatch;
}

function columnVectorSources(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  rowGroup: RowGroupMetadata,
  columns: string[],
  rowGroupStart: number,
  rowGroupEnd: number,
  requestedStart: number,
  requestedEnd: number,
  options: ReadParquetBatchOptions,
): VectorBatchSource[] | undefined {
  const sources: VectorBatchSource[] = [];
  for (const column of columns) {
    const metadataForColumn = directLeafColumnMetadata(rowGroup, column);
    if (metadataForColumn !== undefined && canDirectVector(metadata, metadataForColumn)) {
      sources.push({
        columns: [column],
        iterator: readColumnVectorBatches(
          file,
          metadata,
          metadataForColumn,
          column,
          rowGroupStart,
          rowGroupEnd,
          requestedStart,
          requestedEnd,
          options,
        )[Symbol.asyncIterator](),
      });
      continue;
    }
    if (!canRepresentNestedVectorColumn(metadata, column)) return undefined;
    sources.push({
      columns: [column],
      iterator: readNestedColumnVectorBatches(
        file,
        metadata,
        column,
        requestedStart,
        requestedEnd,
        options,
      )[Symbol.asyncIterator](),
    });
  }
  return sources;
}

async function* readAlignedColumnVectorBatches(
  sources: VectorBatchSource[],
): AsyncIterable<ParquetVectorBatch> {
  const cursors: ColumnVectorCursor[] = sources.map((source) => ({ ...source }));
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
        for (const column of cursor.columns) {
          const vector = current.batch.columns[column];
          if (vector === undefined) return;
          vectors[column] = sliceVector(
            vector,
            rowOffset - current.rowOffset,
            rowEnd - current.rowOffset,
          );
        }
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

async function* readNestedColumnVectorBatches(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  column: string,
  requestedStart: number,
  requestedEnd: number,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetVectorBatch> {
  const batchSize = options.batchSize ?? 4096;
  for (let rowStart = requestedStart; rowStart < requestedEnd; rowStart += batchSize) {
    const rowEnd = Math.min(rowStart + batchSize, requestedEnd);
    const cache = options.decodedColumnCache;
    const key =
      cache === undefined || options.decodedColumnCacheKey === undefined
        ? undefined
        : decodedColumnCacheKey({
            path: options.decodedColumnCacheKey,
            byteLength: file.byteLength,
            ...(file.etag === undefined ? {} : { etag: file.etag }),
            columns: [column],
            rowStart,
            rowEnd,
          });
    const cached = key === undefined || cache === undefined ? undefined : cache.get(key);
    let batch: Batch;
    if (cached !== undefined) {
      batch = cached;
    } else {
      batch = await readParquetColumnBatch(file, metadata, [column], rowStart, rowEnd);
      if (key !== undefined && cache !== undefined) cache.set(key, batch);
    }
    if (key !== undefined && options.stats !== undefined) {
      if (cached === undefined) options.stats.cacheMisses += 1;
      else options.stats.cacheHits += 1;
    }
    yield { rowOffset: rowStart, batch };
  }
}

async function* readColumnVectorBatches(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  columnMetadata: ColumnMetaData,
  column: string,
  rowGroupStart: number,
  rowGroupEnd: number,
  requestedStart: number,
  requestedEnd: number,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetVectorBatch> {
  const chunkStart = safeNumber(
    columnMetadata.dictionary_page_offset ?? columnMetadata.data_page_offset,
  );
  const compressedSize = safeNumber(columnMetadata.total_compressed_size);
  if (chunkStart === undefined || compressedSize === undefined) return;
  if (
    options.canStopEarly === true ||
    requestedStart > rowGroupStart ||
    requestedEnd < rowGroupEnd
  ) {
    yield* readColumnWindowVectorBatches(
      file,
      metadata,
      columnMetadata,
      column,
      rowGroupStart,
      requestedStart,
      requestedEnd,
      chunkStart,
      chunkStart + compressedSize,
      options,
    );
    return;
  }
  const buffer = await file.slice(chunkStart, chunkStart + compressedSize);
  const reader = { view: new DataView(buffer), offset: 0 };
  const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema);
  const leaf = schemaPath[schemaPath.length - 1];
  if (leaf === undefined) return;
  const columnDecoder = {
    pathInSchema: columnMetadata.path_in_schema,
    element: leaf.element,
    schemaPath,
    parsers: { ...DEFAULT_PARSERS, ...lakeqlParquetParsers },
    compressors: lakeqlParquetCompressors,
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
      dictionary = recordDecodeTime(options, () =>
        dictionaryPageValues(
          compressedBytes,
          header,
          columnDecoder,
          column,
          rowGroupStart,
          pageOffset,
          file,
          options,
        ),
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
        const page = recordDecodeTime(options, () =>
          dataPageValues(compressedBytes, header, columnDecoder, dictionary),
        );
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

async function* readColumnWindowVectorBatches(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  columnMetadata: ColumnMetaData,
  column: string,
  rowGroupStart: number,
  requestedStart: number,
  requestedEnd: number,
  chunkStart: number,
  chunkEnd: number,
  options: ReadParquetBatchOptions,
): AsyncIterable<ParquetVectorBatch> {
  const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema);
  const leaf = schemaPath[schemaPath.length - 1];
  if (leaf === undefined) return;
  const columnDecoder = {
    pathInSchema: columnMetadata.path_in_schema,
    element: leaf.element,
    schemaPath,
    parsers: { ...DEFAULT_PARSERS, ...lakeqlParquetParsers },
    compressors: lakeqlParquetCompressors,
    ...columnMetadata,
  } satisfies ColumnDecoder;
  let dictionary: DecodedArray | undefined;
  let pageRowStart = rowGroupStart;
  let offset = 0;
  while (chunkStart + offset < chunkEnd && pageRowStart < requestedEnd) {
    const page = await readPageWindow(file, chunkStart, chunkEnd, offset);
    if (page === undefined) return;
    offset += page.headerBytes + page.header.compressed_page_size;
    if (page.header.type === "DICTIONARY_PAGE") {
      dictionary = recordDecodeTime(options, () =>
        dictionaryPageValues(
          page.compressedBytes,
          page.header,
          columnDecoder,
          column,
          rowGroupStart,
          page.bodyOffset,
          file,
          options,
        ),
      );
      continue;
    }
    const rowCount = dataPageRowCount(page.header);
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
              pageOffset: page.bodyOffset,
              compressedPageSize: page.header.compressed_page_size,
            });
      const cached = key === undefined || cache === undefined ? undefined : cache.getVector(key);
      let vector: Vector;
      if (cached !== undefined) {
        vector = cached;
      } else {
        const values = recordDecodeTime(options, () =>
          dataPageValues(page.compressedBytes, page.header, columnDecoder, dictionary),
        );
        if (values === undefined) continue;
        vector = flatPageVector(
          values.values,
          values.definitionLevels,
          0,
          rowCount,
          values.dictionary,
        );
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

interface PageWindow {
  header: PageHeader;
  headerBytes: number;
  bodyOffset: number;
  compressedBytes: Uint8Array;
}

async function readPageWindow(
  file: StoreAsyncBuffer,
  chunkStart: number,
  chunkEnd: number,
  offset: number,
): Promise<PageWindow | undefined> {
  const absoluteOffset = chunkStart + offset;
  if (absoluteOffset >= chunkEnd) return undefined;
  const header = await readPageHeader(file, absoluteOffset, chunkEnd);
  const bodyOffset = absoluteOffset + header.headerBytes;
  const bodyEnd = bodyOffset + header.header.compressed_page_size;
  if (bodyEnd > chunkEnd) return undefined;
  const body = await file.slice(bodyOffset, bodyEnd);
  return {
    ...header,
    bodyOffset,
    compressedBytes: new Uint8Array(body),
  };
}

async function readPageHeader(
  file: StoreAsyncBuffer,
  absoluteOffset: number,
  chunkEnd: number,
): Promise<{ header: PageHeader; headerBytes: number }> {
  let size = 256;
  let lastError: unknown;
  while (absoluteOffset + size <= chunkEnd || size === 256) {
    const end = Math.min(chunkEnd, absoluteOffset + size);
    const bytes = await file.slice(absoluteOffset, end);
    const reader = { view: new DataView(bytes), offset: 0 };
    try {
      const header = parquetHeader(reader);
      return { header, headerBytes: reader.offset };
    } catch (cause) {
      lastError = cause;
      if (end === chunkEnd) break;
      size *= 4;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Unable to read Parquet page header");
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
    columnDecoder.compressors,
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
      columnDecoder.compressors,
    );
    const { definitionLevels, dataPage } = readDataPage(page, dataHeader, columnDecoder);
    const compactDataPage = compactPresentValues(dataPage);
    const pageDictionary =
      dictionary !== undefined && isDictionaryEncoding(dataHeader.encoding)
        ? dictionary
        : undefined;
    return {
      rowCount: dataHeader.num_values,
      values:
        pageDictionary === undefined
          ? convertWithDictionary(compactDataPage, dictionary, dataHeader.encoding, columnDecoder)
          : compactDataPage,
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
    const compactDataPage = compactPresentValues(dataPage);
    const pageDictionary =
      dictionary !== undefined && isDictionaryEncoding(dataHeader.encoding)
        ? dictionary
        : undefined;
    return {
      rowCount: dataHeader.num_rows,
      values:
        pageDictionary === undefined
          ? convertWithDictionary(compactDataPage, dictionary, dataHeader.encoding, columnDecoder)
          : compactDataPage,
      definitionLevels:
        definitionLevels === undefined || definitionLevels.length === 0
          ? undefined
          : definitionLevels,
      ...(pageDictionary === undefined ? {} : { dictionary: pageDictionary }),
    };
  }
  return undefined;
}

function recordDecodeTime<T>(options: ReadParquetBatchOptions, decode: () => T): T {
  const startedAt = options.now?.();
  try {
    return decode();
  } finally {
    if (startedAt !== undefined && options.stats !== undefined && options.now !== undefined) {
      options.stats.decodeMs = (options.stats.decodeMs ?? 0) + (options.now() - startedAt);
    }
  }
}

function compactPresentValues(values: DecodedArray): DecodedArray {
  if (!Array.isArray(values)) return values;
  return values.filter((value) => value !== null && value !== undefined);
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
    case "f32":
      return optionalVectorValidity(
        { type: "f32", values: vector.values.subarray(start, end) },
        valid,
      );
    case "f64":
      return optionalVectorValidity(
        { type: "f64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "i32":
      return optionalVectorValidity(
        { type: "i32", values: vector.values.subarray(start, end) },
        valid,
      );
    case "u32":
      return optionalVectorValidity(
        { type: "u32", values: vector.values.subarray(start, end) },
        valid,
      );
    case "u8":
      return optionalVectorValidity(
        { type: "u8", values: vector.values.subarray(start, end) },
        valid,
      );
    case "i64":
      return optionalVectorValidity(
        { type: "i64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "timestamp":
      return optionalVectorValidity(
        {
          type: "timestamp",
          values: vector.values.subarray(start, end),
          unit: vector.unit,
          isAdjustedToUTC: vector.isAdjustedToUTC,
        },
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
    case "binary":
      return optionalVectorValidity(
        { type: "binary", values: vector.values.slice(start, end) },
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
  if (values instanceof Float32Array) return { type: "f32", values: values.subarray(start, end) };
  if (values instanceof Int32Array) return { type: "i32", values: values.subarray(start, end) };
  if (values instanceof Uint32Array) return { type: "u32", values: values.subarray(start, end) };
  if (values instanceof Uint8Array) return { type: "u8", values: values.subarray(start, end) };
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
  if (values instanceof Float64Array) {
    const out = new Float64Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = Number(values[valueIndex] ?? 0);
    });
    return optionalVectorValidity({ type: "f64", values: out }, valid);
  }
  if (values instanceof Float32Array) {
    const out = new Float32Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "f32", values: out }, valid);
  }
  if (values instanceof Int32Array) {
    const out = new Int32Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "i32", values: out }, valid);
  }
  if (values instanceof Uint32Array) {
    const out = new Uint32Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "u32", values: out }, valid);
  }
  if (values instanceof Uint8Array) {
    const out = new Uint8Array(length);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "u8", values: out }, valid);
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
    case "object":
      if (isTimestampValue(first)) return vectorFromValues(values.slice(start, end));
      if (isBinaryValue(first)) {
        return {
          type: "binary",
          values: values.slice(start, end).map((value) => binaryArrayValue(value)),
        };
      }
      return { type: "null", length: end - start };
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
    case "object": {
      if (isBinaryValue(first)) {
        const out = new Array<Uint8Array>(length);
        copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
          out[outIndex] = binaryArrayValue(values[valueIndex]);
        });
        for (let index = 0; index < out.length; index += 1) {
          if (out[index] === undefined) out[index] = new Uint8Array();
        }
        return optionalVectorValidity({ type: "binary", values: out }, valid);
      }
      if (!isTimestampValue(first)) return { type: "null", length };
      const out = new Array<unknown>(length);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = values[valueIndex];
      });
      return optionalVectorValidity(vectorFromValues(out), valid);
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

function isBinaryValue(value: unknown): boolean {
  return value instanceof Uint8Array || value instanceof ArrayBuffer || ArrayBuffer.isView(value);
}

function binaryArrayValue(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return new Uint8Array();
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
