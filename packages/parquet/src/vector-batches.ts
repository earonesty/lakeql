import type { ColumnMetaData, DecodedArray, Encoding, PageHeader } from "hyparquet";
import { convertWithDictionary, DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { decompressPage, readDataPage, readDataPageV2 } from "hyparquet/src/datapage.js";
import { isFlatColumn } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  type Batch,
  batchFromVectors,
  closeAsyncIterators,
  isTimestampValue,
  LakeqlError,
  type MemoryBudget,
  MemoryScope,
  MemoryStageScheduler,
  type OwnedMemory,
  type PackedByteVector,
  sliceVector,
  type Vector,
  vectorFromSource,
} from "lakeql-core";
import { cachedPageWindow } from "./cached-page-window.js";
import { dictionaryPageOffset } from "./dictionary-offset.js";
import {
  INITIAL_PAGE_HEADER_BYTES,
  type PreparedPageWindow,
  parquetHeader,
  preparePageWindow,
  readPreparedPageBody,
} from "./page-window.js";
import { recoverColumnExtents } from "./recover-column-extents.js";

export {
  type PreparedPageWindow,
  preparePageWindow,
  readPreparedPageBody,
  validatePreparedPageBody,
} from "./page-window.js";

import { parquetBatchSize } from "./batch-size.js";
import { readPackedBinaryValues } from "./binary-values.js";
import { readParquetColumnBatch } from "./column-batches.js";
import { scopedParquetCompressors } from "./compressors.js";
import { isIntegerDecimal } from "./decimal.js";
import {
  allocateDecodedArray,
  compactDecodedValues,
  decodedBinaryValue,
  decodedStringValue,
} from "./decoded-array.js";
import {
  type DecodedColumnCache,
  decodedColumnCacheKey,
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
} from "./decoded-column-cache.js";
import { logicalPageVector } from "./logical-page-vector.js";
import { convertDecodedValues } from "./logical-values.js";
import { planFlatNumericPage, planFlatNumericPageHeader, readNumericPage } from "./numeric-page.js";
import {
  prepareNumericColumnPages,
  readAlignedNumericPageGroups,
} from "./numeric-page-alignment.js";
import { lakeqlParquetParsers } from "./parsers.js";
import { readPlainValues } from "./plain-values.js";
import { validateParquetReadConcurrency } from "./read-concurrency.js";
import {
  recordReadColumns,
  recordRowGroupRead,
  recordRowGroupSkipped,
  recordRowsDecoded,
} from "./read-metrics.js";
import { ParquetReadScope } from "./read-scope.js";
import { rowGroupMayMatch } from "./row-group-pruning.js";
import { schemaPathIndex } from "./schema-path-index.js";
import type { ParquetMetadata, ReadParquetBatchOptions, StoreAsyncBuffer } from "./types.js";

const numericSchedulers = new WeakMap<MemoryBudget, MemoryStageScheduler>();
type ParquetDictionary = DecodedArray | PackedByteVector;
function isPackedDictionary(value: ParquetDictionary): value is PackedByteVector {
  return "type" in value && (value.type === "binary-view" || value.type === "utf8-view");
}

export interface ParquetVectorBatch {
  rowOffset: number;
  batch: Batch;
}

// Private consumer context: never attach an index to caller-owned/cache metadata.
// The lazy tree dies with the capability check or scan and is shared by its
// prefetch, routing and decoding helpers.
const scanSchemaLookup = Symbol("scanSchemaLookup");
type ScanMetadata = ParquetMetadata & { [scanSchemaLookup]?: ReturnType<typeof schemaPathIndex> };
function scanMetadata(metadata: ParquetMetadata): ScanMetadata {
  if ((metadata as ScanMetadata)[scanSchemaLookup] !== undefined) return metadata;
  let lookup: ReturnType<typeof schemaPathIndex> | undefined;
  return {
    ...metadata,
    [scanSchemaLookup]: (path) => {
      lookup ??= schemaPathIndex(metadata.schema);
      return lookup(path);
    },
  };
}
function getSchemaPath(metadata: ParquetMetadata, path: readonly string[]) {
  const lookup = (metadata as ScanMetadata)[scanSchemaLookup];
  if (lookup === undefined) throw new Error("Missing scan schema context");
  return lookup(path);
}

export function canReadParquetVectorBatches(
  metadata: ParquetMetadata,
  options: ReadParquetBatchOptions,
  capabilities: { nested?: boolean } = {},
): boolean {
  metadata = scanMetadata(metadata);
  const columns = directVectorColumns(options.columns);
  if (columns === undefined) return false;
  for (const rowGroup of metadata.row_groups) {
    for (const column of columns) {
      if (capabilities.nested === false) {
        const leaf = directLeafColumnMetadata(rowGroup, column);
        if (!leaf || !isFlatColumn(getSchemaPath(metadata, leaf.path_in_schema))) return false;
      }
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
  validateParquetReadConcurrency(options);
  metadata = scanMetadata(metadata);
  const batchSize = parquetBatchSize(options.batchSize);
  const columns = directVectorColumns(options.columns);
  if (columns === undefined) return;
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  recordReadColumns(options.stats, columns);
  // Early-stop decoding requests pages on demand. Whole-chunk speculation would
  // transfer unread pages before the consumer can terminate the scan.
  const prefetchWindowSize =
    options.canStopEarly !== true &&
    options.maxConcurrentReads !== undefined &&
    options.maxConcurrentReads > 1
      ? options.maxConcurrentReads
      : undefined;
  const prefetchGroups =
    prefetchWindowSize !== undefined && file.prefetch !== undefined
      ? iterateDirectVectorRanges(metadata, columns, requestedStart, requestedEnd, options.where)
      : undefined;
  let selectedGroupIndex = 0;
  let rowGroupStart = 0;
  for (let rowGroupIndex = 0; rowGroupIndex < metadata.row_groups.length; rowGroupIndex += 1) {
    let rowGroup = metadata.row_groups[rowGroupIndex];
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
    const recovered = await recoverColumnExtents(
      file,
      metadata,
      { columns },
      options.allocationScope ?? options.decompressionScope,
      () => metadata.row_groups,
      rowGroupIndex,
    );
    rowGroup = recovered.row_groups[rowGroupIndex] ?? rowGroup;
    if (
      prefetchGroups !== undefined &&
      file.prefetch !== undefined &&
      prefetchWindowSize !== undefined &&
      selectedGroupIndex % prefetchWindowSize === 0
    ) {
      const ranges: { start: number; end: number }[] = [];
      for (let index = 0; index < prefetchWindowSize; index += 1) {
        const next = prefetchGroups.next();
        if (next.done) break;
        ranges.push(...next.value.ranges);
      }
      await file.prefetch(ranges);
    }
    selectedGroupIndex += 1;
    const numericSources = groupedNumericSources(
      file,
      metadata,
      rowGroup,
      columns,
      Math.max(0, requestedStart - rowGroupStart),
      Math.min(rowGroupEnd, requestedEnd) - rowGroupStart,
      options,
    );
    if (numericSources !== undefined && options.allocationScope !== undefined) {
      const scope = options.allocationScope;
      const pool = scope.budget.allocationPool;
      let scheduler = numericSchedulers.get(pool);
      if (scheduler === undefined) {
        scheduler = new MemoryStageScheduler(pool);
        numericSchedulers.set(pool, scheduler);
      }
      recordRowGroupRead(options.stats);
      for await (const owner of readAlignedNumericPageGroups(file, numericSources, scope, {
        batchSize,
        scheduler,
        ...((options.maxConcurrentPageReads ?? options.maxConcurrentReads) === undefined
          ? {}
          : { maxConcurrentReads: options.maxConcurrentPageReads ?? options.maxConcurrentReads }),
        rowStart: Math.max(0, requestedStart - rowGroupStart),
        rowEnd: Math.min(rowGroupEnd, requestedEnd) - rowGroupStart,
        decode: (work) => recordDecodeTime(options, work),
        onCacheLookup(hit) {
          if (options.stats !== undefined) {
            if (hit) options.stats.cacheHits++;
            else options.stats.cacheMisses++;
          }
        },
        ...(options.decodedColumnCache === undefined || options.decodedColumnCacheKey === undefined
          ? {}
          : {
              decodedCache: {
                cache: options.decodedColumnCache,
                path: options.decodedColumnCacheKey,
                rowGroupStart,
              },
            }),
      })) {
        const pin = scope.adopt(owner);
        try {
          recordRowsDecoded(options.stats, owner.value.batch.rowCount);
          yield { rowOffset: rowGroupStart + owner.value.rowOffset, batch: owner.value.batch };
        } finally {
          pin.release();
        }
      }
      rowGroupStart = rowGroupEnd;
      continue;
    }
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
    for await (const vectorBatch of readAlignedColumnVectorBatches(vectorSources, batchSize)) {
      recordRowsDecoded(options.stats, vectorBatch.batch.rowCount);
      yield vectorBatch;
    }
    rowGroupStart = rowGroupEnd;
  }
}

export function* iterateDirectVectorRanges(
  metadata: ParquetMetadata,
  columns: readonly string[],
  requestedStart: number,
  requestedEnd: number,
  where: ReadParquetBatchOptions["where"],
  groups: Iterable<ParquetMetadata["row_groups"][number]> = metadata.row_groups,
): Generator<{ rowGroupIndex: number; ranges: { start: number; end: number }[] }> {
  metadata = scanMetadata(metadata);
  let rowGroupStart = 0;
  let nextIndex = 0;
  for (const rowGroup of groups) {
    const rowGroupIndex = nextIndex++;
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
          dictionaryPageOffset(columnMetadata) ?? columnMetadata.data_page_offset,
        );
        const length = safeNumber(columnMetadata.total_compressed_size);
        return start === undefined || length === undefined || length <= 0
          ? []
          : [{ start, end: start + length }];
      });
      yield { rowGroupIndex, ranges };
    }
    rowGroupStart = rowGroupEnd;
    if (rowGroupStart >= requestedEnd) break;
  }
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
    schemaPath = getSchemaPath(metadata, [column]);
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
  const schemaPath = getSchemaPath(metadata, column.path_in_schema);
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
    return true;
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
    case "FIXED_LEN_BYTE_ARRAY":
      return leaf.converted_type === undefined && logicalType === undefined;
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

function isUnsigned64Leaf(leaf: ColumnDecoder["element"]): boolean {
  return (
    leaf.converted_type === "UINT_64" ||
    (leaf.logical_type?.type === "INTEGER" &&
      leaf.logical_type.bitWidth === 64 &&
      !leaf.logical_type.isSigned)
  );
}

function usesDictionaryEncoding(column: ColumnMetaData): boolean {
  return (
    column.encodings?.some(isDictionaryEncoding) === true ||
    column.encoding_stats?.some((stats) => isDictionaryEncoding(stats.encoding)) === true
  );
}

/** Internal cursor lease; public batches retain their independent owners. */
interface ColumnVectorBatch extends ParquetVectorBatch {
  releaseConsumedPage?: () => void;
}

interface VectorBatchSource {
  columns: readonly string[];
  iterator: AsyncIterator<ColumnVectorBatch>;
}

interface ColumnVectorCursor extends VectorBatchSource {
  current?: ColumnVectorBatch;
}

/** Metadata capability gate: every selected column must have a bounded flat
 * numeric plan. Unsupported encodings and logical shapes keep their normal reader.
 */
function groupedNumericSources(
  file: StoreAsyncBuffer,
  metadata: ParquetMetadata,
  rowGroup: RowGroupMetadata,
  columns: string[],
  rowStart: number,
  rowEnd: number,
  options: ReadParquetBatchOptions,
) {
  const scope = options.allocationScope;
  if (
    scope === undefined ||
    options.pageAllocationLifetime !== "iteration" ||
    file.sliceOwned === undefined ||
    file.ownedReadAllocationBytes === undefined ||
    // Single-column scans have no peer replacement contention and benefit from
    // the existing adaptive whole-chunk path when headroom permits.
    columns.length < 2
  )
    return undefined;
  const sources = [];
  let wholeChunkEstimate = 0;
  for (const column of columns) {
    const info = directLeafColumnMetadata(rowGroup, column);
    if (
      info === undefined ||
      !canDirectVector(metadata, info) ||
      info.dictionary_page_offset !== undefined ||
      !["UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP", "LZ4_RAW", "LZ4"].includes(info.codec) ||
      !["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE"].includes(info.type) ||
      info.encodings.length === 0 ||
      info.encodings.some((encoding) => !["PLAIN", "BYTE_STREAM_SPLIT", "RLE"].includes(encoding))
    )
      return undefined;
    const schemaPath = getSchemaPath(metadata, info.path_in_schema);
    const leaf = schemaPath.at(-1);
    if (leaf === undefined) return undefined;
    const decoder: ColumnDecoder = {
      ...info,
      pathInSchema: info.path_in_schema,
      schemaPath,
      element: leaf.element,
      parsers: { ...DEFAULT_PARSERS, ...lakeqlParquetParsers },
      utf8: false,
    };
    if (flatNumericVectorDecimal(decoder) === undefined) return undefined;
    const start = safeNumber(info.data_page_offset);
    const size = safeNumber(info.total_compressed_size);
    if (start === undefined || size === undefined || !Number.isSafeInteger(start + size))
      return undefined;
    const uncompressed = safeNumber(info.total_uncompressed_size);
    // Routing estimate, not an allocation exemption: include encoded backing,
    // decoded bytes and full-width converted values plus validity. Actual reads
    // and decodes still admit every allocation through their existing scopes.
    wholeChunkEstimate += size + (uncompressed ?? Infinity) + Number(rowGroup.num_rows) * 17;
    sources.push({
      column,
      pages: prepareNumericColumnPages(
        file,
        decoder,
        { start, end: start + size },
        scope,
        {
          start: rowStart,
          end: rowEnd,
        },
        options.decodedColumnCache && options.decodedColumnCacheKey !== undefined
          ? { cache: options.decodedColumnCache, path: options.decodedColumnCacheKey }
          : undefined,
      ),
    });
  }
  if (
    options.canStopEarly !== true &&
    rowStart === 0 &&
    rowEnd === Number(rowGroup.num_rows) &&
    Number.isSafeInteger(wholeChunkEstimate) &&
    wholeChunkEstimate <= scope.budget.availableBytes / 2
  )
    return undefined;
  return sources;
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
  batchSize: number,
): AsyncIterable<ParquetVectorBatch> {
  const cursors: ColumnVectorCursor[] = sources.map((source) => ({ ...source }));
  try {
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
      for (let batchStart = rowOffset; batchStart < rowEnd; batchStart += batchSize) {
        const batchEnd = Math.min(batchStart + batchSize, rowEnd);
        const vectors: Record<string, Vector> = Object.create(null);
        for (const cursor of cursors) {
          const current = cursor.current;
          if (current === undefined) return;
          for (const column of cursor.columns) {
            const vector = current.batch.columns[column];
            if (vector === undefined) return;
            vectors[column] = sliceVector(
              vector,
              batchStart - current.rowOffset,
              batchEnd - current.rowOffset,
            );
          }
        }
        yield { rowOffset: batchStart, batch: batchFromVectors(vectors) };
      }

      // Release every exhausted peer before any replacement decode. Otherwise
      // the first advancing column competes with consumed pages in later cursors.
      for (const cursor of cursors) {
        const current = cursor.current;
        if (current !== undefined && current.rowOffset + current.batch.rowCount <= rowEnd)
          current.releaseConsumedPage?.();
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
  } finally {
    await closeAsyncIterators(cursors.map((cursor) => cursor.iterator));
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
    const cached =
      key === undefined || cache === undefined
        ? undefined
        : cache.get(key, options.allocationScope);
    let batch: Batch;
    if (cached !== undefined) {
      batch = cached;
    } else {
      batch = await readParquetColumnBatch(
        file,
        metadata,
        [column],
        rowStart,
        rowEnd,
        options.allocationScope ?? options.decompressionScope,
      );
      if (key !== undefined && cache !== undefined) cache.set(key, batch, options.allocationScope);
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
): AsyncIterable<ColumnVectorBatch> {
  const chunkStart = safeNumber(
    dictionaryPageOffset(columnMetadata) ?? columnMetadata.data_page_offset,
  );
  const compressedSize = safeNumber(columnMetadata.total_compressed_size);
  if (chunkStart === undefined || compressedSize === undefined) return;
  if (
    // On a transport known to return the whole object for every range, splitting
    // a chunk into header/payload requests amplifies transfer. Still use bounded
    // page windows below when the chunk cannot fit current allocation headroom.
    (options.canStopEarly === true && options.rangeReadMode !== "emulated") ||
    // Keep headroom for decode and other active columns instead of admitting an
    // entire compressed chunk when the shared pool is already under pressure.
    (options.adaptiveReadWindows === true &&
      compressedSize > INITIAL_PAGE_HEADER_BYTES &&
      file.sliceOwned !== undefined &&
      options.pageAllocationLifetime === "iteration" &&
      options.allocationScope !== undefined &&
      compressedSize > Math.floor(options.allocationScope.budget.availableBytes / 2)) ||
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
  const schemaPath = getSchemaPath(metadata, columnMetadata.path_in_schema);
  const leaf = schemaPath[schemaPath.length - 1];
  if (leaf === undefined) return;
  const columnDecoder = {
    pathInSchema: columnMetadata.path_in_schema,
    element: leaf.element,
    schemaPath,
    parsers: { ...DEFAULT_PARSERS, ...lakeqlParquetParsers },
    utf8: false,
    compressors: scopedParquetCompressors(options.allocationScope ?? options.decompressionScope),
    ...columnMetadata,
  } satisfies ColumnDecoder;
  let dictionary: ParquetDictionary | undefined;
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
      const pageScope =
        options.allocationScope === undefined || options.pageAllocationLifetime !== "iteration"
          ? undefined
          : options.allocationScope.childScope();
      const pageDecoder =
        pageScope === undefined
          ? columnDecoder
          : {
              ...columnDecoder,
              compressors: scopedParquetCompressors(pageScope),
            };
      try {
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
        const cached =
          key === undefined || cache === undefined
            ? undefined
            : cache.getVector(key, pageScope ?? options.allocationScope);
        let vector: Vector;
        if (cached !== undefined) {
          vector = cached;
        } else {
          const decoded = recordDecodeTime(options, () =>
            decodeFlatPageVector(
              compressedBytes,
              header,
              pageDecoder,
              dictionary,
              pageScope ?? options.allocationScope,
            ),
          );
          if (decoded === undefined) continue;
          vector = decoded;
          if (key !== undefined && cache !== undefined)
            cachePageVector(cache, key, vector, pageScope, options.allocationScope);
        }
        if (key !== undefined && options.stats !== undefined) {
          if (cached === undefined) options.stats.cacheMisses += 1;
          else options.stats.cacheHits += 1;
        }
        const pagePin =
          pageScope === undefined || options.allocationScope === undefined
            ? undefined
            : options.allocationScope.adopt(pageScope.retainAllocations(vector));
        try {
          yield {
            ...(pageScope === undefined
              ? {}
              : {
                  releaseConsumedPage() {
                    pagePin?.release();
                    pageScope.dispose();
                  },
                }),
            rowOffset: start,
            batch: batchFromVectors({
              [column]: sliceVector(vector, start - pageRowStart, end - pageRowStart),
            }),
          };
        } finally {
          pagePin?.release();
        }
      } finally {
        pageScope?.dispose();
      }
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
): AsyncIterable<ColumnVectorBatch> {
  const schemaPath = getSchemaPath(metadata, columnMetadata.path_in_schema);
  const leaf = schemaPath[schemaPath.length - 1];
  if (leaf === undefined) return;
  const columnDecoder = {
    pathInSchema: columnMetadata.path_in_schema,
    element: leaf.element,
    schemaPath,
    parsers: { ...DEFAULT_PARSERS, ...lakeqlParquetParsers },
    utf8: false,
    compressors: scopedParquetCompressors(options.allocationScope ?? options.decompressionScope),
    ...columnMetadata,
  } satisfies ColumnDecoder;
  let dictionary: ParquetDictionary | undefined;
  let pageRowStart = rowGroupStart;
  let offset = 0;
  while (chunkStart + offset < chunkEnd && pageRowStart < requestedEnd) {
    const transport =
      options.pageAllocationLifetime === "iteration" &&
      options.allocationScope !== undefined &&
      file.sliceOwned !== undefined
        ? new ParquetReadScope(file, options.allocationScope)
        : undefined;
    let transportPin: OwnedMemory<PageWindow> | undefined;
    try {
      let pageCacheKey: string | undefined;
      let cachedPageVector: Vector | undefined;
      let cachedDictionary: ParquetDictionary | undefined;
      const page = await readPageWindow(
        transport?.file ?? file,
        chunkStart,
        chunkEnd,
        offset,
        (prepared) => {
          const header = prepared.header;
          if (header.type === "DICTIONARY_PAGE") {
            const key = dictionaryCacheKey(
              header,
              column,
              rowGroupStart,
              prepared.bodyOffset,
              file,
              options,
            );
            if (key !== undefined)
              cachedDictionary = options.decodedColumnCache?.getValue<ParquetDictionary>(
                key,
                options.allocationScope,
              );
            return cachedDictionary === undefined;
          }
          const rows = dataPageRowCount(header);
          if (
            rows === undefined ||
            pageRowStart + rows <= requestedStart ||
            pageRowStart >= requestedEnd
          )
            return false;
          if (options.decodedColumnCache && options.decodedColumnCacheKey !== undefined) {
            pageCacheKey = decodedColumnPageCacheKey({
              path: options.decodedColumnCacheKey,
              byteLength: file.byteLength,
              ...(file.etag === undefined ? {} : { etag: file.etag }),
              column,
              rowGroupStart,
              pageRowStart,
              pageRowEnd: pageRowStart + rows,
              pageOffset: prepared.bodyOffset,
              compressedPageSize: header.compressed_page_size,
            });
            cachedPageVector = options.decodedColumnCache.getVector(
              pageCacheKey,
              transport?.allocations ?? options.allocationScope,
            );
          }
          return cachedPageVector === undefined;
        },
        transport?.allocations ?? options.allocationScope,
        options.decodedColumnCache && options.decodedColumnCacheKey !== undefined
          ? { cache: options.decodedColumnCache, path: options.decodedColumnCacheKey }
          : undefined,
      );
      if (page === undefined) return;
      if (transport !== undefined && options.allocationScope !== undefined) {
        transportPin = options.allocationScope.adopt(transport.allocations.retainAllocations(page));
      }
      offset += page.headerBytes + page.header.compressed_page_size;
      if (page.header.type === "DICTIONARY_PAGE") {
        if (cachedDictionary !== undefined) {
          dictionary = cachedDictionary;
          if (options.stats !== undefined) options.stats.cacheHits++;
          continue;
        }
        // Newly decoded dictionary values can alias this page until the column completes.
        transportPin = undefined;
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
        const pageScope =
          options.allocationScope === undefined || options.pageAllocationLifetime !== "iteration"
            ? undefined
            : options.allocationScope.childScope();
        const pageDecoder =
          pageScope === undefined
            ? columnDecoder
            : {
                ...columnDecoder,
                compressors: scopedParquetCompressors(pageScope),
              };
        try {
          const cache = options.decodedColumnCache;
          const key = pageCacheKey;
          const cached = cachedPageVector;
          let vector: Vector;
          if (cached !== undefined) {
            vector = cached;
          } else {
            const decoded = recordDecodeTime(options, () =>
              decodeFlatPageVector(
                page.compressedBytes,
                page.header,
                pageDecoder,
                dictionary,
                pageScope ?? options.allocationScope,
              ),
            );
            if (decoded === undefined) continue;
            vector = decoded;
            if (key !== undefined && cache !== undefined)
              cachePageVector(cache, key, vector, pageScope, options.allocationScope);
          }
          if (key !== undefined && options.stats !== undefined) {
            if (cached === undefined) options.stats.cacheMisses += 1;
            else options.stats.cacheHits += 1;
          }
          const pagePin =
            pageScope === undefined || options.allocationScope === undefined
              ? undefined
              : options.allocationScope.adopt(pageScope.retainAllocations(vector));
          try {
            yield {
              ...(pageScope === undefined
                ? {}
                : {
                    releaseConsumedPage() {
                      pagePin?.release();
                      pageScope.dispose();
                      transportPin?.release();
                      transport?.dispose();
                    },
                  }),
              rowOffset: start,
              batch: batchFromVectors({
                [column]: sliceVector(vector, start - pageRowStart, end - pageRowStart),
              }),
            };
          } finally {
            pagePin?.release();
          }
        } finally {
          pageScope?.dispose();
        }
      }
      pageRowStart = pageRowEnd;
    } finally {
      transportPin?.release();
      transport?.dispose();
    }
  }
}

function cachePageVector(
  cache: DecodedColumnCache,
  key: string,
  vector: Vector,
  pageScope: MemoryScope | undefined,
  inputScope: MemoryScope | undefined,
): void {
  if (pageScope === undefined || inputScope === undefined) {
    cache.setVector(key, vector, inputScope);
    return;
  }
  // Packed vectors can alias transport or dictionary backing owned by the input scope.
  const backing = new MemoryScope(inputScope.budget, [inputScope.signal]);
  try {
    backing.adopt(inputScope.retainAllocations(vector));
    backing.adopt(pageScope.retainAllocations(vector));
    cache.setVector(key, vector, backing);
  } finally {
    backing.dispose();
  }
}

interface PageWindow extends PreparedPageWindow {
  compressedBytes: Uint8Array;
}

async function readPageWindow(
  file: StoreAsyncBuffer,
  chunkStart: number,
  chunkEnd: number,
  offset: number,
  shouldReadBody: (prepared: PreparedPageWindow) => boolean,
  scope?: MemoryScope,
  cache?: { cache: DecodedColumnCache; path: string },
): Promise<PageWindow | undefined> {
  const prepared =
    scope && cache
      ? await cachedPageWindow(file, { start: chunkStart, end: chunkEnd }, offset, scope, cache)
      : await preparePageWindow(file, chunkStart, chunkEnd, offset, scope);
  if (prepared === undefined) return undefined;
  return {
    ...prepared,
    compressedBytes: shouldReadBody(prepared)
      ? await readPreparedPageBody(file, prepared, scope)
      : new Uint8Array(0),
  };
}

function dataPageRowCount(header: PageHeader): number | undefined {
  if (header.type === "DATA_PAGE") return header.data_page_header?.num_values;
  if (header.type === "DATA_PAGE_V2") return header.data_page_header_v2?.num_rows;
  return undefined;
}

function dictionaryCacheKey(
  header: PageHeader,
  column: string,
  rowGroupStart: number,
  pageOffset: number,
  file: StoreAsyncBuffer,
  options: ReadParquetBatchOptions,
): string | undefined {
  const dictionaryHeader = header.dictionary_page_header;
  const cache = options.decodedColumnCache;
  if (!dictionaryHeader) return undefined;
  return cache === undefined || options.decodedColumnCacheKey === undefined
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
): ParquetDictionary | undefined {
  const dictionaryHeader = header.dictionary_page_header;
  if (dictionaryHeader === undefined) return undefined;
  const cache = options.decodedColumnCache;
  const key = dictionaryCacheKey(header, column, rowGroupStart, pageOffset, file, options);
  const cached =
    key === undefined || cache === undefined
      ? undefined
      : cache.getValue<ParquetDictionary>(key, options.allocationScope);
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
  const leaf = columnDecoder.element;
  const text = leaf.converted_type === "UTF8" || leaf.logical_type?.type === "STRING";
  if (
    options.allocationScope !== undefined &&
    (columnDecoder.type === "BYTE_ARRAY" || columnDecoder.type === "FIXED_LEN_BYTE_ARRAY") &&
    (text || (leaf.converted_type === undefined && leaf.logical_type === undefined))
  ) {
    const fixedLength =
      columnDecoder.type === "FIXED_LEN_BYTE_ARRAY" ? leaf.type_length : undefined;
    if (columnDecoder.type === "FIXED_LEN_BYTE_ARRAY" && fixedLength === undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing fixed binary width");
    const packed = readPackedBinaryValues(
      pageReader,
      dictionaryHeader.num_values,
      fixedLength,
      options.allocationScope,
    );
    const dictionary: PackedByteVector = {
      type: text ? "utf8-view" : "binary-view",
      data: packed.payload,
      length: packed.count,
      ...(packed.ranges === undefined ? {} : { ranges: packed.ranges }),
      ...(packed.fixedLength === undefined ? {} : { fixedLength: packed.fixedLength }),
    };
    if (key !== undefined && cache !== undefined) {
      cache.setValue(
        key,
        dictionary,
        packed.payload.byteLength + (packed.ranges?.byteLength ?? 0),
        options.allocationScope,
      );
      if (options.stats !== undefined) options.stats.cacheMisses += 1;
    }
    return dictionary;
  }
  const rawDictionary = readPlainValues(
    pageReader,
    columnDecoder.type,
    dictionaryHeader.num_values,
    columnDecoder.element.type_length,
    options.allocationScope,
  );
  const dictionary = convertDecodedValues(rawDictionary, columnDecoder, options.allocationScope);
  if (key !== undefined && cache !== undefined) {
    cache.setValue(key, dictionary, estimateDecodedArrayBytes(dictionary), options.allocationScope);
    if (options.stats !== undefined) options.stats.cacheMisses += 1;
  }
  return dictionary;
}

/** Shared complete-vector decode for chunk and window transports. These callers
 * install our built-in scoped codecs, so planning can bind them to its own stage.
 */
function decodeFlatPageVector(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
  dictionary: ParquetDictionary | undefined,
  scope?: MemoryScope,
): Vector | undefined {
  if (scope !== undefined) {
    const { compressors: _builtInCompressors, ...planningDecoder } = decoder;
    const plan = planFlatNumericVector(bytes, header, planningDecoder);
    // Conservative total-new admission can exceed incremental live peak (notably
    // ZSTD). Keep the bounded incremental path when that reservation cannot fit.
    if (plan !== undefined && plan.bytes <= scope.availableAllocationBytes) {
      if (plan.bytes === 0) return plan.execute(scope);
      const stage = scope.childStage(plan.bytes, "complete Parquet numeric vector");
      try {
        const vector = plan.execute(stage);
        scope.adopt(stage.retainAllocations(vector));
        return vector;
      } finally {
        stage.dispose();
      }
    }
  }
  const page = dataPageValues(bytes, header, decoder, dictionary, scope);
  if (page === undefined) return undefined;
  if (decoder.element.converted_type === "JSON") {
    if (page.dictionary !== undefined && isPackedDictionary(page.dictionary))
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "JSON requires a logical dictionary");
    return logicalPageVector(
      page.values,
      page.definitionLevels,
      dataPageRowCount(header) ?? 0,
      page.dictionary,
      scope,
    );
  }
  return (
    page.packedVector ??
    flatPageVector(
      page.values,
      page.definitionLevels,
      0,
      dataPageRowCount(header) ?? 0,
      page.dictionary,
      isUnsigned64Leaf(decoder.element),
      scope,
      page.booleanValues,
    )
  );
}

function dataPageValues(
  compressedBytes: Uint8Array,
  header: PageHeader,
  columnDecoder: ColumnDecoder,
  dictionary: ParquetDictionary | undefined,
  scope?: MemoryScope,
):
  | {
      rowCount: number;
      values: DecodedArray;
      definitionLevels: ArrayLike<number> | undefined;
      dictionary?: ParquetDictionary;
      booleanValues?: boolean;
      packedVector?: Vector;
    }
  | undefined {
  if (scope !== undefined) {
    const admitted = readNumericPage(
      compressedBytes,
      header,
      columnDecoder,
      scope,
      dictionary?.length,
    );
    if (admitted !== undefined)
      return {
        rowCount: dataPageRowCount(header) ?? 0,
        ...(admitted.packedVector === undefined ? {} : { packedVector: admitted.packedVector }),
        values: admitted.dictionaryEncoded
          ? admitted.values
          : convertDecodedValues(admitted.values, columnDecoder, scope),
        ...(admitted.dictionaryEncoded && dictionary !== undefined ? { dictionary } : {}),
        definitionLevels: admitted.definitionLevels,
        ...(admitted.booleanValues === true ? { booleanValues: true } : {}),
      };
  }
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
    const compactDataPage = compactDecodedValues(dataPage, scope);
    const pageDictionary =
      dictionary !== undefined && isDictionaryEncoding(dataHeader.encoding)
        ? dictionary
        : undefined;
    return {
      rowCount: dataHeader.num_values,
      values:
        pageDictionary === undefined
          ? convertDecodedValuesWithDictionary(
              compactDataPage,
              dictionary,
              dataHeader.encoding,
              columnDecoder,
              scope,
            )
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
    const compactDataPage = compactDecodedValues(dataPage, scope);
    const pageDictionary =
      dictionary !== undefined && isDictionaryEncoding(dataHeader.encoding)
        ? dictionary
        : undefined;
    return {
      rowCount: dataHeader.num_rows,
      values:
        pageDictionary === undefined
          ? convertDecodedValuesWithDictionary(
              compactDataPage,
              dictionary,
              dataHeader.encoding,
              columnDecoder,
              scope,
            )
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

function convertDecodedValuesWithDictionary(
  values: DecodedArray,
  dictionary: ParquetDictionary | undefined,
  encoding: Encoding,
  columnDecoder: ColumnDecoder,
  scope?: MemoryScope,
): DecodedArray {
  if (dictionary !== undefined && encoding.endsWith("_DICTIONARY")) {
    return convertWithDictionary(
      values,
      isPackedDictionary(dictionary) ? undefined : dictionary,
      encoding,
      columnDecoder,
    );
  }
  return convertDecodedValues(values, columnDecoder, scope);
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

/** Complete typed backing through flat numeric vector construction.
 * Explicit logical capabilities exclude parsers that allocate JS objects/arrays.
 * Encoded input ownership, dictionary pages and aligned columns remain separate.
 */
export function planFlatNumericVector(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
): { bytes: number; borrowsInput: boolean; execute(scope: MemoryScope): Vector } | undefined {
  const decimal = flatNumericVectorDecimal(decoder);
  if (decimal === undefined) return undefined;
  const physical = planFlatNumericPage(bytes, header, decoder);
  if (physical === undefined) return undefined;
  const nullable = physical.presentCount < physical.rowCount;
  const extent = numericVectorExtent(physical, decimal);
  return {
    bytes: extent,
    borrowsInput: physical.borrowsInput && !decimal && !nullable,
    execute(scope) {
      // No detached values or validity: conversion is an alias-only operation,
      // so a scratch child and ownership snapshot would retain no extra backing.
      if (!decimal && !nullable) {
        const page = physical.execute(scope);
        return flatPageVector(
          convertDecodedValues(page.values, decoder, scope),
          page.definitionLevels,
          0,
          physical.rowCount,
          undefined,
          isUnsigned64Leaf(decoder.element),
          scope,
          page.booleanValues,
        );
      }
      const decodeScope = scope.childScope();
      try {
        const page = physical.execute(decodeScope);
        if (physical.presentCount === 0) return { type: "null", length: physical.rowCount };
        const values = convertDecodedValues(page.values, decoder, scope);
        // Preserve only backing used by the result; compact decoded values are
        // scratch when conversion or nullable expansion detaches the output.
        if (page.definitionLevels !== undefined)
          scope.adopt(decodeScope.retainAllocation(page.definitionLevels));
        return flatPageVector(
          values,
          page.definitionLevels,
          0,
          physical.rowCount,
          undefined,
          isUnsigned64Leaf(decoder.element),
          scope,
          page.booleanValues,
        );
      } finally {
        decodeScope.dispose();
      }
    },
  };
}

/** Complete typed-vector estimate before body transport. Encoded input and
 * retained peer columns remain separate allocations for the enclosing stage.
 */
export function planFlatNumericVectorHeader(
  header: PageHeader,
  decoder: ColumnDecoder,
):
  | {
      bytes: number;
      mayBorrowInput: boolean;
      execute(input: Uint8Array, scope: MemoryScope): Vector;
    }
  | undefined {
  const decimal = flatNumericVectorDecimal(decoder);
  if (decimal === undefined) return undefined;
  const physical = planFlatNumericPageHeader(header, decoder);
  if (physical === undefined) return undefined;
  const extent = numericVectorExtent(physical, decimal);
  return {
    bytes: extent,
    mayBorrowInput: decoder.type !== "BOOLEAN" && !physical.compressed,
    execute(input, scope) {
      if (input.byteLength !== header.compressed_page_size)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Prepared numeric body length changed");
      const precise = planFlatNumericVector(input, header, decoder);
      if (precise === undefined && header.type === "DATA_PAGE") {
        const scratch = scope.childScope();
        try {
          const page = readNumericPage(
            input,
            header,
            { ...decoder, compressors: scopedParquetCompressors(scratch, physical.zstd) },
            scratch,
          );
          if (page === undefined)
            throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing V1 numeric capability");
          const values = convertDecodedValues(page.values, decoder, scope);
          if (page.definitionLevels !== undefined)
            scope.adopt(scratch.retainAllocation(page.definitionLevels));
          const vector = flatPageVector(
            values,
            page.definitionLevels,
            0,
            physical.rowCount,
            undefined,
            isUnsigned64Leaf(decoder.element),
            scope,
            page.booleanValues,
          );
          if (page.definitionLevels === undefined && !decimal)
            scope.adopt(scratch.retainAllocations(vector));
          return vector;
        } finally {
          scratch.dispose();
        }
      }
      if (precise === undefined || precise.bytes > extent)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Numeric vector exceeds header plan");
      return precise.execute(scope);
    },
  };
}

function flatNumericVectorDecimal(decoder: ColumnDecoder): boolean | undefined {
  const { converted_type: converted, logical_type: logical } = decoder.element;
  const decimal = isIntegerDecimal(decoder.element);
  if (
    !decimal &&
    ((logical !== undefined && logical.type !== "INTEGER") ||
      (converted !== undefined &&
        ![
          "INT_8",
          "INT_16",
          "INT_32",
          "INT_64",
          "UINT_8",
          "UINT_16",
          "UINT_32",
          "UINT_64",
        ].includes(converted)))
  )
    return undefined;
  if (!decimal && (converted !== undefined || logical?.type === "INTEGER")) {
    const width =
      logical?.type === "INTEGER" ? logical.bitWidth : converted?.endsWith("64") ? 64 : 32;
    if (decoder.type !== (width === 64 ? "INT64" : "INT32")) return undefined;
  }
  return decimal;
}

function numericVectorExtent(
  physical: {
    bytes: number;
    presentCount: number;
    rowCount: number;
    valueWidth: number;
    mayHaveNulls?: boolean;
  },
  decimal: boolean,
): number {
  const nullable = physical.mayHaveNulls === true || physical.presentCount < physical.rowCount;
  const convertedBytes = decimal ? physical.presentCount * 8 : 0;
  const expansionBytes =
    nullable && physical.presentCount > 0
      ? physical.rowCount * (decimal ? 8 : physical.valueWidth)
      : 0;
  const extent = physical.bytes + convertedBytes + expansionBytes;
  if (!Number.isSafeInteger(extent))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Numeric vector extent exceeds safe capacity");
  return extent;
}

function flatPageVector(
  values: DecodedArray,
  definitionLevels: ArrayLike<number> | undefined,
  start: number,
  end: number,
  dictionary?: ParquetDictionary,
  unsigned64 = false,
  scope?: MemoryScope,
  booleanValues = false,
): Vector {
  const vector =
    dictionary !== undefined
      ? dictionaryPageVector(values, dictionary, definitionLevels, start, end, unsigned64, scope)
      : definitionLevels === undefined
        ? nonNullFlatVector(values, start, end, unsigned64, scope)
        : nullableFlatVector(values, definitionLevels, start, end, unsigned64, scope);
  if (definitionLevels !== undefined) {
    // Nullable expansion copies the compact values/indices into final storage.
    // Release only exact owned allocations, never borrowed page backing or aliases.
    scope?.tryReleaseAllocation(values);
    if (!("valid" in vector) || vector.valid !== definitionLevels)
      scope?.tryReleaseAllocation(definitionLevels);
  }
  return booleanValues && vector.type === "u8" ? { ...vector, type: "bool" } : vector;
}

function dictionaryPageVector(
  values: DecodedArray,
  dictionary: ParquetDictionary,
  definitionLevels: ArrayLike<number> | undefined,
  start: number,
  end: number,
  unsigned64: boolean,
  scope?: MemoryScope,
): Vector {
  const dictionaryVector = isPackedDictionary(dictionary)
    ? dictionary
    : nonNullFlatVector(dictionary, 0, dictionary.length, unsigned64, scope);
  if (definitionLevels === undefined && values instanceof Uint32Array) {
    return { type: "dict", indices: values.subarray(start, end), dictionary: dictionaryVector };
  }
  const length = end - start;
  const indices = allocateVectorBuffer(Uint32Array, length, scope);
  if (definitionLevels === undefined) {
    for (let index = 0; index < length; index += 1) {
      indices[index] = Number(values[start + index] ?? 0);
    }
    return { type: "dict", indices, dictionary: dictionaryVector };
  }
  const valid = pageValidity(definitionLevels, start, end, scope);
  copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
    indices[outIndex] = Number(values[valueIndex] ?? 0);
  });
  return optionalVectorValidity({ type: "dict", indices, dictionary: dictionaryVector }, valid);
}

function nonNullFlatVector(
  values: DecodedArray,
  start: number,
  end: number,
  unsigned64 = false,
  scope?: MemoryScope,
): Vector {
  if (values instanceof Float64Array) return { type: "f64", values: values.subarray(start, end) };
  if (values instanceof Float32Array) return { type: "f32", values: values.subarray(start, end) };
  if (values instanceof Int32Array) return { type: "i32", values: values.subarray(start, end) };
  if (values instanceof Uint32Array) return { type: "u32", values: values.subarray(start, end) };
  if (values instanceof Uint8Array) return { type: "u8", values: values.subarray(start, end) };
  if (values instanceof BigInt64Array) {
    if (unsigned64) {
      const unsigned = new BigUint64Array(values.buffer, values.byteOffset, values.length);
      return { type: "u64", values: unsigned.subarray(start, end) };
    }
    return { type: "i64", values: values.subarray(start, end) };
  }
  if (values instanceof BigUint64Array) return { type: "u64", values: values.subarray(start, end) };
  return arrayFlatVector(values, start, end, unsigned64, scope);
}

function nullableFlatVector(
  values: DecodedArray,
  definitionLevels: ArrayLike<number>,
  start: number,
  end: number,
  unsigned64 = false,
  scope?: MemoryScope,
): Vector {
  const length = end - start;
  const valid = Array.isArray(values)
    ? allocateVectorBuffer(Uint8Array, length, scope)
    : pageValidity(definitionLevels, start, end, scope);
  if (values instanceof Float64Array) {
    const out = allocateVectorBuffer(Float64Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = Number(values[valueIndex] ?? 0);
    });
    return optionalVectorValidity({ type: "f64", values: out }, valid);
  }
  if (values instanceof Float32Array) {
    const out = allocateVectorBuffer(Float32Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "f32", values: out }, valid);
  }
  if (values instanceof Int32Array) {
    const out = allocateVectorBuffer(Int32Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "i32", values: out }, valid);
  }
  if (values instanceof Uint32Array) {
    const out = allocateVectorBuffer(Uint32Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "u32", values: out }, valid);
  }
  if (values instanceof Uint8Array) {
    const out = allocateVectorBuffer(Uint8Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0;
    });
    return optionalVectorValidity({ type: "u8", values: out }, valid);
  }
  if (values instanceof BigInt64Array) {
    if (unsigned64) {
      const source = new BigUint64Array(values.buffer, values.byteOffset, values.length);
      const out = allocateVectorBuffer(BigUint64Array, length, scope);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = source[valueIndex] ?? 0n;
      });
      return optionalVectorValidity({ type: "u64", values: out }, valid);
    }
    const out = allocateVectorBuffer(BigInt64Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = BigInt(values[valueIndex] ?? 0n);
    });
    return optionalVectorValidity({ type: "i64", values: out }, valid);
  }
  if (values instanceof BigUint64Array) {
    const out = allocateVectorBuffer(BigUint64Array, length, scope);
    copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
      out[outIndex] = values[valueIndex] ?? 0n;
    });
    return optionalVectorValidity({ type: "u64", values: out }, valid);
  }
  return nullableArrayFlatVector(values, definitionLevels, start, end, valid, unsigned64, scope);
}

/** Owned numeric levels are normalized bytes and can become final validity. */
function pageValidity(
  levels: ArrayLike<number>,
  start: number,
  end: number,
  scope?: MemoryScope,
): Uint8Array {
  return scope !== undefined && levels instanceof Uint8Array && start === 0 && end === levels.length
    ? levels
    : allocateVectorBuffer(Uint8Array, end - start, scope);
}

function copyNullableValues(
  definitionLevels: ArrayLike<number>,
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

function arrayFlatVector(
  values: unknown[],
  start: number,
  end: number,
  unsigned64: boolean,
  scope?: MemoryScope,
): Vector {
  const first = firstPresentArrayValue(values, start, end);
  switch (typeof first) {
    case "number": {
      const out = allocateVectorBuffer(Float64Array, end - start, scope);
      for (let index = 0; index < out.length; index += 1) {
        out[index] = Number(values[start + index] ?? 0);
      }
      return { type: "f64", values: out };
    }
    case "bigint": {
      if (unsigned64) {
        const out = allocateVectorBuffer(BigUint64Array, end - start, scope);
        for (let index = 0; index < out.length; index += 1) {
          out[index] = BigInt.asUintN(64, bigintArrayValue(values[start + index]));
        }
        return { type: "u64", values: out };
      }
      const out = allocateVectorBuffer(BigInt64Array, end - start, scope);
      for (let index = 0; index < out.length; index += 1) {
        out[index] = bigintArrayValue(values[start + index]);
      }
      return { type: "i64", values: out };
    }
    case "boolean": {
      const out = allocateVectorBuffer(Uint8Array, end - start, scope);
      for (let index = 0; index < out.length; index += 1) {
        out[index] = values[start + index] === true ? 1 : 0;
      }
      return { type: "bool", values: out };
    }
    case "string": {
      const out = allocateDecodedArray<string>(end - start, scope);
      for (let index = 0; index < out.length; index++)
        out[index] = decodedStringValue(values[start + index]);
      return { type: "utf8", values: out };
    }
    case "object":
      if (first instanceof Date || isTimestampValue(first))
        return vectorFromSource(
          { length: end - start, get: (index) => values[start + index] },
          scope,
        );
      if (isBinaryValue(first)) {
        const out = allocateDecodedArray<Uint8Array>(end - start, scope);
        for (let index = 0; index < out.length; index++)
          out[index] = decodedBinaryValue(values[start + index], scope);
        return { type: "binary", values: out };
      }
      return { type: "null", length: end - start };
    default:
      return { type: "null", length: end - start };
  }
}

function nullableArrayFlatVector(
  values: unknown[],
  definitionLevels: ArrayLike<number>,
  start: number,
  end: number,
  valid: Uint8Array,
  unsigned64: boolean,
  scope?: MemoryScope,
): Vector {
  const first = firstPresentDefinitionValue(values, definitionLevels, start, end);
  const length = end - start;
  switch (typeof first) {
    case "number": {
      const out = allocateVectorBuffer(Float64Array, length, scope);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = Number(values[valueIndex] ?? 0);
      });
      return optionalVectorValidity({ type: "f64", values: out }, valid);
    }
    case "bigint": {
      if (unsigned64) {
        const out = allocateVectorBuffer(BigUint64Array, length, scope);
        copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
          out[outIndex] = BigInt.asUintN(64, bigintArrayValue(values[valueIndex]));
        });
        return optionalVectorValidity({ type: "u64", values: out }, valid);
      }
      const out = allocateVectorBuffer(BigInt64Array, length, scope);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = bigintArrayValue(values[valueIndex]);
      });
      return optionalVectorValidity({ type: "i64", values: out }, valid);
    }
    case "boolean": {
      const out = allocateVectorBuffer(Uint8Array, length, scope);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = values[valueIndex] === true ? 1 : 0;
      });
      return optionalVectorValidity({ type: "bool", values: out }, valid);
    }
    case "string": {
      const out = allocateDecodedArray<string>(length, scope);
      copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
        out[outIndex] = decodedStringValue(values[valueIndex]);
      });
      for (let index = 0; index < out.length; index += 1) {
        if (out[index] === undefined) out[index] = "";
      }
      return optionalVectorValidity({ type: "utf8", values: out }, valid);
    }
    case "object": {
      if (isBinaryValue(first)) {
        const out = allocateDecodedArray<Uint8Array>(length, scope);
        copyNullableValues(definitionLevels, start, end, valid, (outIndex, valueIndex) => {
          out[outIndex] = decodedBinaryValue(values[valueIndex], scope);
        });
        for (let index = 0; index < out.length; index += 1) {
          if (out[index] === undefined) out[index] = decodedBinaryValue(undefined, scope);
        }
        return optionalVectorValidity({ type: "binary", values: out }, valid);
      }
      if (!(first instanceof Date) && !isTimestampValue(first)) return { type: "null", length };
      // Rewind on a new inference/construction pass; no row-sized reconstruction array.
      let row = 0;
      let valueIndex = 0;
      // The constructor creates its validity mask; release the unused mask first.
      scope?.releaseAllocation(valid);
      const vector = vectorFromSource(
        {
          length,
          get(index) {
            const target = start + index;
            if (target < row) {
              row = 0;
              valueIndex = 0;
            }
            while (row < target) {
              if (definitionLevels[row] !== 0) valueIndex++;
              row++;
            }
            return definitionLevels[target] === 0 ? null : values[valueIndex];
          },
        },
        scope,
      );
      return vector;
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

function firstPresentDefinitionValue(
  values: readonly unknown[],
  definitionLevels: ArrayLike<number>,
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

function safeNumber(value: bigint | number | undefined): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (typeof value === "bigint") {
    const numberValue = Number(value);
    if (Number.isSafeInteger(numberValue) && BigInt(numberValue) === value) return numberValue;
  }
  return undefined;
}

function allocateVectorBuffer<T extends ArrayBufferView>(
  Constructor: { new (length: number): T; readonly BYTES_PER_ELEMENT: number },
  length: number,
  scope: MemoryScope | undefined,
): T {
  if (scope === undefined) return new Constructor(length);
  return scope.allocate(
    length * Constructor.BYTES_PER_ELEMENT,
    "Parquet vector buffer",
    () => new Constructor(length),
  ).value;
}
