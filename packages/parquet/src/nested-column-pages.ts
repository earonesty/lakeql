import type { ColumnMetaData } from "hyparquet";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope, type OwnedMemory, type Vector } from "lakeql-core";
import { dictionaryPageOffset } from "./dictionary-offset.js";
import type { TemporalColumnDecoder } from "./int96-interpretation.js";
import { decodeCompactNestedColumnPage, decodeNestedColumnPage } from "./nested-column-page.js";
import { decodeNestedDictionaryPage } from "./nested-dictionary-page.js";
import { preparePageWindow, readPreparedPageBody } from "./page-window.js";
import type { ReadParquetBatchOptions, StoreAsyncBuffer } from "./types.js";

export type NestedPageCacheOptions = Pick<
  ReadParquetBatchOptions,
  "decodedColumnCache" | "decodedColumnCacheKey" | "stats"
>;

type CompactPage = NonNullable<ReturnType<typeof decodeCompactNestedColumnPage>>;

/** Yield page leases valid until advance/close. Retain explicitly to outlive the iterator.
 * Uses owned transport reads, keeps one dictionary, and carries only scalar continuation
 * state between pages. The caller's budget bounds retained pages and dictionary together.
 */
export function readNestedColumnPages(
  file: StoreAsyncBuffer,
  column: ColumnMetaData,
  decoder: ColumnDecoder,
  scope: MemoryScope,
  options: NestedPageCacheOptions = {},
) {
  return readNestedPages(
    file,
    column,
    decoder,
    scope,
    options,
    decodeNestedColumnPage,
    "leaf-slots",
  );
}

export function readNestedCompactColumnPages(
  file: StoreAsyncBuffer,
  column: ColumnMetaData,
  decoder: ColumnDecoder,
  scope: MemoryScope,
  options: NestedPageCacheOptions = {},
) {
  return readNestedPages(
    file,
    column,
    decoder,
    scope,
    options,
    decodeCompactNestedColumnPage,
    "compact",
  );
}

async function* readNestedPages<Page extends CompactPage>(
  file: StoreAsyncBuffer,
  column: ColumnMetaData,
  decoder: ColumnDecoder,
  scope: MemoryScope,
  options: NestedPageCacheOptions,
  decode: (...args: Parameters<typeof decodeNestedColumnPage>) => Page | undefined,
  shape: "compact" | "leaf-slots",
): AsyncGenerator<OwnedMemory<Page>> {
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested column stream");
  if (file.sliceOwned === undefined)
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
      "Nested column streaming requires owned reads",
    );
  const dataStart = Number(column.data_page_offset);
  const dictionaryOffset = dictionaryPageOffset(column);
  const start =
    dictionaryOffset === undefined ? dataStart : Math.min(dataStart, Number(dictionaryOffset));
  const size = Number(column.total_compressed_size);
  const expected = Number(column.num_values);
  const end = start + size;
  if (
    ![dataStart, start, size, expected, end].every((n) => Number.isSafeInteger(n) && n >= 0) ||
    end > file.byteLength ||
    dataStart > end
  )
    throw invalid();
  const lifetime = scope.childScope();
  let dictionary: OwnedMemory<Vector> | undefined;
  let previousDefinition: number | undefined;
  let events = 0;
  let offset = 0;
  let sawData = false;
  try {
    while (offset < size) {
      if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Nested column scope disposed");
      const pageScope = lifetime.childScope();
      const inspection = pageScope.childScope();
      try {
        const prepared = await preparePageWindow(file, start, end, offset, inspection, true);
        if (!prepared) throw invalid();
        const header = prepared.header;
        const readBody = async () => {
          const body = await readPreparedPageBody(file, prepared, inspection);
          // Only a cache miss can make transport backing part of decoded output.
          pageScope.adopt(inspection.retainAllocations(undefined));
          inspection.dispose();
          return body;
        };
        if (header.type === "INDEX_PAGE") {
          offset = prepared.bodyEnd - start;
          continue;
        }
        const cache = options.decodedColumnCache;
        const key =
          cache && options.decodedColumnCacheKey !== undefined
            ? JSON.stringify([
                "nested-page-v3",
                header.type === "DICTIONARY_PAGE" ? "dictionary" : shape,
                (decoder as TemporalColumnDecoder).int96Interpretation ?? "julian-nanos",
                options.decodedColumnCacheKey,
                file.byteLength,
                file.etag,
                column.path_in_schema,
                start + offset,
                prepared.bodyEnd,
                previousDefinition,
              ])
            : undefined;
        if (header.type === "DICTIONARY_PAGE") {
          if (dictionary !== undefined || sawData) throw invalid();
          const cached = key === undefined ? undefined : cache?.getVector(key, pageScope);
          if (cached !== undefined) inspection.dispose();
          const vector =
            cached ?? decodeNestedDictionaryPage(await readBody(), header, decoder, pageScope);
          if (!vector)
            throw new LakeqlError(
              "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
              "Unsupported nested dictionary page",
            );
          if (key !== undefined && cache) {
            if (options.stats) {
              if (cached) options.stats.cacheHits++;
              else options.stats.cacheMisses++;
            }
            if (!cached) cache.setVector(key, vector, pageScope);
          }
          dictionary = lifetime.adopt(pageScope.retainAllocations(vector));
        } else {
          const cached = key === undefined ? undefined : cache?.getValue<Page>(key, pageScope);
          if (cached !== undefined) inspection.dispose();
          const page =
            cached ??
            decode(
              await readBody(),
              header,
              decoder,
              previousDefinition,
              pageScope,
              dictionary?.value,
            );
          if (!page)
            throw new LakeqlError(
              "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
              "Unsupported nested data page",
            );
          const count =
            header.data_page_header?.num_values ?? header.data_page_header_v2?.num_values;
          if (
            count === undefined ||
            !Number.isSafeInteger(events + count) ||
            events + count > expected
          )
            throw invalid();
          // A cached page already owns the dictionary used to decode it. A
          // freshly decoded replacement dictionary must not become extra output
          // backing when its standalone cache entry has been evicted.
          if (dictionary && cached === undefined) pageScope.adopt(dictionary.retain());
          if (key !== undefined && cache) {
            if (options.stats) {
              if (cached) options.stats.cacheHits++;
              else options.stats.cacheMisses++;
            }
            if (!cached) cache.setValue(key, page, 0, pageScope);
          }
          const owner = pageScope.retainAllocations(page);
          pageScope.dispose();
          previousDefinition = page.nextDefinition;
          events += count;
          sawData = true;
          offset = prepared.bodyEnd - start;
          try {
            yield owner;
          } finally {
            owner.release();
          }
          continue;
        }
        offset = prepared.bodyEnd - start;
      } finally {
        pageScope.dispose();
      }
    }
    if (events !== expected) throw invalid();
  } finally {
    lifetime.dispose();
  }
}
