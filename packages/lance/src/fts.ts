/**
 * Full-text search over Lance inverted indexes (`INVERTED` scalar index, on-disk v1/v2
 * partitioned layout with FST token sets and block-compressed posting lists).
 *
 * Memory contract: every buffer, plan and map is admitted against the operation budget before
 * it is allocated, and resident state is bounded by the query shape and `LanceFtsLimits`,
 * never by match count. Two structures do grow with the number of partitions, and both are
 * reserved before they are built: the decoded file directories of an opened index, and the
 * query plan (one posting location per scoring term per partition).
 *   - Opening an index keeps only file directories. Each directory is admitted from its
 *     encoded size before it is decoded; once the shared cache holds the opened index the
 *     cache owns that charge.
 *   - The format stores no corpus token total, so BM25's average document length needs every
 *     document length once per index. That pass is deferred until a query term actually
 *     resolves (an absent term reads no lengths), runs as bounded concurrent chunks alongside
 *     the posting-location wave, keeps one chunk per reader resident, and is cached with the
 *     opened index.
 *   - A partition's FST token set (one blob in the format, so it must be resident while terms
 *     are resolved) is loaded in memory-sized partition groups and released after resolution,
 *     unless the shared cache keeps it.
 *   - Postings are decoded `postingBlocksPerBatch` blocks at a time per term and scored
 *     document-at-a-time across terms in batches of `documentsPerBatch`. Document lengths and
 *     phrase positions are read per batch and released with it.
 *   - Ranking keeps a bounded top-k heap of live rows. Rows deleted after indexing are
 *     filtered before they can enter it: each batch's best candidates that would still enter
 *     the heap have their row ids read and checked, so one scan returns the final ranking and
 *     no document is ever scored twice.
 *
 * Round trips: a warm query costs posting offsets and lengths, then per batch block offsets,
 * block bytes and document lengths, then `_rowid` of the ranked results. When a term's
 * postings fit in one batch this is a constant five waves, independent of dataset size.
 */
import { LakeqlError, type SharedMemoryCache } from "lakeql";
import { BLOCK_LEN, packedBlockBytes, unpackBlock, unpackSortedBlock } from "./bitpack.js";
import {
  childField,
  columnOfField,
  readBinaryRanges,
  readListOffsets,
  readU32Ranges,
  readU32Rows,
  readU64Rows,
  resolveColumn,
} from "./column.js";
import { type InspectedLanceFile, inspectLanceFile } from "./file.js";
import { type FstAutomaton, FstMap } from "./fst.js";
import {
  analyzeTokens,
  assertSupportedAnalyzer,
  type LanceTokenizerParams,
  parseTokenizerParams,
  type QueryToken,
} from "./fts-tokenizer.js";
import type { LanceReadContext, MemoryLease, MemoryReservation } from "./io.js";
import type { LanceIndexMetadata, LanceManifest } from "./proto.js";
import { roaringContains } from "./roaring.js";
import { loadLanceIndexMetadata } from "./scalar.js";

export const INVERTED_INDEX_DETAILS_TYPE_URL = "/lance.table.invertedindexdetails";

export interface LanceFtsIndexInfo {
  name: string;
  uuid: string;
  column: string;
  indexVersion?: number;
  /** Positions are stored, so phrase queries are available. */
  withPosition: boolean;
  tokenizer: Pick<
    LanceTokenizerParams,
    "baseTokenizer" | "language" | "lowerCase" | "stem" | "removeStopWords" | "asciiFolding"
  >;
  /** False when this reader cannot search the index; `unsupportedReason` says why. */
  supported: boolean;
  unsupportedReason?: string;
}

export interface LanceFtsLimits {
  /**
   * Largest serialized FST token set of one partition. The format stores it as a single blob,
   * so it must be resident (and admitted against the memory budget) while terms resolve.
   */
  maxTokenSetBytes: number;
  /** Posting entries (document, frequency pairs) decoded by one query. */
  maxPostingEntries: number;
  /** Documents scored by one query (a work bound; ranking memory is bounded by `limit`). */
  maxCandidates: number;
  /** Phrase candidates whose positions are read. */
  maxPhraseCandidates: number;
  /** 128-entry posting blocks decoded per term at a time. */
  postingBlocksPerBatch: number;
  /** Documents scored, or phrase candidates checked, per batch. */
  documentsPerBatch: number;
  /** Document lengths read per request when summing the corpus token total. */
  documentLengthChunkRows: number;
}

export const DEFAULT_FTS_LIMITS: LanceFtsLimits = {
  maxTokenSetBytes: 64 * 1024 * 1024,
  maxPostingEntries: 4_000_000,
  maxCandidates: 1_000_000,
  maxPhraseCandidates: 100_000,
  postingBlocksPerBatch: 64,
  documentsPerBatch: 4096,
  documentLengthChunkRows: 16_384,
};

export type LanceFtsQuery =
  | {
      kind: "match";
      text: string;
      /** How multiple query terms combine. Default `or`. */
      operator?: "or" | "and";
      /**
       * Edit distance (0, 1 or 2) for expanding each term through the token dictionary.
       * Default 0 (exact terms).
       */
      fuzziness?: 0 | 1 | 2;
      /** Leading UTF-8 bytes that must match exactly when expanding fuzzy terms. */
      prefixLength?: number;
      /** Most dictionary terms a fuzzy query term may expand to. Default 50. */
      maxExpansions?: number;
    }
  | {
      kind: "phrase";
      text: string;
      /** Extra positions allowed between consecutive terms; terms must stay in order. */
      slop?: number;
    }
  | {
      kind: "boolean";
      must?: readonly LanceFtsQuery[];
      should?: readonly LanceFtsQuery[];
      mustNot?: readonly LanceFtsQuery[];
    };

export interface LanceFtsHit {
  rowId: bigint;
  score: number;
}

export interface LanceFtsSearchStats {
  partitions: number;
  tokensMatched: number;
  postingEntriesDecoded: number;
  candidatesScored: number;
  phraseCandidatesChecked: number;
  /** Document batches scored (bounded-memory units of work). */
  batches: number;
  /**
   * Candidates whose row ids were read to rank them (and to check they are still live). Only
   * candidates that would enter the top-k at the time are resolved, so this stays near
   * `limit` per batch unless many of the best matches were deleted after indexing.
   */
  rowIdsResolved: number;
  /** Document lengths read by this search to compute the corpus token total (0 when cached). */
  documentLengthsSummed: number;
  /**
   * Accounted bytes of the opened index's decoded file directories. Charged to this operation
   * when it opened the index, and to the shared cache when the index came from there.
   */
  directoryBytes: number;
  /** Accounted bytes of the query plan retained for the whole search. */
  planBytes: number;
}

interface Partition {
  id: number;
  tokens: InspectedLanceFile;
  invert: InspectedLanceFile;
  docs: InspectedLanceFile;
  /** Accounted bytes of this partition's three decoded file directories and its descriptor. */
  directoryBytes: number;
  fstColumn: number;
  tokenSetBytes: number;
  numDocs: number;
  tailCodec: "fixed32" | "varint_delta";
  positions: "none" | "legacy_per_doc" | "shared_stream";
  lengthColumn: number;
  postingColumn: number;
  postingItemColumn: number;
  rowIdColumn: number;
  numTokensColumn: number;
  positionColumn?: number;
  positionOuterItemColumn?: number;
  positionItemColumn?: number;
}

/** What an opened index retains: file directories, plus the corpus token total once known. */
export interface OpenedFtsIndex {
  info: LanceFtsIndexInfo;
  tokenizer: LanceTokenizerParams;
  partitions: Partition[];
  numDocs: number;
  /**
   * Sum of every document's token count, for BM25's average document length. The format does
   * not persist it, so it is computed by the first search that needs it (`corpusTokens`).
   */
  totalTokens: number | undefined;
  /** Accounted bytes of everything this object keeps resident. */
  directoryBytes: number;
  cacheNamespace: string | undefined;
}

/** Work the opening of an index required, re-checked against the caller's limits on reuse. */
function enforceOpenLimits(index: OpenedFtsIndex, limits: LanceFtsLimits): void {
  for (const partition of index.partitions) {
    if (partition.tokenSetBytes > limits.maxTokenSetBytes) {
      budget("FTS token set bytes", limits.maxTokenSetBytes, partition.tokenSetBytes);
    }
  }
}

type Source = {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexMetadata?: readonly LanceIndexMetadata[];
  indexCache?: SharedMemoryCache | undefined;
  cacheNamespace?: string;
};

export async function loadFtsIndexes(
  options: Source,
): Promise<{ metadata: LanceIndexMetadata; column: string }[]> {
  const metadata = options.indexMetadata ?? (await loadLanceIndexMetadata(options));
  const fieldById = new Map(options.manifest.fields.map((field) => [field.id, field]));
  return metadata.filter(isInverted).map((index) => {
    if (index.fields.length !== 1) {
      corrupt("Lance inverted index must address exactly one field", { index: index.name });
    }
    const field = fieldById.get(index.fields[0] as number);
    if (field === undefined) corrupt("Lance inverted index references an unknown field");
    return { metadata: index, column: field.name };
  });
}

export function isInverted(index: LanceIndexMetadata): boolean {
  return index.detailsTypeUrl.toLowerCase() === INVERTED_INDEX_DETAILS_TYPE_URL;
}

const SUPPORTED_INDEX_VERSIONS = [1, 2];

interface IndexConfig {
  directory: string;
  sizes: ReadonlyMap<string, number>;
  tokenizer: LanceTokenizerParams;
  partitionIds: number[];
}

/** Read `metadata.lance` (one small file) and describe the index without opening partitions. */
async function readIndexConfig(
  context: LanceReadContext,
  root: string,
  metadata: LanceIndexMetadata,
): Promise<IndexConfig> {
  const directory = `${root}/_indices/${metadata.uuid}`;
  // The manifest records every index file's size, which saves a HEAD round trip per file.
  const sizes = new Map(metadata.files.map((file) => [file.path, file.sizeBytes]));
  // `metadata.lance` is only needed until its settings are parsed.
  const metaDirectory = context.openReservation();
  try {
    const meta = await inspectLanceFile(
      context,
      `${directory}/metadata.lance`,
      sizes.get("metadata.lance"),
      (bytes) => metaDirectory.grow(bytes),
    );
    return parseIndexConfig(directory, sizes, metadata, meta);
  } finally {
    metaDirectory.release();
  }
}

function parseIndexConfig(
  directory: string,
  sizes: ReadonlyMap<string, number>,
  metadata: LanceIndexMetadata,
  meta: InspectedLanceFile,
): IndexConfig {
  const schemaMetadata = decodeMetadata(meta.schemaMetadata);
  const paramsJson = schemaMetadata.get("params");
  const partitionsJson = schemaMetadata.get("partitions");
  if (paramsJson === undefined || partitionsJson === undefined) {
    unsupported("Legacy single-partition Lance inverted indexes are not supported");
  }
  const tokenSetFormat = schemaMetadata.get("token_set_format") ?? "arrow";
  if (tokenSetFormat !== "fst") {
    unsupported("Lance inverted index uses a token set format other than fst", {
      format: tokenSetFormat,
    });
  }
  if (
    metadata.indexVersion === undefined ||
    !SUPPORTED_INDEX_VERSIONS.includes(metadata.indexVersion)
  ) {
    unsupported("Unsupported Lance inverted index version", { version: metadata.indexVersion });
  }
  return {
    directory,
    sizes,
    tokenizer: parseTokenizerParams(paramsJson),
    partitionIds: parsePartitionIds(partitionsJson),
  };
}

function describeIndex(
  metadata: LanceIndexMetadata,
  column: string,
  tokenizer: LanceTokenizerParams | undefined,
  reason: string | undefined,
): LanceFtsIndexInfo {
  return {
    name: metadata.name,
    uuid: metadata.uuid,
    column,
    ...(metadata.indexVersion === undefined ? {} : { indexVersion: metadata.indexVersion }),
    withPosition: tokenizer?.withPosition ?? false,
    tokenizer: {
      baseTokenizer: tokenizer?.baseTokenizer ?? "unknown",
      language: tokenizer?.language ?? "unknown",
      lowerCase: tokenizer?.lowerCase ?? false,
      stem: tokenizer?.stem ?? false,
      removeStopWords: tokenizer?.removeStopWords ?? false,
      asciiFolding: tokenizer?.asciiFolding ?? false,
    },
    supported: reason === undefined,
    ...(reason === undefined ? {} : { unsupportedReason: reason }),
  };
}

/**
 * Describe every inverted index. An index this reader cannot search is still listed, flagged
 * `supported: false`, so one unusual index never hides the usable ones; searching it fails.
 */
export async function describeFtsIndexes(options: Source): Promise<LanceFtsIndexInfo[]> {
  const indexes = await loadFtsIndexes(options);
  return await all(
    indexes.map(async ({ metadata, column }) => {
      const unsupportedReason = (error: unknown): string | undefined =>
        error instanceof LakeqlError && error.code === "LAKEQL_UNSUPPORTED_LANCE_FEATURE"
          ? error.message
          : undefined;
      let config: IndexConfig;
      try {
        config = await readIndexConfig(options.context, options.root, metadata);
      } catch (error) {
        const reason = unsupportedReason(error);
        if (reason === undefined) throw error;
        return describeIndex(metadata, column, undefined, reason);
      }
      try {
        assertSupportedAnalyzer(config.tokenizer);
      } catch (error) {
        const reason = unsupportedReason(error);
        if (reason === undefined) throw error;
        // Keep the persisted analyzer settings so callers can see why it is unsupported.
        return describeIndex(metadata, column, config.tokenizer, reason);
      }
      return describeIndex(metadata, column, config.tokenizer, undefined);
    }),
  );
}

/** Open (and cache) one inverted index. */
/** Open (and cache) one inverted index: directories and corpus totals only. */
export async function openFtsIndex(
  options: Source,
  metadata: LanceIndexMetadata,
  column: string,
  limits: LanceFtsLimits,
): Promise<OpenedFtsIndex> {
  const cacheKey =
    options.indexCache === undefined || options.cacheNamespace === undefined
      ? undefined
      : JSON.stringify([options.cacheNamespace, "fts", metadata.uuid]);
  if (cacheKey !== undefined) {
    const cached = options.indexCache?.get<OpenedFtsIndex>(cacheKey);
    if (cached !== undefined) {
      options.context.stats.cacheHits += 1;
      enforceOpenLimits(cached.value, limits);
      return cached.value;
    }
    options.context.stats.cacheMisses += 1;
  }
  const context = options.context;
  if (metadata.fragmentBitmap === undefined) {
    unsupported("Inverted index has no fragment coverage metadata");
  }
  const covers = roaringContains(metadata.fragmentBitmap);
  if (options.manifest.fragments.some((fragment) => !covers(Number(fragment.id)))) {
    unsupported(
      "Inverted index does not cover all active fragments; rebuild or optimize the index after appending or rewriting data",
    );
  }
  const config = await readIndexConfig(context, options.root, metadata);
  assertSupportedAnalyzer(config.tokenizer);
  const { tokenizer } = config;
  // Directories stay resident for the whole operation (and then in the shared cache), so each
  // is charged to the operation before it is decoded: the partition table up front, and every
  // file directory from its encoded size inside `inspectLanceFile`.
  context.accountDecodedMemory(OPENED_INDEX_BYTES + config.partitionIds.length * PARTITION_BYTES);
  // An index whose analyzer removed every token has no partitions and matches nothing.
  const partitions: Partition[] = new Array(config.partitionIds.length);
  await context.runTasks(config.partitionIds, async (id, i) => {
    partitions[i] = await openPartition(context, config.directory, id, limits, config.sizes);
  });
  const numDocs = partitions.reduce((sum, p) => sum + p.numDocs, 0);
  const info = describeIndex(metadata, column, tokenizer, undefined);
  const opened: OpenedFtsIndex = {
    info,
    tokenizer,
    partitions,
    numDocs,
    totalTokens: undefined,
    directoryBytes: partitions.reduce((sum, p) => sum + p.directoryBytes, OPENED_INDEX_BYTES),
    cacheNamespace:
      options.cacheNamespace === undefined
        ? undefined
        : JSON.stringify([options.cacheNamespace, metadata.uuid]),
  };
  // Retained: file directories only (token sets are cached separately, per partition). The
  // cache is charged the same bytes the operation reserved.
  if (cacheKey !== undefined) {
    options.indexCache?.set(cacheKey, opened, opened.directoryBytes, { priority: 4 });
  }
  return opened;
}

/** Retained bytes of an opened index beyond its partitions (settings and totals). */
const OPENED_INDEX_BYTES = 1024;
/** Retained bytes of one partition descriptor beyond its file directories. */
const PARTITION_BYTES = 512;

/**
 * The corpus token total, computed once per opened index. Every document length has to be read
 * (Lance itself sums the column when it loads an index), so the pass is shaped to cost as
 * little as that allows: chunks of `documentLengthChunkRows` rows across all partitions are
 * read by up to `maxConcurrentReads` readers at once, so the dependent chain is the chunk
 * count divided by the concurrency rather than the chunk count; each reader holds one chunk;
 * and the result is kept on the opened index, so only the first search that needs scores pays.
 * Rows are charged against `maxRowsDecoded` before each chunk is read.
 */
async function corpusTokens(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<number> {
  if (index.totalTokens !== undefined) return index.totalTokens;
  let partition = 0;
  let start = 0;
  const nextChunk = (): { partition: Partition; start: number; end: number } | undefined => {
    for (;;) {
      const current = index.partitions[partition];
      if (current === undefined) return undefined;
      if (start < current.numDocs) {
        const chunk = {
          partition: current,
          start,
          end: Math.min(current.numDocs, start + limits.documentLengthChunkRows),
        };
        start = chunk.end;
        return chunk;
      }
      partition += 1;
      start = 0;
    }
  };
  let total = 0;
  const readers = Array.from({ length: context.budget.maxConcurrentReads ?? 4 });
  await context.runTasks(
    readers,
    async () => {
      for (let chunk = nextChunk(); chunk !== undefined; chunk = nextChunk()) {
        const rows = chunk.end - chunk.start;
        context.reserveDecodedRows(rows);
        counters.documentLengths += rows;
        const read = await readU32Ranges(
          context,
          chunk.partition.docs,
          chunk.partition.numTokensColumn,
          [{ start: chunk.start, end: chunk.end }],
        );
        try {
          for (const value of read.values[0] as Uint32Array) total += value;
        } finally {
          read.release();
        }
      }
    },
    readers.length,
  );
  index.totalTokens = total;
  return total;
}

async function openPartition(
  context: LanceReadContext,
  directory: string,
  id: number,
  limits: LanceFtsLimits,
  sizes: ReadonlyMap<string, number>,
): Promise<Partition> {
  const prefix = `${directory}/part_${id}_`;
  let directoryBytes = PARTITION_BYTES;
  const open = (name: string) =>
    inspectLanceFile(context, `${prefix}${name}`, sizes.get(`part_${id}_${name}`), (bytes) => {
      context.accountDecodedMemory(bytes);
      directoryBytes += bytes;
    });
  const [tokens, invert, docs] = await all([
    open("tokens.lance"),
    open("invert.lance"),
    open("docs.lance"),
  ]);
  const invertMetadata = decodeMetadata(invert.schemaMetadata);
  if (invert.fields.find((field) => field.name === "_posting") === undefined) {
    unsupported("Legacy Lance inverted-index posting layout is not supported");
  }
  const codec = invertMetadata.get("posting_tail_codec") ?? "fixed32_v1";
  if (codec !== "fixed32_v1" && codec !== "varint_delta_v1") {
    unsupported("Unknown Lance posting tail codec", { codec });
  }
  const positions = invert.fields.some((field) => field.name === "_compressed_position")
    ? "shared_stream"
    : invert.fields.some((field) => field.name === "_position")
      ? "legacy_per_doc"
      : "none";
  const posting = resolveColumn(invert, { name: "_posting" });
  let positionColumns:
    | { positionColumn: number; positionOuterItemColumn: number; positionItemColumn: number }
    | undefined;
  if (positions === "legacy_per_doc") {
    const outer = resolveColumn(invert, { name: "_position" });
    const outerItem = childField(invert, outer.fieldId);
    positionColumns = {
      positionColumn: outer.column,
      positionOuterItemColumn: columnOfField(invert, outerItem),
      positionItemColumn: columnOfField(invert, childField(invert, outerItem)),
    };
  }
  const fstColumn = resolveColumn(tokens, { name: "_token_fst_bytes" }).column;
  const fstPage = tokens.columns[fstColumn]?.pages[0];
  const tokenSetBytes =
    fstPage?.encoding.kind === "binary" && fstPage.encoding.bytes.kind === "flat"
      ? (fstPage.bufferSizes[fstPage.encoding.bytes.bufferIndex] ?? 0)
      : 0;
  if (tokenSetBytes > limits.maxTokenSetBytes) {
    budget("FTS token set bytes", limits.maxTokenSetBytes, tokenSetBytes);
  }
  const numTokensColumn = resolveColumn(docs, { name: "_num_tokens" }).column;
  return {
    id,
    tokens,
    invert,
    docs,
    directoryBytes,
    fstColumn,
    tokenSetBytes,
    numDocs: docs.rowCount,
    tailCodec: codec === "fixed32_v1" ? "fixed32" : "varint_delta",
    positions,
    lengthColumn: resolveColumn(invert, { name: "_length" }).column,
    postingColumn: posting.column,
    postingItemColumn: columnOfField(invert, childField(invert, posting.fieldId)),
    rowIdColumn: resolveColumn(docs, { name: "_rowid" }).column,
    numTokensColumn,
    ...positionColumns,
  };
}

/**
 * Like `Promise.all`, but waits for every started read before rejecting, so a failed branch
 * never leaves siblings issuing requests after the caller has already seen the error.
 */
async function all<T extends readonly unknown[] | []>(
  promises: T,
): Promise<{ -readonly [K in keyof T]: Awaited<T[K]> }> {
  const settled = await Promise.allSettled(promises);
  const failure = settled.find((result) => result.status === "rejected");
  if (failure !== undefined) throw (failure as PromiseRejectedResult).reason;
  return settled.map((result) => (result as PromiseFulfilledResult<unknown>).value) as never;
}

/** Run `items` in sequential groups whose summed `weight` fits `capacity` (at least one each). */
export async function inGroups<T>(
  items: readonly T[],
  weight: (item: T) => number,
  capacity: number,
  run: (item: T) => Promise<void>,
): Promise<void> {
  let group: T[] = [];
  let used = 0;
  const flush = async () => {
    if (group.length > 0) await all(group.map(run));
    group = [];
    used = 0;
  };
  for (const item of items) {
    const w = weight(item);
    if (group.length > 0 && used + w > capacity) await flush();
    group.push(item);
    used += w;
  }
  await flush();
}

/**
 * Give each partition's FST to `use`, holding token sets only while they are needed. Groups of
 * partitions are processed concurrently while their token sets fit in half of the remaining
 * memory budget; each set is admitted before it is read and released after `use`, unless the
 * shared cache retains it (the cache charges it separately).
 */
async function withTokenSets(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  cache: SharedMemoryCache | undefined,
  partitions: readonly Partition[],
  use: (partition: Partition, fst: FstMap) => void,
): Promise<void> {
  const capacity = Math.max(1, Math.floor(context.availableMemoryBytes / 2));
  await inGroups(
    partitions,
    (partition) => partition.tokenSetBytes,
    capacity,
    async (partition) => {
      const key =
        cache === undefined || index.cacheNamespace === undefined
          ? undefined
          : JSON.stringify([index.cacheNamespace, "tokens", partition.id]);
      const cached = key === undefined ? undefined : cache?.get<FstMap>(key);
      if (cached !== undefined) {
        context.stats.cacheHits += 1;
        use(partition, cached.value);
        return;
      }
      const read = await readBinaryRanges(context, partition.tokens, partition.fstColumn, [
        { start: 0, end: 1 },
      ]);
      try {
        const bytes = (read.values[0] as Uint8Array[])[0];
        if (bytes === undefined) corrupt("Lance token set is empty");
        const fst = new FstMap(bytes);
        use(partition, fst);
        if (key !== undefined) cache?.set(key, fst, bytes.byteLength + 1024, { priority: 4 });
      } finally {
        read.release();
      }
    },
  );
}

// ----- query execution -------------------------------------------------------------

/** Map key packing a partition index and document id into one safe integer. */
function key(partition: number, doc: number): number {
  return partition * 2 ** 32 + doc;
}

function unkey(value: number): { partition: number; doc: number } {
  const doc = value % 2 ** 32;
  return { partition: (value - doc) / 2 ** 32, doc };
}

class Counters {
  postingEntries = 0;
  candidates = 0;
  phraseCandidates = 0;
  tokensMatched = 0;
  batches = 0;
  rowIds = 0;
  documentLengths = 0;
}

const K1 = 1.2;
const B = 0.75;
const f32 = Math.fround;

function idf(tokenDocs: number, numDocs: number): number {
  return f32(Math.log(f32(f32(f32(numDocs - tokenDocs + 0.5) / f32(tokenDocs + 0.5)) + 1)));
}

function docWeight(freq: number, docTokens: number, avgDocLength: number): number {
  const norm = f32(K1 * f32(1 - B + f32(f32(B * docTokens) / avgDocLength)));
  return f32(f32(f32(K1 + 1) * freq) / f32(freq + norm));
}

/** Count one more scored document against the operation-wide candidate limit. */
function admitCandidate(counters: Counters, limits: LanceFtsLimits): void {
  counters.candidates += 1;
  if (counters.candidates > limits.maxCandidates) {
    budget("FTS candidates", limits.maxCandidates, counters.candidates);
  }
}

/** Count one more phrase candidate (a document whose positions will be read). */
function admitPhraseCandidate(counters: Counters, limits: LanceFtsLimits): void {
  counters.phraseCandidates += 1;
  if (counters.phraseCandidates > limits.maxPhraseCandidates) {
    budget("FTS phrase candidates", limits.maxPhraseCandidates, counters.phraseCandidates);
  }
}

/** One scoring term of one partition: a posting list, located by its block range. */
interface PostingTerm {
  text: string;
  tokenId: number;
  /** Documents in the posting list (`_length`). */
  length: number;
  blockStart: number;
  blockEnd: number;
}

type CompiledLeaf =
  | {
      kind: "match";
      operator: "or" | "and";
      /** Posting lists per partition (deduplicated by token id). */
      terms: Map<Partition, PostingTerm[]>;
    }
  | {
      kind: "phrase";
      slop: number;
      queryPositions: number[];
      /** Per partition: the token id of each phrase term, in order (only complete partitions). */
      ids: Map<Partition, number[]>;
      /** Posting list per distinct token id, per partition. */
      terms: Map<Partition, Map<number, PostingTerm>>;
    };

/** Bytes of one plan map entry (key, value and table slot), excluding any string it owns. */
const PLAN_ENTRY_BYTES = 64;
/** Bytes of one per-partition plan map or array beyond its entries. */
const PLAN_MAP_BYTES = 96;
/** Bytes of one resolved term or located posting list (object, id and its references). */
const PLAN_TERM_BYTES = 96;
/** Bytes a term's text occupies as a string. */
const textBytes = (text: string): number => 32 + 2 * text.length;

interface Compiled {
  /** Bytes of this plan that stay charged to the operation. */
  planBytes: number;
  leaves: Map<LanceFtsQuery, CompiledLeaf>;
  /** Posting entries the scan decodes (every distinct scoring list, every partition). */
  plannedEntries: number;
  /** Index-global document frequency per token text (every partition that holds it). */
  documentFrequency: Map<string, number>;
  avgDocLength: number;
}

interface LeafRequest {
  query: Extract<LanceFtsQuery, { kind: "match" | "phrase" }>;
  tokens: QueryToken[];
}

/** Bytes of one collected query leaf (its request, token list and table slots). */
const LEAF_BYTES = 160;
/** Bytes of one analyzed query token beyond its text. */
const QUERY_TOKEN_BYTES = 64;

/**
 * Collect the leaves of a query tree. `admit` runs before each leaf is collected, so a query
 * with very many clauses is charged as it is traversed rather than after it is flattened.
 */
function leavesOf(
  query: LanceFtsQuery,
  admit: () => void,
  out: LanceFtsQuery[] = [],
): LanceFtsQuery[] {
  if (query.kind === "boolean") {
    for (const clauses of [query.must, query.should, query.mustNot]) {
      for (const clause of clauses ?? []) leavesOf(clause, admit, out);
    }
  } else {
    admit();
    out.push(query);
  }
  return out;
}

/**
 * Pass 1: resolve every leaf's terms against each partition's token set, then read posting
 * locations and index-global document frequencies (one wave). Token sets are only resident
 * while their partition group resolves.
 *
 * The plan holds one entry per term per partition, so it grows with the partition count.
 * Every entry is charged before it is inserted: resolution state to `working`, which is
 * released when compilation ends, and the plan that scoring keeps to the operation.
 */
async function compile(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  cache: SharedMemoryCache | undefined,
  query: LanceFtsQuery,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<Compiled> {
  const working = context.openReservation();
  try {
    return await compilePlan(context, index, cache, query, limits, counters, working);
  } finally {
    working.release();
  }
}

async function compilePlan(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  cache: SharedMemoryCache | undefined,
  query: LanceFtsQuery,
  limits: LanceFtsLimits,
  counters: Counters,
  working: MemoryReservation,
): Promise<Compiled> {
  let planBytes = 0;
  const retain = (bytes: number): void => {
    context.accountDecodedMemory(bytes);
    planBytes += bytes;
  };
  // The query is caller input of any width and length: every leaf and every token is charged
  // before it is collected, so analysis cannot outgrow the budget ahead of the planner.
  const admitToken = (text: string) => working.grow(2 * QUERY_TOKEN_BYTES + textBytes(text));
  const requests: LeafRequest[] = [];
  for (const leaf of leavesOf(query, () => working.grow(LEAF_BYTES))) {
    const q = leaf as LeafRequest["query"];
    if (q.kind === "match") {
      // Lance scores each distinct query term once.
      const seen = new Set<string>();
      const tokens: QueryToken[] = [];
      for (const token of analyzeTokens(q.text, index.tokenizer)) {
        if (seen.has(token.text)) continue;
        admitToken(token.text);
        seen.add(token.text);
        tokens.push(token);
      }
      requests.push({ query: q, tokens });
    } else {
      const tokens: QueryToken[] = [];
      for (const token of analyzeTokens(q.text, index.tokenizer)) {
        admitToken(token.text);
        tokens.push(token);
      }
      requests.push({ query: q, tokens });
    }
  }
  if (requests.some((r) => r.query.kind === "phrase")) {
    if (!index.tokenizer.withPosition || index.partitions.some((p) => p.positions === "none")) {
      unsupported("Phrase queries require an inverted index created with positions");
    }
    if (index.partitions.some((p) => p.positions === "shared_stream")) {
      unsupported("Lance shared position streams are not supported for phrase queries");
    }
  }
  const knownTexts = new Set<string>();
  for (const request of requests) {
    for (const token of request.tokens) {
      if (knownTexts.has(token.text)) continue;
      working.grow(PLAN_ENTRY_BYTES);
      knownTexts.add(token.text);
    }
  }
  // Per partition: token ids needed for scoring, and token ids needed only for frequencies.
  const scoringIds = new Map<Partition, Map<number, string>>();
  const frequencyIds = new Map<Partition, Map<string, number>>();
  const resolved = new Map<
    Partition,
    Map<LeafRequest, { tokenId: number; text: string }[] | number[]>
  >();
  const expansionTexts = new Set<string>();
  await withTokenSets(context, index, cache, index.partitions, (partition, fst) => {
    // This partition's three maps, with room for every exact term and phrase position.
    working.grow(
      3 * (PLAN_MAP_BYTES + PLAN_ENTRY_BYTES) +
        knownTexts.size * PLAN_ENTRY_BYTES +
        requests.length * (PLAN_MAP_BYTES + PLAN_ENTRY_BYTES) +
        requests.reduce((sum, r) => sum + r.tokens.length, 0) * (PLAN_ENTRY_BYTES + 8),
    );
    const scoring = new Map<number, string>();
    const frequency = new Map<string, number>();
    const perLeaf = new Map<LeafRequest, { tokenId: number; text: string }[] | number[]>();
    for (const text of knownTexts) {
      const id = fst.get(encoder.encode(text));
      if (id !== undefined) frequency.set(text, id);
    }
    for (const request of requests) {
      if (request.query.kind === "phrase") {
        const ids: number[] = [];
        for (const token of request.tokens) {
          const id = frequency.get(token.text);
          if (id === undefined) break;
          ids.push(id);
        }
        if (ids.length > 0 && ids.length === request.tokens.length) {
          perLeaf.set(request, ids);
          for (const [i, id] of ids.entries()) scoring.set(id, request.tokens[i]?.text as string);
        }
        continue;
      }
      const q = request.query;
      const matched = new Set<number>();
      const terms = resolvePartitionTerms(
        fst,
        request.tokens,
        q.fuzziness ?? 0,
        q.prefixLength ?? 0,
        q.maxExpansions ?? 50,
        matched,
        // Each kept match becomes a term, a scoring entry and possibly an expansion text.
        (text) => working.grow(PLAN_TERM_BYTES + 2 * PLAN_ENTRY_BYTES + textBytes(text)),
      );
      // AND: a partition only holds a document containing every term if each term resolved
      // in it (fuzzy expansions shared between terms count for both).
      if ((q.operator ?? "or") === "and" && matched.size < request.tokens.length) continue;
      if (terms.length === 0) continue;
      perLeaf.set(request, terms);
      for (const term of terms) {
        scoring.set(term.tokenId, term.text);
        if (!knownTexts.has(term.text)) expansionTexts.add(term.text);
      }
    }
    scoringIds.set(partition, scoring);
    frequencyIds.set(partition, frequency);
    resolved.set(partition, perLeaf);
  });
  // Fuzzy expansions found in one partition still count in every partition holding them.
  if (expansionTexts.size > 0) {
    await withTokenSets(context, index, cache, index.partitions, (partition, fst) => {
      working.grow(expansionTexts.size * PLAN_ENTRY_BYTES);
      const frequency = frequencyIds.get(partition) as Map<string, number>;
      for (const text of expansionTexts) {
        const id = fst.get(encoder.encode(text));
        if (id !== undefined) frequency.set(text, id);
      }
    });
  }
  // One wave: posting locations for scoring terms, posting lengths for every needed token.
  // What scoring keeps is charged to the operation here, before the wave builds it: a
  // frequency per distinct term, and a located posting list per scoring term per partition.
  let frequencyBytes = 2 * PLAN_MAP_BYTES;
  for (const text of knownTexts) frequencyBytes += PLAN_ENTRY_BYTES + textBytes(text);
  for (const text of expansionTexts) frequencyBytes += PLAN_ENTRY_BYTES + textBytes(text);
  retain(frequencyBytes);
  const documentFrequency = new Map<string, number>();
  const located = new Map<Partition, Map<number, PostingTerm>>();
  // Scores need the corpus token total, but only if some term resolved: a query whose terms
  // are all absent returns without reading a single document length. When it is needed it
  // runs alongside this wave rather than ahead of it.
  let scoring = false;
  for (const ids of scoringIds.values()) scoring ||= ids.size > 0;
  const totalTokens = scoring ? corpusTokens(context, index, limits, counters) : undefined;
  await all([
    totalTokens,
    ...index.partitions.map(async (partition) => {
      const scoringTerms = scoringIds.get(partition)?.size ?? 0;
      const frequencyTerms = frequencyIds.get(partition)?.size ?? 0;
      if (scoringTerms + frequencyTerms === 0) return;
      // Request lists for this partition (released with the rest of the resolution state).
      working.grow((scoringTerms + frequencyTerms) * 5 * PLAN_ENTRY_BYTES);
      retain(
        PLAN_MAP_BYTES + PLAN_ENTRY_BYTES + scoringTerms * (PLAN_TERM_BYTES + PLAN_ENTRY_BYTES),
      );
      const scoring = [...(scoringIds.get(partition) ?? new Map<number, string>())];
      const frequency = [...(frequencyIds.get(partition) ?? new Map<string, number>())];
      const lengthIds = [
        ...new Set([...scoring.map(([id]) => id), ...frequency.map(([, id]) => id)]),
      ];
      const [bounds, lengths] = await all([
        readListOffsets(
          context,
          partition.invert,
          partition.postingColumn,
          scoring.map(([id]) => ({ start: id, end: id + 1 })),
        ),
        readU32Ranges(
          context,
          partition.invert,
          partition.lengthColumn,
          lengthIds.map((id) => ({ start: id, end: id + 1 })),
        ),
      ]);
      try {
        const lengthOf = new Map(
          lengthIds.map((id, i) => [id, (lengths.values[i] as Uint32Array)[0] as number]),
        );
        for (const [text, id] of frequency) {
          documentFrequency.set(text, (documentFrequency.get(text) ?? 0) + (lengthOf.get(id) ?? 0));
        }
        const terms = new Map<number, PostingTerm>();
        for (const [i, [id, text]] of scoring.entries()) {
          const offsets = bounds.values[i] as number[];
          terms.set(id, {
            text,
            tokenId: id,
            length: lengthOf.get(id) ?? 0,
            blockStart: offsets[0] as number,
            blockEnd: offsets[1] as number,
          });
        }
        located.set(partition, terms);
      } finally {
        bounds.release();
        lengths.release();
      }
    }),
  ]);
  const leaves = new Map<LanceFtsQuery, CompiledLeaf>();
  for (const request of requests) {
    retain(3 * PLAN_MAP_BYTES + PLAN_ENTRY_BYTES + request.tokens.length * 8);
    if (request.query.kind === "phrase") {
      const ids = new Map<Partition, number[]>();
      const terms = new Map<Partition, Map<number, PostingTerm>>();
      for (const partition of index.partitions) {
        const leafIds = resolved.get(partition)?.get(request) as number[] | undefined;
        if (leafIds === undefined) continue;
        retain(2 * (PLAN_MAP_BYTES + PLAN_ENTRY_BYTES) + leafIds.length * (PLAN_ENTRY_BYTES + 8));
        ids.set(partition, leafIds);
        const byId = new Map<number, PostingTerm>();
        for (const id of leafIds) byId.set(id, located.get(partition)?.get(id) as PostingTerm);
        terms.set(partition, byId);
        counters.tokensMatched += leafIds.length;
      }
      leaves.set(request.query, {
        kind: "phrase",
        slop: request.query.slop ?? 0,
        queryPositions: request.tokens.map((t) => t.position),
        ids,
        terms,
      });
      continue;
    }
    const terms = new Map<Partition, PostingTerm[]>();
    for (const partition of index.partitions) {
      const leafTerms = resolved.get(partition)?.get(request) as
        | { tokenId: number; text: string }[]
        | undefined;
      if (leafTerms === undefined) continue;
      retain(PLAN_MAP_BYTES + PLAN_ENTRY_BYTES + leafTerms.length * 8);
      terms.set(
        partition,
        leafTerms.map((term) => located.get(partition)?.get(term.tokenId) as PostingTerm),
      );
      counters.tokensMatched += leafTerms.length;
    }
    leaves.set(request.query, {
      kind: "match",
      operator: request.query.operator ?? "or",
      terms,
    });
  }
  let plannedEntries = 0;
  for (const terms of located.values())
    for (const term of terms.values()) plannedEntries += term.length;
  return {
    planBytes,
    leaves,
    plannedEntries,
    documentFrequency,
    avgDocLength: totalTokens === undefined ? 0 : f32((await totalTokens) / index.numDocs),
  };
}

/**
 * Map one partition's analyzed query terms to dictionary entries. With `fuzziness > 0` every
 * term is expanded through a Levenshtein automaton; like Lance, `maxExpansions` caps the
 * expansions accumulated across the partition's terms, checked after each kept match (so every
 * later term still contributes its first match). Results are deduplicated by token id.
 * `admit` is called for each match before it is kept, so the caller can reserve its memory.
 */
function resolvePartitionTerms(
  fst: FstMap,
  tokens: readonly QueryToken[],
  fuzziness: number,
  prefixLength: number,
  maxExpansions: number,
  matchedTerms: Set<number>,
  admit: (text: string) => void,
): { tokenId: number; text: string }[] {
  const terms: { tokenId: number; text: string }[] = [];
  const seen = new Set<number>();
  let accumulated = 0;
  for (const [termIndex, token] of tokens.entries()) {
    const matches: { text: string; id: number }[] = [];
    if (fuzziness === 0) {
      const id = fst.get(encoder.encode(token.text));
      if (id !== undefined) {
        admit(token.text);
        matches.push({ text: token.text, id });
      }
    } else {
      const bytes = encoder.encode(token.text);
      const prefixEnd = Math.min(prefixLength, bytes.length);
      if (prefixEnd < bytes.length && ((bytes[prefixEnd] as number) & 0xc0) === 0x80) {
        validation(
          `FTS prefixLength ${prefixLength} splits a multi-byte character in "${token.text}"`,
        );
      }
      fst.search(
        editDistanceAutomaton(token.text, fuzziness, bytes.subarray(0, prefixEnd)),
        (keyBytes, id) => {
          const text = decoder.decode(keyBytes);
          admit(text);
          matches.push({ text, id });
          accumulated += 1;
          return accumulated < maxExpansions;
        },
      );
    }
    if (matches.length > 0) matchedTerms.add(termIndex);
    for (const match of matches) {
      if (seen.has(match.id)) continue;
      seen.add(match.id);
      terms.push({ tokenId: match.id, text: match.text });
    }
  }
  return terms;
}

// ----- streaming operators -----------------------------------------------------------

/**
 * A batch of documents in ascending (partition-local) document order, with scores. Only the
 * first `count` entries are meaningful: producers reuse their columns from batch to batch, so
 * a batch is valid until the next one is requested.
 */
interface ScoredBatch {
  docs: ArrayLike<number>;
  scores: ArrayLike<number>;
  count: number;
}

type DocStream = AsyncGenerator<ScoredBatch, void, undefined>;

interface StreamContext {
  context: LanceReadContext;
  partition: Partition;
  compiled: Compiled;
  numDocs: number;
  limits: LanceFtsLimits;
  counters: Counters;
}

const NO_ENTRIES = new Uint32Array(0);

/**
 * Cursor over one posting list that decodes `postingBlocksPerBatch` blocks at a time. Each
 * batch's decoded arrays are admitted before allocation; the raw block bytes are released as
 * soon as they are decoded. A lease is only ever returned after the cursor has dropped the
 * arrays it paid for, so the accounted bytes never fall below what the cursor can still reach.
 */
export class PostingCursor {
  private docIds = NO_ENTRIES;
  private freqs = NO_ENTRIES;
  private index = 0;
  /** Absolute posting-entry index of `docIds[0]`. */
  private base = 0;
  private nextBlock: number;
  private lease: MemoryLease | undefined;
  private leased = 0;

  constructor(
    private readonly s: StreamContext,
    readonly term: PostingTerm,
  ) {
    this.nextBlock = term.blockStart;
  }

  get exhausted(): boolean {
    return this.index >= this.docIds.length && this.nextBlock >= this.term.blockEnd;
  }

  get needsLoad(): boolean {
    return this.index >= this.docIds.length && this.nextBlock < this.term.blockEnd;
  }

  get doc(): number {
    return this.index < this.docIds.length ? (this.docIds[this.index] as number) : Infinity;
  }

  get freq(): number {
    return this.freqs[this.index] as number;
  }

  /** Decoded posting entries the cursor currently holds. */
  get residentEntries(): number {
    return this.docIds.length;
  }

  /** Bytes currently leased for the decoded entries. */
  get leasedBytes(): number {
    return this.leased;
  }

  /** Absolute index of the current entry within the whole posting list. */
  get entry(): number {
    return this.base + this.index;
  }

  /** Largest document id whose postings are complete without another load. */
  get loadedThrough(): number {
    if (this.nextBlock >= this.term.blockEnd) return Infinity;
    return this.docIds.length === 0 ? -1 : (this.docIds[this.docIds.length - 1] as number);
  }

  advance(): void {
    this.index += 1;
  }

  async load(): Promise<void> {
    const { context, limits, counters, partition } = this.s;
    const first = this.nextBlock;
    const last = Math.min(this.term.blockEnd, first + limits.postingBlocksPerBatch);
    const firstEntry = (first - this.term.blockStart) * BLOCK_LEN;
    const entries =
      Math.min(this.term.length, (last - this.term.blockStart) * BLOCK_LEN) - firstEntry;
    if (entries < 0) corrupt("Lance posting list block count does not match its length");
    counters.postingEntries += entries;
    if (counters.postingEntries > limits.maxPostingEntries) {
      budget("FTS posting entries", limits.maxPostingEntries, counters.postingEntries);
    }
    context.reserveDecodedRows(entries);
    // The previous batch is fully consumed (`needsLoad`): drop it, return its charge, and only
    // then admit the next batch.
    this.release();
    this.lease = context.leaseDecodedMemory(entries * 8);
    this.leased = entries * 8;
    const read = await readBinaryRanges(context, partition.invert, partition.postingItemColumn, [
      { start: first, end: last },
    ]);
    try {
      const blocks = read.values[0] as Uint8Array[];
      const docIds = new Uint32Array(entries);
      const freqs = new Uint32Array(entries);
      let written = 0;
      for (const [i, block] of blocks.entries()) {
        const blockEntry = (first + i - this.term.blockStart) * BLOCK_LEN;
        const count = Math.min(BLOCK_LEN, this.term.length - blockEntry);
        if (count <= 0) corrupt("Lance posting list block count does not match its length");
        // Blocks of exactly 128 entries are bit-packed; only a final short block uses the tail codec.
        decodeBlockInto(block, count, partition.tailCodec, docIds, freqs, written);
        written += count;
      }
      if (written !== entries) corrupt("Lance posting list block count does not match its length");
      this.docIds = docIds;
      this.freqs = freqs;
      this.index = 0;
      this.base = firstEntry;
      this.nextBlock = last;
    } finally {
      read.release();
    }
  }

  /** Drop the decoded entries, then return their charge. */
  release(): void {
    this.base += this.index;
    this.docIds = NO_ENTRIES;
    this.freqs = NO_ENTRIES;
    this.index = 0;
    this.lease?.release();
    this.lease = undefined;
    this.leased = 0;
  }
}

/**
 * Lengths of `docs` (ascending) gathered into one admitted array, released by the caller.
 * Nearby documents share a planned request, so bytes charged stay 4 per document.
 */
async function readDocLengths(s: StreamContext, docs: ArrayLike<number>) {
  return await readU32Rows(s.context, s.partition.docs, s.partition.numTokensColumn, docs);
}

/** Retained bytes of one typed-array column beyond its elements. */
const COLUMN_BYTES = 96;

/**
 * Documents merged from posting cursors, held in columns that are allocated once per merge and
 * reused from batch to batch. Row `i`, cursor `c` lives at index `i * width + c`.
 */
interface MergedBatch {
  count: number;
  /** Number of cursors. */
  width: number;
  docs: Uint32Array;
  /** 1 when the cursor's posting list holds the document. */
  present: Uint8Array;
  freqs: Uint32Array;
  /** Absolute entry index within the posting list (locates positions), when requested. */
  entries: Uint32Array | undefined;
}

/**
 * Most documents one merged batch can hold: the batch limit, the partition's document count,
 * and what the posting lists can produce (their shortest for an intersection, their sum for a
 * union).
 */
function mergeCapacity(
  s: StreamContext,
  terms: readonly PostingTerm[],
  requireAll: boolean,
): number {
  if (terms.length === 0) return 0;
  const lengths = terms.map((term) => term.length);
  const documents = requireAll ? Math.min(...lengths) : lengths.reduce((sum, n) => sum + n, 0);
  return Math.min(s.limits.documentsPerBatch, s.partition.numDocs, documents);
}

/**
 * Document-at-a-time merge over posting cursors. Each round decodes the next batch for any
 * cursor that needs one, then takes up to `capacity` documents whose postings are all loaded
 * (`requireAll` keeps only documents present in every list), so memory never depends on
 * posting-list length.
 *
 * The batch columns are admitted once, before they are allocated, at `capacity` rows. `admit`
 * runs for each document before it joins the batch, so work limits stop the merge at the first
 * document past the limit instead of after the batch is built.
 */
async function* mergeCursors(
  s: StreamContext,
  cursors: readonly PostingCursor[],
  requireAll: boolean,
  capacity: number,
  withEntries: boolean,
  admit: () => void,
): AsyncGenerator<MergedBatch, void, undefined> {
  if (cursors.length === 0 || capacity === 0) return;
  const width = cursors.length;
  const lease = s.context.leaseDecodedMemory(
    4 * COLUMN_BYTES + capacity * (4 + width * (withEntries ? 9 : 5)),
  );
  try {
    const batch: MergedBatch = {
      count: 0,
      width,
      docs: new Uint32Array(capacity),
      present: new Uint8Array(capacity * width),
      freqs: new Uint32Array(capacity * width),
      entries: withEntries ? new Uint32Array(capacity * width) : undefined,
    };
    for (;;) {
      await all(cursors.filter((c) => c.needsLoad).map((c) => c.load()));
      if (requireAll ? cursors.some((c) => c.exhausted) : cursors.every((c) => c.exhausted)) return;
      const bound = Math.min(...cursors.filter((c) => !c.exhausted).map((c) => c.loadedThrough));
      batch.count = 0;
      while (batch.count < capacity) {
        let doc = Infinity;
        for (const c of cursors) if (c.doc < doc) doc = c.doc;
        if (doc === Infinity || doc > bound) break;
        let present = 0;
        for (const c of cursors) if (c.doc === doc) present += 1;
        if (!requireAll || present === width) {
          admit();
          const row = batch.count * width;
          batch.docs[batch.count] = doc;
          for (const [i, c] of cursors.entries()) {
            const here = c.doc === doc;
            batch.present[row + i] = here ? 1 : 0;
            batch.freqs[row + i] = here ? c.freq : 0;
            if (batch.entries !== undefined) batch.entries[row + i] = here ? c.entry : 0;
          }
          batch.count += 1;
        }
        for (const c of cursors) if (c.doc === doc) c.advance();
      }
      if (batch.count > 0) yield batch;
    }
  } finally {
    lease.release();
  }
}

function termWeight(s: StreamContext, text: string): number {
  return idf(s.compiled.documentFrequency.get(text) ?? 0, s.numDocs);
}

/** BM25 over match terms, batch by batch. */
async function* matchStream(
  s: StreamContext,
  leaf: Extract<CompiledLeaf, { kind: "match" }>,
): DocStream {
  const terms = leaf.terms.get(s.partition) ?? [];
  // AND needs the document in every posting list the partition resolved (every fuzzy
  // expansion included), as Lance's WAND does.
  const requireAll = leaf.operator === "and";
  const capacity = mergeCapacity(s, terms, requireAll);
  if (capacity === 0) return;
  const cursors = terms.map((term) => new PostingCursor(s, term));
  const weights = terms.map((term) => termWeight(s, term.text));
  const scoreLease = s.context.leaseDecodedMemory(COLUMN_BYTES + capacity * 4);
  try {
    const scores = new Float32Array(capacity);
    const admit = () => admitCandidate(s.counters, s.limits);
    for await (const batch of mergeCursors(s, cursors, requireAll, capacity, false, admit)) {
      s.counters.batches += 1;
      const lengths = await readDocLengths(s, batch.docs.subarray(0, batch.count));
      try {
        for (let i = 0; i < batch.count; i += 1) {
          let score = 0;
          for (let c = 0; c < batch.width; c += 1) {
            const at = i * batch.width + c;
            if (batch.present[at] !== 1) continue;
            score = f32(
              score +
                f32(
                  (weights[c] as number) *
                    docWeight(
                      batch.freqs[at] as number,
                      lengths.values[i] as number,
                      s.compiled.avgDocLength,
                    ),
                ),
            );
          }
          scores[i] = score;
        }
      } finally {
        lengths.release();
      }
      yield { docs: batch.docs, scores, count: batch.count };
    }
  } finally {
    scoreLease.release();
    for (const cursor of cursors) cursor.release();
  }
}

/** Phrases, batch by batch: candidates come from the merge, positions are read per batch. */
async function* phraseStream(
  s: StreamContext,
  leaf: Extract<CompiledLeaf, { kind: "phrase" }>,
): DocStream {
  const ids = leaf.ids.get(s.partition);
  const byId = leaf.terms.get(s.partition);
  if (ids === undefined || byId === undefined) return;
  const distinct = [...byId.values()];
  const capacity = mergeCapacity(s, distinct, true);
  if (capacity === 0) return;
  const cursorOf = new Map(distinct.map((term, i) => [term.tokenId, i]));
  /** The merge cursor of each phrase term, in phrase order. */
  const cursorOfTerm = ids.map((id) => cursorOf.get(id) as number);
  const cursors = distinct.map((term) => new PostingCursor(s, term));
  const weights = ids.map((id) => termWeight(s, (byId.get(id) as PostingTerm).text));
  // Matching documents of a batch: their ids, batch rows and scores.
  const keptLease = s.context.leaseDecodedMemory(3 * COLUMN_BYTES + capacity * 12);
  try {
    const keptDocs = new Uint32Array(capacity);
    const keptRows = new Uint32Array(capacity);
    const scores = new Float32Array(capacity);
    // The span of each term's per-document position lists never changes: read it once.
    const spans = await readListOffsets(
      s.context,
      s.partition.invert,
      s.partition.positionColumn as number,
      distinct.map((term) => ({ start: term.tokenId, end: term.tokenId + 1 })),
    );
    const spanBase = distinct.map((_t, i) => (spans.values[i] as number[])[0] as number);
    spans.release();
    // The phrase-candidate limit is checked as each candidate joins the intersection.
    const admit = () => admitPhraseCandidate(s.counters, s.limits);
    for await (const batch of mergeCursors(s, cursors, true, capacity, true, admit)) {
      s.counters.batches += 1;
      let kept = 0;
      const positions = await readPositionBatch(s, spanBase, batch);
      try {
        for (let i = 0; i < batch.count; i += 1) {
          const perDistinct = positions.values[i] as number[][];
          const perTerm = cursorOfTerm.map((cursor) => perDistinct[cursor] as number[]);
          if (!phraseMatches(perTerm, leaf.queryPositions, leaf.slop)) continue;
          admitCandidate(s.counters, s.limits);
          keptDocs[kept] = batch.docs[i] as number;
          keptRows[kept] = i;
          kept += 1;
        }
      } finally {
        positions.release();
      }
      if (kept === 0) continue;
      const lengths = await readDocLengths(s, keptDocs.subarray(0, kept));
      try {
        for (let j = 0; j < kept; j += 1) {
          const row = (keptRows[j] as number) * batch.width;
          let score = 0;
          // Repeated phrase terms each contribute, as each is a separate Lance posting iterator.
          for (const [t, cursor] of cursorOfTerm.entries()) {
            score = f32(
              score +
                f32(
                  (weights[t] as number) *
                    docWeight(
                      batch.freqs[row + cursor] as number,
                      lengths.values[j] as number,
                      s.compiled.avgDocLength,
                    ),
                ),
            );
          }
          scores[j] = score;
        }
      } finally {
        lengths.release();
      }
      yield { docs: keptDocs, scores, count: kept };
    }
  } finally {
    keptLease.release();
    for (const cursor of cursors) cursor.release();
  }
}

/** Bytes of one planned position read (its entry index and block range). */
const POSITION_PLAN_BYTES = 48;
/** Bytes of one decoded position list beyond its positions. */
const POSITION_LIST_BYTES = 32;

/**
 * Positions of one batch of candidates via the legacy `_position` list-of-lists layout:
 * per term one contiguous read of block offsets over the batch's entry span, then the
 * candidates' position blocks. The batch's plan and result tables are reserved before they are
 * built, and every position list is validated and admitted before decoding.
 */
async function readPositionBatch(
  s: StreamContext,
  spanBase: readonly number[],
  batch: MergedBatch,
): Promise<{ values: number[][][]; release: () => void }> {
  const { context, partition } = s;
  const { count, width } = batch;
  const entries = batch.entries as Uint32Array;
  const entry = (term: number, candidate: number) => entries[candidate * width + term] as number;
  const held = context.openReservation();
  try {
    held.grow(
      count * width * (POSITION_PLAN_BYTES + POSITION_LIST_BYTES) +
        (count + width) * POSITION_LIST_BYTES,
    );
    const docOffsets = await readListOffsets(
      context,
      partition.invert,
      partition.positionOuterItemColumn as number,
      Array.from({ length: width }, (_v, t) => ({
        start: (spanBase[t] as number) + entry(t, 0),
        end: (spanBase[t] as number) + entry(t, count - 1) + 1,
      })),
    );
    const blockRanges: { start: number; end: number }[] = [];
    try {
      for (let t = 0; t < width; t += 1) {
        const offsets = docOffsets.values[t] as number[];
        for (let c = 0; c < count; c += 1) {
          const at = entry(t, c) - entry(t, 0);
          blockRanges.push({ start: offsets[at] as number, end: offsets[at + 1] as number });
        }
      }
    } finally {
      docOffsets.release();
    }
    const blocks = await readBinaryRanges(
      context,
      partition.invert,
      partition.positionItemColumn as number,
      blockRanges,
    );
    try {
      const values: number[][][] = Array.from({ length: count }, () => []);
      for (let t = 0; t < width; t += 1) {
        for (let c = 0; c < count; c += 1) {
          (values[c] as number[][])[t] = decodePositions(
            blocks.values[t * count + c] as Uint8Array[],
            (bytes) => held.grow(bytes),
          );
        }
      }
      return { values, release: () => held.release() };
    } finally {
      blocks.release();
    }
  } catch (error) {
    held.release();
    throw error;
  }
}

/** Pull-based view of a stream: the current document and score, loading batches on demand. */
class Peekable {
  private batch: ScoredBatch | undefined;
  private index = 0;
  private done = false;

  constructor(private readonly stream: DocStream) {}

  async head(): Promise<number> {
    while (!this.done && (this.batch === undefined || this.index >= this.batch.count)) {
      const next = await this.stream.next();
      if (next.done === true) this.done = true;
      else {
        this.batch = next.value;
        this.index = 0;
      }
    }
    return this.done ? Infinity : (this.batch?.docs[this.index] as number);
  }

  score(): number {
    return this.batch?.scores[this.index] as number;
  }

  /** Skip documents below `doc`; returns the new head. */
  async seek(doc: number): Promise<number> {
    let head = await this.head();
    while (head < doc) {
      this.index += 1;
      head = await this.head();
    }
    return head;
  }

  advance(): void {
    this.index += 1;
  }

  async close(): Promise<void> {
    await this.stream.return(undefined);
  }
}

/** Bytes of one document in a composed batch (its id and its score). */
const SCORED_DOC_BYTES = 16;

/**
 * Boolean composition over child streams, all in ascending document order: `must` documents
 * are intersected and their scores summed, `should` scores are added (or unioned when there is
 * no `must`), and `mustNot` documents are dropped. Only one batch per child is resident.
 */
async function* booleanStream(
  s: StreamContext,
  query: Extract<LanceFtsQuery, { kind: "boolean" }>,
): DocStream {
  const open = (clauses: readonly LanceFtsQuery[] | undefined) =>
    (clauses ?? []).map((clause) => new Peekable(streamFor(s, clause)));
  const must = open(query.must);
  const should = open(query.should);
  const mustNot = open(query.mustNot);
  const batch = s.context.openReservation();
  try {
    let docs: number[] = [];
    let scores: number[] = [];
    for (;;) {
      let doc: number;
      let score = 0;
      if (must.length > 0) {
        // Align every must stream on a common document.
        doc = await (must[0] as Peekable).head();
        for (let aligned = false; !aligned && doc !== Infinity; ) {
          aligned = true;
          for (const child of must) {
            const head = await child.seek(doc);
            if (head > doc) {
              doc = head;
              aligned = false;
            }
          }
        }
        if (doc === Infinity) break;
        for (const child of must) {
          score = f32(score + child.score());
          child.advance();
        }
        for (const child of should) {
          if ((await child.seek(doc)) === doc) {
            score = f32(score + child.score());
            child.advance();
          }
        }
      } else {
        doc = Infinity;
        for (const child of should) doc = Math.min(doc, await child.head());
        if (doc === Infinity) break;
        for (const child of should) {
          if ((await child.head()) === doc) {
            score = f32(score + child.score());
            child.advance();
          }
        }
      }
      let excluded = false;
      for (const child of mustNot) if ((await child.seek(doc)) === doc) excluded = true;
      if (excluded) continue;
      batch.grow(SCORED_DOC_BYTES);
      docs.push(doc);
      scores.push(score);
      if (docs.length >= s.limits.documentsPerBatch) {
        yield { docs, scores, count: docs.length };
        docs = [];
        scores = [];
        batch.release();
      }
    }
    if (docs.length > 0) yield { docs, scores, count: docs.length };
  } finally {
    batch.release();
    await all([...must, ...should, ...mustNot].map((child) => child.close()));
  }
}

function streamFor(s: StreamContext, query: LanceFtsQuery): DocStream {
  if (query.kind === "boolean") return booleanStream(s, query);
  const leaf = s.compiled.leaves.get(query);
  if (leaf === undefined) corrupt("FTS query leaf was not compiled");
  return leaf.kind === "match" ? matchStream(s, leaf) : phraseStream(s, leaf);
}

/** One ranked document: its packed partition/document key, its score, and its row id. */
interface RankedEntry {
  key: number;
  score: number;
  rowId: bigint;
}

/** Bounded top-k by score (ties: smaller key first), admitted before it can grow. */
class TopK {
  private readonly scores: number[] = [];
  private readonly keys: number[] = [];
  private readonly rowIds: bigint[] = [];

  /**
   * `after` restricts the heap to entries ranking strictly after it. Ranks form a total order
   * (score descending, then key ascending), so the top `capacity` entries after the last one
   * already examined are exactly the next best ones.
   */
  constructor(
    readonly capacity: number,
    private readonly after?: { score: number; key: number },
  ) {}

  /** True when (score, key) ranks strictly worse than (otherScore, otherKey). */
  private static worse(score: number, key: number, otherScore: number, otherKey: number): boolean {
    return score < otherScore || (score === otherScore && key > otherKey);
  }

  /** Whether `push` would keep this entry right now. */
  accepts(score: number, key: number): boolean {
    const after = this.after;
    if (after !== undefined && !TopK.worse(score, key, after.score, after.key)) return false;
    return (
      this.scores.length < this.capacity ||
      TopK.worse(this.scores[0] as number, this.keys[0] as number, score, key)
    );
  }

  push(score: number, key: number, rowId = 0n): void {
    if (!this.accepts(score, key)) return;
    const n = this.scores.length;
    if (n < this.capacity) {
      this.scores.push(score);
      this.keys.push(key);
      this.rowIds.push(rowId);
      this.up(n);
    } else {
      this.scores[0] = score;
      this.keys[0] = key;
      this.rowIds[0] = rowId;
      this.down(0);
    }
  }

  // Min-heap on rank: the root is the worst kept entry.
  private less(i: number, j: number): boolean {
    return TopK.worse(
      this.scores[i] as number,
      this.keys[i] as number,
      this.scores[j] as number,
      this.keys[j] as number,
    );
  }

  private swap(i: number, j: number): void {
    [this.scores[i], this.scores[j]] = [this.scores[j] as number, this.scores[i] as number];
    [this.keys[i], this.keys[j]] = [this.keys[j] as number, this.keys[i] as number];
    [this.rowIds[i], this.rowIds[j]] = [this.rowIds[j] as bigint, this.rowIds[i] as bigint];
  }

  private up(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (!this.less(i, parent)) break;
      this.swap(i, parent);
      i = parent;
    }
  }

  private down(i: number): void {
    const n = this.scores.length;
    for (;;) {
      const l = 2 * i + 1;
      const r = l + 1;
      let m = i;
      if (l < n && this.less(l, m)) m = l;
      if (r < n && this.less(r, m)) m = r;
      if (m === i) return;
      this.swap(i, m);
      i = m;
    }
  }

  /** Kept entries, best first. */
  sorted(): RankedEntry[] {
    return this.keys
      .map(
        (key, i): RankedEntry => ({
          key,
          score: this.scores[i] as number,
          rowId: this.rowIds[i] as bigint,
        }),
      )
      .sort((a, b) => b.score - a.score || a.key - b.key);
  }
}

/**
 * Retained bytes charged per ranked entry: its heap slots, its sorted entry, and the hit (row
 * id and score) it becomes.
 */
const RANKED_ENTRY_BYTES = 160;

/** Reports, for each row id of an indexed document, whether the row still exists. */
export type LanceRowLiveness = (rowIds: readonly bigint[]) => Promise<readonly boolean[]>;

interface RankContext {
  context: LanceReadContext;
  index: OpenedFtsIndex;
  counters: Counters;
  top: TopK;
  /** Most candidates one liveness round may resolve (the buffered-row bound). */
  maxRound: number;
  isLive: LanceRowLiveness | undefined;
}

/**
 * Offer one scored batch to the top-k, admitting only rows that still exist.
 *
 * Rows deleted after indexing stay in the index, so a candidate's row id has to be read and
 * checked before it may take a heap slot. Only candidates that would enter the heap right now
 * are worth that read. Each round takes the batch's best such candidates not yet examined,
 * resolves their row ids in one read, and pushes the live ones; live rows raise the heap's
 * threshold, so a batch without deleted candidates takes exactly one round, and a batch whose
 * documents cannot beat the heap takes none. A round that lost candidates to deletion is
 * followed by a larger one (doubling, up to the buffered-row bound), so a long run of deleted
 * hits costs logarithmically many reads. Nothing is rescored and no posting is decoded twice.
 */
async function offerBatch(
  r: RankContext,
  partitionIndex: number,
  batch: ScoredBatch,
  scratch: MemoryReservation,
): Promise<void> {
  let after: { score: number; key: number } | undefined;
  for (let size = Math.min(r.top.capacity, r.maxRound); ; size = Math.min(size * 2, r.maxRound)) {
    scratch.release();
    scratch.grow(size * RANKED_ENTRY_BYTES);
    const round = new TopK(size, after);
    for (let i = 0; i < batch.count; i += 1) {
      const score = batch.scores[i] as number;
      const packed = key(partitionIndex, batch.docs[i] as number);
      if (r.top.accepts(score, packed)) round.push(score, packed);
    }
    const candidates = round.sorted();
    if (candidates.length === 0) return;
    r.counters.rowIds += candidates.length;
    const rowIds = await readRowIds(
      r.context,
      r.index,
      candidates.map(({ key: packed }) => unkey(packed)),
    );
    const live = r.isLive === undefined ? undefined : await r.isLive(rowIds);
    for (const [i, candidate] of candidates.entries()) {
      if (live === undefined || live[i] === true) {
        r.top.push(candidate.score, candidate.key, rowIds[i] as bigint);
      }
    }
    // Fewer than a full round means every candidate of the batch has now been examined.
    if (candidates.length < size) return;
    after = candidates.at(-1);
  }
}

/**
 * Pass 2: stream every partition's documents into a bounded top-k of live rows. Partitions run
 * concurrently in groups sized so that their posting batches fit the remaining memory budget.
 * The caller has already leased the heap's `k` entries.
 */
async function rank(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  compiled: Compiled,
  query: LanceFtsQuery,
  k: number,
  isLive: LanceRowLiveness | undefined,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<{ ranked: RankedEntry[]; total: number }> {
  // Check the whole posting plan before any payload is read (cursors re-check per batch).
  if (counters.postingEntries + compiled.plannedEntries > limits.maxPostingEntries) {
    budget(
      "FTS posting entries",
      limits.maxPostingEntries,
      counters.postingEntries + compiled.plannedEntries,
    );
  }
  const r: RankContext = {
    context,
    index,
    counters,
    top: new TopK(k),
    maxRound: Math.max(k, context.budget.maxBufferedRows ?? limits.documentsPerBatch),
    isLive,
  };
  let total = 0;
  // Per partition: one decoded posting batch per scoring term plus one document batch.
  const perPartition =
    Math.max(1, compiled.leaves.size) * limits.postingBlocksPerBatch * BLOCK_LEN * 8 * 2 +
    limits.documentsPerBatch * 64;
  const capacity = Math.max(1, Math.floor(context.availableMemoryBytes / 2));
  await inGroups(
    index.partitions.map((partition, partitionIndex) => ({ partition, partitionIndex })),
    () => perPartition,
    capacity,
    async ({ partition, partitionIndex }) => {
      const s: StreamContext = {
        context,
        partition,
        compiled,
        numDocs: index.numDocs,
        limits,
        counters,
      };
      const scratch = context.openReservation();
      try {
        for await (const batch of streamFor(s, query)) {
          total += batch.count;
          await offerBatch(r, partitionIndex, batch, scratch);
        }
      } finally {
        scratch.release();
      }
    },
  );
  return { ranked: r.top.sorted(), total };
}

export async function searchFtsIndex(
  options: Source & {
    indexName: string;
    query: LanceFtsQuery;
    limit: number;
    limits?: Partial<LanceFtsLimits>;
    /**
     * Tells which indexed rows still exist. Rows deleted after indexing stay in the index;
     * with this, they are filtered before ranking so `hits` are the best `limit` live rows.
     * Without it every indexed row is treated as live.
     */
    isLive?: LanceRowLiveness;
  },
): Promise<{
  index: LanceFtsIndexInfo;
  /** The best `limit` live rows, best first. */
  hits: LanceFtsHit[];
  /** Release the memory charged for `hits`. Call when they are no longer needed. */
  close: () => void;
  /** Documents the query matched in the index, including rows deleted after indexing. */
  total: number;
  stats: LanceFtsSearchStats;
}> {
  const limits = { ...DEFAULT_FTS_LIMITS, ...options.limits };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) validation(`Invalid FTS limit ${name}`);
  }
  if (!Number.isSafeInteger(options.limit) || options.limit <= 0) {
    validation("FTS search limit must be a positive integer");
  }
  // Validate the whole query tree before any index data is read.
  validateQuery(options.query, 0);
  const context = options.context;
  const bufferedRows = context.budget.maxBufferedRows;
  if (bufferedRows !== undefined && options.limit > bufferedRows) {
    budget("buffered rows", bufferedRows, options.limit);
  }
  const indexes = await loadFtsIndexes(options);
  const selected = indexes.find((index) => index.metadata.name === options.indexName);
  if (selected === undefined) validation(`Unknown Lance inverted index ${options.indexName}`);
  const index = await openFtsIndex(options, selected.metadata, selected.column, limits);
  const counters = new Counters();
  const compiled = await compile(
    context,
    index,
    options.indexCache,
    options.query,
    limits,
    counters,
  );
  // The heap, the sorted ranking and the hits it becomes, admitted before the scan.
  const page = context.leaseDecodedMemory(options.limit * RANKED_ENTRY_BYTES);
  try {
    const { ranked, total } = await rank(
      context,
      index,
      compiled,
      options.query,
      options.limit,
      options.isLive,
      limits,
      counters,
    );
    return {
      index: index.info,
      hits: ranked.map(({ rowId, score }) => ({ rowId, score })),
      close: () => page.release(),
      total,
      stats: {
        partitions: index.partitions.length,
        tokensMatched: counters.tokensMatched,
        postingEntriesDecoded: counters.postingEntries,
        candidatesScored: counters.candidates,
        phraseCandidatesChecked: counters.phraseCandidates,
        batches: counters.batches,
        rowIdsResolved: counters.rowIds,
        documentLengthsSummed: counters.documentLengths,
        directoryBytes: index.directoryBytes,
        planBytes: compiled.planBytes,
      },
    };
  } catch (error) {
    page.release();
    throw error;
  }
}

/** Row ids of one partition's documents, in the order given. */
async function readRowIds(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  keys: readonly { partition: number; doc: number }[],
): Promise<bigint[]> {
  const byPartition = new Map<number, { doc: number; at: number }[]>();
  for (const [at, { partition, doc }] of keys.entries()) {
    const list = byPartition.get(partition) ?? [];
    list.push({ doc, at });
    byPartition.set(partition, list);
  }
  const out = new Array<bigint>(keys.length);
  await all(
    [...byPartition].map(async ([partitionIndex, docs]) => {
      const partition = index.partitions[partitionIndex] as Partition;
      docs.sort((a, b) => a.doc - b.doc);
      const read = await readU64Rows(
        context,
        partition.docs,
        partition.rowIdColumn,
        docs.map(({ doc }) => doc),
      );
      try {
        for (const [i, { at }] of docs.entries()) out[at] = read.values[i] as bigint;
      } finally {
        read.release();
      }
    }),
  );
  return out;
}

// ----- block decoding ----------------------------------------------------------------

/** One block of unpacked values, reused by every (synchronous) block decode. */
const BLOCK_SCRATCH = new Uint32Array(BLOCK_LEN);

/**
 * Decode one posting block into `docIds`/`freqs` at `at`. A block of exactly 128 entries is
 * bit-packed; a shorter final block uses the index's tail codec.
 */
function decodeBlockInto(
  block: Uint8Array,
  count: number,
  tailCodec: "fixed32" | "varint_delta",
  docIds: Uint32Array,
  freqs: Uint32Array,
  at: number,
): void {
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  if (count === BLOCK_LEN) {
    if (block.length < 10) corrupt("Lance posting block is truncated");
    const first = view.getUint32(4, true);
    const docBits = block[8] as number;
    let offset = 9;
    if (docBits > 32 || offset + packedBlockBytes(docBits) + 1 > block.length) {
      corrupt("Lance posting block is truncated");
    }
    const scratch = BLOCK_SCRATCH;
    unpackSortedBlock(block, offset, docBits, first, scratch);
    docIds.set(scratch, at);
    offset += packedBlockBytes(docBits);
    const freqBits = block[offset] as number;
    offset += 1;
    if (freqBits > 32 || offset + packedBlockBytes(freqBits) > block.length) {
      corrupt("Lance posting block is truncated");
    }
    unpackBlock(block, offset, freqBits, scratch);
    freqs.set(scratch, at);
    return;
  }
  let offset = 4;
  if (tailCodec === "fixed32") {
    if (block.length < 4 + count * 8) corrupt("Lance posting tail block is truncated");
    for (let i = 0; i < count; i += 1) {
      docIds[at + i] = view.getUint32(offset + i * 4, true);
      freqs[at + i] = view.getUint32(offset + (count + i) * 4, true);
    }
    return;
  }
  const readVarint = (): number => {
    let value = 0;
    let shift = 0;
    for (;;) {
      const byte = block[offset++];
      if (byte === undefined) corrupt("Lance posting tail block is truncated");
      value += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) {
        if (value > 0xffffffff) corrupt("Lance posting varint exceeds 32 bits");
        return value;
      }
      shift += 7;
      if (shift > 28) corrupt("Lance posting varint is too long");
    }
  };
  let previous = 0;
  for (let i = 0; i < count; i += 1) {
    previous = i === 0 ? readVarint() : previous + readVarint();
    if (previous > 0xffffffff) corrupt("Lance posting document id exceeds 32 bits");
    docIds[at + i] = previous;
  }
  for (let i = 0; i < count; i += 1) freqs[at + i] = readVarint();
}

/** Decode a whole block-compressed posting list (used for verification and tests). */
export function decodePosting(
  blocks: readonly Uint8Array[],
  length: number,
  tailCodec: "fixed32" | "varint_delta",
): { docIds: Uint32Array; freqs: Uint32Array } {
  if (blocks.length !== Math.ceil(length / BLOCK_LEN)) {
    corrupt("Lance posting list block count does not match its length", {
      blocks: blocks.length,
      length,
    });
  }
  const docIds = new Uint32Array(length);
  const freqs = new Uint32Array(length);
  for (const [i, block] of blocks.entries()) {
    decodeBlockInto(
      block,
      Math.min(BLOCK_LEN, length - i * BLOCK_LEN),
      tailCodec,
      docIds,
      freqs,
      i * BLOCK_LEN,
    );
  }
  return { docIds, freqs };
}

/**
 * Decode one document's position list. The header count is validated against the block
 * layout and `reserve(bytes)` is called before the result is allocated, so an oversized or
 * corrupt list fails before it can allocate.
 */
export function decodePositions(
  blocks: readonly Uint8Array[],
  reserve: (bytes: number) => void = () => undefined,
): number[] {
  const header = blocks[0];
  if (header === undefined || header.length < 4) corrupt("Lance position list header is missing");
  const total = new DataView(header.buffer, header.byteOffset, header.byteLength).getUint32(
    0,
    true,
  );
  if (blocks.length - 1 !== Math.ceil(total / BLOCK_LEN)) {
    corrupt("Lance position list length does not match its header", {
      total,
      blocks: blocks.length - 1,
    });
  }
  reserve(total * 8);
  const out: number[] = [];
  const scratch = BLOCK_SCRATCH;
  for (let b = 1; b < blocks.length; b += 1) {
    const block = blocks[b] as Uint8Array;
    const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
    if (out.length + BLOCK_LEN <= total) {
      if (
        block.length < 5 ||
        (block[4] as number) > 32 ||
        5 + packedBlockBytes(block[4] as number) > block.length
      ) {
        corrupt("Lance position block is truncated");
      }
      unpackSortedBlock(block, 5, block[4] as number, view.getUint32(0, true), scratch);
      for (let i = 0; i < BLOCK_LEN; i += 1) out.push(scratch[i] as number);
    } else {
      const count = total - out.length;
      if (block.length < count * 4) corrupt("Lance position block is truncated");
      for (let i = 0; i < count; i += 1) out.push(view.getUint32(i * 4, true));
    }
  }
  return out;
}

const MAX_QUERY_DEPTH = 16;

function validateQuery(query: LanceFtsQuery, depth: number): void {
  if (depth > MAX_QUERY_DEPTH) validation("FTS boolean queries are nested too deeply");
  const kind = (query as { kind?: unknown } | null)?.kind;
  switch (kind) {
    case "match": {
      const q = query as Extract<LanceFtsQuery, { kind: "match" }>;
      if (typeof q.text !== "string") validation("FTS query text must be a string");
      if (q.fuzziness !== undefined && ![0, 1, 2].includes(q.fuzziness)) {
        validation("FTS fuzziness must be 0, 1 or 2");
      }
      if (
        q.prefixLength !== undefined &&
        !(Number.isSafeInteger(q.prefixLength) && q.prefixLength >= 0)
      ) {
        validation("Invalid FTS prefixLength");
      }
      if (
        q.maxExpansions !== undefined &&
        !(Number.isSafeInteger(q.maxExpansions) && q.maxExpansions > 0)
      ) {
        validation("Invalid FTS maxExpansions");
      }
      if (q.operator !== undefined && q.operator !== "or" && q.operator !== "and") {
        validation("FTS operator must be or/and");
      }
      return;
    }
    case "phrase": {
      const q = query as Extract<LanceFtsQuery, { kind: "phrase" }>;
      if (typeof q.text !== "string") validation("FTS query text must be a string");
      if (q.slop !== undefined && !(Number.isSafeInteger(q.slop) && q.slop >= 0)) {
        validation("Phrase slop must be a non-negative integer");
      }
      return;
    }
    case "boolean": {
      const q = query as Extract<LanceFtsQuery, { kind: "boolean" }>;
      if ((q.must ?? []).length + (q.should ?? []).length === 0) {
        validation("A boolean FTS query needs at least one must or should clause");
      }
      for (const clauses of [q.must, q.should, q.mustNot]) {
        for (const clause of clauses ?? []) validateQuery(clause, depth + 1);
      }
      return;
    }
    default:
      validation(`Unknown FTS query kind ${String(kind)}`);
  }
}

// ----- term resolution -------------------------------------------------------------

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type EditState = { row: number[]; pending: number[]; dead: boolean; consumed: number };

function editDistanceAutomaton(
  term: string,
  maxDistance: number,
  prefixBytes: Uint8Array,
): FstAutomaton<EditState> {
  // Levenshtein over Unicode scalar values; the FST is walked byte by byte, so partial UTF-8
  // sequences are buffered until a full scalar is available. `prefixBytes` must match exactly.
  const target = [...term];
  const initialRow = Array.from({ length: target.length + 1 }, (_v, i) => i);
  return {
    start: () => ({ row: initialRow, pending: [], dead: false, consumed: 0 }),
    canMatch: (state) => !state.dead,
    isMatch: (state) =>
      !state.dead &&
      state.pending.length === 0 &&
      state.consumed >= prefixBytes.length &&
      (state.row[target.length] as number) <= maxDistance,
    accept: (state, byte) => {
      if (state.dead) return state;
      if (state.consumed < prefixBytes.length && byte !== prefixBytes[state.consumed]) {
        return { ...state, dead: true };
      }
      const pending = [...state.pending, byte];
      const consumed = state.consumed + 1;
      let expected = 1;
      const lead = pending[0] as number;
      if (lead >= 0xf0) expected = 4;
      else if (lead >= 0xe0) expected = 3;
      else if (lead >= 0xc0) expected = 2;
      if (pending.length < expected) return { ...state, pending, consumed };
      const char = decoder.decode(Uint8Array.from(pending));
      const row = [(state.row[0] as number) + 1];
      for (let i = 1; i <= target.length; i += 1) {
        const substitution = (state.row[i - 1] as number) + (target[i - 1] === char ? 0 : 1);
        row.push(Math.min(substitution, (state.row[i] as number) + 1, (row[i - 1] as number) + 1));
      }
      const alive = Math.min(...row) <= maxDistance;
      return { row, pending: [], dead: !alive, consumed };
    },
  };
}

export function phraseMatches(
  positions: readonly (readonly number[])[],
  queryPositions: readonly number[],
  slop: number,
): boolean {
  interface Cursor {
    values: readonly number[];
    query: number;
    index: number;
  }
  const cursors: Cursor[] = positions.map((values, i) => ({
    values,
    query: queryPositions[i] as number,
    index: 0,
  }));
  // First index in `values[from..]` whose value is >= target.
  const advanceTo = (cursor: Cursor, target: number): void => {
    if (cursor.index >= cursor.values.length) return;
    let low = cursor.index;
    let high = cursor.values.length;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if ((cursor.values[mid] as number) < target) low = mid + 1;
      else high = mid;
    }
    cursor.index = low;
  };
  const advanceToRelative = (cursor: Cursor, relative: number): void =>
    advanceTo(cursor, Math.max(0, relative + cursor.query));
  const relativeOf = (cursor: Cursor): number | undefined => {
    const value = cursor.values[cursor.index];
    return value === undefined ? undefined : value - cursor.query;
  };
  if (cursors.length === 0) return false;
  if (slop === 0) {
    const ordered = [...cursors].sort((a, b) => a.values.length - b.values.length);
    const lead = ordered[0] as Cursor;
    for (;;) {
      const anchor = lead.values[lead.index];
      if (anchor === undefined) return false;
      const base = anchor - lead.query;
      if (base < 0) {
        lead.index = Math.min(lead.index + 1, lead.values.length);
        continue;
      }
      let nextLeadRelative: number | undefined;
      for (const follower of ordered.slice(1)) {
        advanceTo(follower, base + follower.query);
        const position = follower.values[follower.index];
        if (position === undefined) return false;
        if (position !== base + follower.query) {
          nextLeadRelative = position - follower.query;
          break;
        }
      }
      if (nextLeadRelative === undefined) return true;
      advanceToRelative(lead, nextLeadRelative);
    }
  }
  const ordered = [...cursors].sort((a, b) => a.query - b.query);
  for (;;) {
    let maxRelative: number | undefined;
    let aligned = true;
    for (let i = 0; i + 1 < ordered.length; i += 1) {
      const last = relativeOf(ordered[i] as Cursor);
      const next = relativeOf(ordered[i + 1] as Cursor);
      if (last === undefined || next === undefined) return false;
      const moveTo = last > next ? last : Math.max(last + 1, next - slop);
      maxRelative = maxRelative === undefined ? moveTo : Math.max(maxRelative, moveTo);
      if (!(last <= next && next <= last + slop)) {
        aligned = false;
        break;
      }
    }
    if (aligned) return true;
    for (const cursor of ordered) advanceToRelative(cursor, maxRelative as number);
  }
}

// ----- helpers ---------------------------------------------------------------------

function decodeMetadata(raw: Record<string, Uint8Array>): Map<string, string> {
  return new Map(Object.entries(raw).map(([name, value]) => [name, decoder.decode(value)]));
}

function parsePartitionIds(json: string): number[] {
  let value: unknown;
  try {
    value = JSON.parse(json);
  } catch {
    corrupt("Lance inverted index partition list is not JSON");
  }
  if (!Array.isArray(value) || !value.every(Number.isSafeInteger)) {
    corrupt("Lance inverted index partition list is invalid");
  }
  return value as number[];
}

function validation(message: string): never {
  throw new LakeqlError("LAKEQL_VALIDATION_ERROR", message);
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}

function budget(metric: string, limit: number, actual: number): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Lance full-text search exceeded ${metric} limit (${actual} > ${limit})`,
    { metric, limit, actual },
  );
}
