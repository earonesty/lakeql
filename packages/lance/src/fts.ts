/**
 * Full-text search over Lance inverted indexes (`INVERTED` scalar index, on-disk v1/v2
 * partitioned layout with FST token sets and block-compressed posting lists).
 *
 * Memory contract: every buffer is admitted against the operation budget before it is
 * allocated, and resident state is bounded by the query shape and `LanceFtsLimits`, never by
 * index size or match count.
 *   - Opening an index keeps only file directories and corpus totals. Document lengths are
 *     summed in bounded chunks and then dropped.
 *   - A partition's FST token set (one blob in the format, so it must be resident while terms
 *     are resolved) is loaded in memory-sized partition groups and released after resolution,
 *     unless the shared cache keeps it.
 *   - Postings are decoded `postingBlocksPerBatch` blocks at a time per term and scored
 *     document-at-a-time across terms in batches of `documentsPerBatch`. Document lengths and
 *     phrase positions are read per batch and released with it.
 *   - Ranking keeps a bounded top-k heap. Refilling past rows deleted after indexing reruns the
 *     scan with a larger k instead of buffering every match.
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
  readU64Ranges,
  resolveColumn,
} from "./column.js";
import { type InspectedLanceFile, inspectLanceFile } from "./file.js";
import { type FstAutomaton, FstMap } from "./fst.js";
import {
  analyze,
  assertSupportedAnalyzer,
  type LanceTokenizerParams,
  parseTokenizerParams,
  type QueryToken,
} from "./fts-tokenizer.js";
import type { LanceReadContext, MemoryLease } from "./io.js";
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
}

export const DEFAULT_FTS_LIMITS: LanceFtsLimits = {
  maxTokenSetBytes: 64 * 1024 * 1024,
  maxPostingEntries: 4_000_000,
  maxCandidates: 1_000_000,
  maxPhraseCandidates: 100_000,
  postingBlocksPerBatch: 64,
  documentsPerBatch: 4096,
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
  /** Scans of the postings: 1, plus one per refill rerun past deleted rows. */
  scans: number;
}

interface Partition {
  id: number;
  tokens: InspectedLanceFile;
  invert: InspectedLanceFile;
  docs: InspectedLanceFile;
  fstColumn: number;
  tokenSetBytes: number;
  numDocs: number;
  totalTokens: number;
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

/** What an opened index retains: directories and totals only, independent of index size. */
export interface OpenedFtsIndex {
  info: LanceFtsIndexInfo;
  tokenizer: LanceTokenizerParams;
  partitions: Partition[];
  numDocs: number;
  totalTokens: number;
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
  const meta = await inspectLanceFile(
    context,
    `${directory}/metadata.lance`,
    sizes.get("metadata.lance"),
  );
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
  // An index whose analyzer removed every token has no partitions and matches nothing.
  const partitions: Partition[] = new Array(config.partitionIds.length);
  await context.runTasks(config.partitionIds, async (id, i) => {
    partitions[i] = await openPartition(context, config.directory, id, limits, config.sizes);
  });
  const numDocs = partitions.reduce((sum, p) => sum + p.numDocs, 0);
  const totalTokens = partitions.reduce((sum, p) => sum + p.totalTokens, 0);
  const info = describeIndex(metadata, column, tokenizer, undefined);
  const opened: OpenedFtsIndex = {
    info,
    tokenizer,
    partitions,
    numDocs,
    totalTokens,
    cacheNamespace:
      options.cacheNamespace === undefined
        ? undefined
        : JSON.stringify([options.cacheNamespace, metadata.uuid]),
  };
  // Retained: file directories only (token sets are cached separately, per partition).
  if (cacheKey !== undefined) {
    options.indexCache?.set(cacheKey, opened, 4096 + partitions.length * 16_384, { priority: 4 });
  }
  return opened;
}

/** Document lengths summed per chunk: `DOC_LENGTH_CHUNK_ROWS * 4` bytes resident at a time. */
const DOC_LENGTH_CHUNK_ROWS = 16_384;

async function openPartition(
  context: LanceReadContext,
  directory: string,
  id: number,
  limits: LanceFtsLimits,
  sizes: ReadonlyMap<string, number>,
): Promise<Partition> {
  const prefix = `${directory}/part_${id}_`;
  const open = (name: string) =>
    inspectLanceFile(context, `${prefix}${name}`, sizes.get(`part_${id}_${name}`));
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
  // BM25 needs the corpus token total (Lance sums every document length too). Stream it in
  // bounded chunks so the column is never resident; per-document lengths are read per batch.
  let totalTokens = 0;
  for (let start = 0; start < docs.rowCount; start += DOC_LENGTH_CHUNK_ROWS) {
    const end = Math.min(docs.rowCount, start + DOC_LENGTH_CHUNK_ROWS);
    context.reserveDecodedRows(end - start);
    const chunk = await readU32Ranges(context, docs, numTokensColumn, [{ start, end }]);
    for (const value of chunk.values[0] as Uint32Array) totalTokens += value;
    chunk.release();
  }
  return {
    id,
    tokens,
    invert,
    docs,
    fstColumn,
    tokenSetBytes,
    numDocs: docs.rowCount,
    totalTokens,
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
  scans = 0;
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

interface Compiled {
  leaves: Map<LanceFtsQuery, CompiledLeaf>;
  /** Posting entries a full scan decodes (every distinct scoring list, every partition). */
  plannedEntries: number;
  /** Index-global document frequency per token text (every partition that holds it). */
  documentFrequency: Map<string, number>;
  avgDocLength: number;
}

interface LeafRequest {
  query: Extract<LanceFtsQuery, { kind: "match" | "phrase" }>;
  tokens: QueryToken[];
}

function leavesOf(query: LanceFtsQuery, out: LanceFtsQuery[] = []): LanceFtsQuery[] {
  if (query.kind === "boolean") {
    for (const clause of [
      ...(query.must ?? []),
      ...(query.should ?? []),
      ...(query.mustNot ?? []),
    ]) {
      leavesOf(clause, out);
    }
  } else out.push(query);
  return out;
}

/**
 * Pass 1: resolve every leaf's terms against each partition's token set, then read posting
 * locations and index-global document frequencies (one wave). Token sets are only resident
 * while their partition group resolves.
 */
async function compile(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  cache: SharedMemoryCache | undefined,
  query: LanceFtsQuery,
  counters: Counters,
): Promise<Compiled> {
  const requests: LeafRequest[] = leavesOf(query).map((leaf) => {
    const q = leaf as LeafRequest["query"];
    const tokens = analyze(q.text, index.tokenizer);
    if (q.kind === "match") {
      // Lance scores each distinct query term once.
      const seen = new Set<string>();
      return { query: q, tokens: tokens.filter((t) => !seen.has(t.text) && seen.add(t.text)) };
    }
    return { query: q, tokens };
  });
  if (requests.some((r) => r.query.kind === "phrase")) {
    if (!index.tokenizer.withPosition || index.partitions.some((p) => p.positions === "none")) {
      unsupported("Phrase queries require an inverted index created with positions");
    }
    if (index.partitions.some((p) => p.positions === "shared_stream")) {
      unsupported("Lance shared position streams are not supported for phrase queries");
    }
  }
  const knownTexts = new Set(requests.flatMap((r) => r.tokens.map((t) => t.text)));
  // Per partition: token ids needed for scoring, and token ids needed only for frequencies.
  const scoringIds = new Map<Partition, Map<number, string>>();
  const frequencyIds = new Map<Partition, Map<string, number>>();
  const resolved = new Map<
    Partition,
    Map<LeafRequest, { tokenId: number; text: string }[] | number[]>
  >();
  const expansionTexts = new Set<string>();
  await withTokenSets(context, index, cache, index.partitions, (partition, fst) => {
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
      const frequency = frequencyIds.get(partition) as Map<string, number>;
      for (const text of expansionTexts) {
        const id = fst.get(encoder.encode(text));
        if (id !== undefined) frequency.set(text, id);
      }
    });
  }
  // One wave: posting locations for scoring terms, posting lengths for every needed token.
  const documentFrequency = new Map<string, number>();
  const located = new Map<Partition, Map<number, PostingTerm>>();
  await all(
    index.partitions.map(async (partition) => {
      const scoring = [...(scoringIds.get(partition) ?? new Map<number, string>())];
      const frequency = [...(frequencyIds.get(partition) ?? new Map<string, number>())];
      const lengthIds = [
        ...new Set([...scoring.map(([id]) => id), ...frequency.map(([, id]) => id)]),
      ];
      if (lengthIds.length === 0) return;
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
  );
  const leaves = new Map<LanceFtsQuery, CompiledLeaf>();
  for (const request of requests) {
    if (request.query.kind === "phrase") {
      const ids = new Map<Partition, number[]>();
      const terms = new Map<Partition, Map<number, PostingTerm>>();
      for (const partition of index.partitions) {
        const leafIds = resolved.get(partition)?.get(request) as number[] | undefined;
        if (leafIds === undefined) continue;
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
    leaves,
    plannedEntries,
    documentFrequency,
    avgDocLength: f32(index.totalTokens / index.numDocs),
  };
}

/**
 * Map one partition's analyzed query terms to dictionary entries. With `fuzziness > 0` every
 * term is expanded through a Levenshtein automaton; like Lance, `maxExpansions` caps the
 * expansions accumulated across the partition's terms, checked after each kept match (so every
 * later term still contributes its first match). Results are deduplicated by token id.
 */
function resolvePartitionTerms(
  fst: FstMap,
  tokens: readonly QueryToken[],
  fuzziness: number,
  prefixLength: number,
  maxExpansions: number,
  matchedTerms: Set<number>,
): { tokenId: number; text: string }[] {
  const terms: { tokenId: number; text: string }[] = [];
  const seen = new Set<number>();
  let accumulated = 0;
  for (const [termIndex, token] of tokens.entries()) {
    const matches: { text: string; id: number }[] = [];
    if (fuzziness === 0) {
      const id = fst.get(encoder.encode(token.text));
      if (id !== undefined) matches.push({ text: token.text, id });
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
          matches.push({ text: decoder.decode(keyBytes), id });
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

/** A batch of documents in ascending (partition-local) document order, with scores. */
interface ScoredBatch {
  docs: number[];
  scores: number[];
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

/**
 * Cursor over one posting list that decodes `postingBlocksPerBatch` blocks at a time. Each
 * batch's decoded arrays are admitted before allocation and released when the next batch
 * replaces them; the raw block bytes are released as soon as they are decoded.
 */
class PostingCursor {
  private docIds = new Uint32Array(0);
  private freqs = new Uint32Array(0);
  private index = 0;
  /** Absolute posting-entry index of `docIds[0]`. */
  private base = 0;
  private nextBlock: number;
  private lease: MemoryLease | undefined;

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
    this.release();
    this.lease = context.leaseDecodedMemory(entries * 8);
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

  release(): void {
    this.lease?.release();
    this.lease = undefined;
  }
}

/** Lengths of `docs` (ascending), read as coalesced runs and released by the caller. */
async function readDocLengths(
  s: StreamContext,
  docs: readonly number[],
): Promise<{ lengths: Uint32Array; release: () => void }> {
  const ranges: { start: number; end: number }[] = [];
  // Exactly the scored documents: adjacent ones share a range, and nearby ranges are merged
  // into one physical request by the read planner, so bytes charged stay 4 per document.
  for (const doc of docs) {
    const last = ranges.at(-1);
    if (last !== undefined && doc === last.end) last.end = doc + 1;
    else ranges.push({ start: doc, end: doc + 1 });
  }
  const lease = s.context.leaseDecodedMemory(docs.length * 4);
  try {
    const read = await readU32Ranges(
      s.context,
      s.partition.docs,
      s.partition.numTokensColumn,
      ranges,
    );
    const lengths = new Uint32Array(docs.length);
    let r = 0;
    for (const [i, doc] of docs.entries()) {
      while (doc >= (ranges[r] as { end: number }).end) r += 1;
      lengths[i] = (read.values[r] as Uint32Array)[
        doc - (ranges[r] as { start: number }).start
      ] as number;
    }
    read.release();
    return { lengths, release: () => lease.release() };
  } catch (error) {
    lease.release();
    throw error;
  }
}

/** A document present in some of the merged posting lists. */
interface Member {
  cursor: number;
  freq: number;
  /** Absolute entry index within that posting list (locates its positions). */
  entry: number;
}

/**
 * Document-at-a-time merge over posting cursors. Each round decodes the next batch for any
 * cursor that needs one, then takes up to `documentsPerBatch` documents whose postings are all
 * loaded (`requireAll` keeps only documents present in every list), so memory never depends
 * on posting-list length.
 */
async function* mergeCursors(
  s: StreamContext,
  cursors: readonly PostingCursor[],
  requireAll: boolean,
): AsyncGenerator<{ docs: number[]; members: Member[][] }, void, undefined> {
  for (;;) {
    await all(cursors.filter((c) => c.needsLoad).map((c) => c.load()));
    if (cursors.length === 0) return;
    if (requireAll ? cursors.some((c) => c.exhausted) : cursors.every((c) => c.exhausted)) return;
    const bound = Math.min(...cursors.filter((c) => !c.exhausted).map((c) => c.loadedThrough));
    const docs: number[] = [];
    const members: Member[][] = [];
    while (docs.length < s.limits.documentsPerBatch) {
      let doc = Infinity;
      for (const c of cursors) if (c.doc < doc) doc = c.doc;
      if (doc === Infinity || doc > bound) break;
      const present: Member[] = [];
      for (const [i, c] of cursors.entries()) {
        if (c.doc === doc) present.push({ cursor: i, freq: c.freq, entry: c.entry });
      }
      if (!requireAll || present.length === cursors.length) {
        docs.push(doc);
        members.push(present);
      }
      for (const member of present) (cursors[member.cursor] as PostingCursor).advance();
    }
    if (docs.length > 0) yield { docs, members };
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
  if (terms.length === 0) return;
  const cursors = terms.map((term) => new PostingCursor(s, term));
  const weights = terms.map((term) => termWeight(s, term.text));
  try {
    // AND needs the document in every posting list the partition resolved (every fuzzy
    // expansion included), as Lance's WAND does.
    for await (const batch of mergeCursors(s, cursors, leaf.operator === "and")) {
      s.counters.batches += 1;
      const lengths = await readDocLengths(s, batch.docs);
      try {
        const scores = batch.docs.map((_doc, i) => {
          admitCandidate(s.counters, s.limits);
          let score = 0;
          for (const member of batch.members[i] as Member[]) {
            score = f32(
              score +
                f32(
                  (weights[member.cursor] as number) *
                    docWeight(member.freq, lengths.lengths[i] as number, s.compiled.avgDocLength),
                ),
            );
          }
          return score;
        });
        yield { docs: batch.docs, scores };
      } finally {
        lengths.release();
      }
    }
  } finally {
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
  const cursorOf = new Map(distinct.map((term, i) => [term.tokenId, i]));
  const cursors = distinct.map((term) => new PostingCursor(s, term));
  const weights = ids.map((id) => termWeight(s, (byId.get(id) as PostingTerm).text));
  // The span of each term's per-document position lists never changes: read it once.
  const spans = await readListOffsets(
    s.context,
    s.partition.invert,
    s.partition.positionColumn as number,
    distinct.map((term) => ({ start: term.tokenId, end: term.tokenId + 1 })),
  );
  const spanBase = distinct.map((_t, i) => (spans.values[i] as number[])[0] as number);
  spans.release();
  try {
    for await (const batch of mergeCursors(s, cursors, true)) {
      s.counters.batches += 1;
      s.counters.phraseCandidates += batch.docs.length;
      if (s.counters.phraseCandidates > s.limits.maxPhraseCandidates) {
        budget("FTS phrase candidates", s.limits.maxPhraseCandidates, s.counters.phraseCandidates);
      }
      const positions = await readPositionBatch(s, spanBase, batch.members, distinct.length);
      try {
        const kept: number[] = [];
        const keptMembers: Member[][] = [];
        for (const [i, doc] of batch.docs.entries()) {
          const perDistinct = positions.values[i] as number[][];
          const perTerm = ids.map((id) => perDistinct[cursorOf.get(id) as number] as number[]);
          if (!phraseMatches(perTerm, leaf.queryPositions, leaf.slop)) continue;
          kept.push(doc);
          keptMembers.push(batch.members[i] as Member[]);
        }
        if (kept.length === 0) continue;
        const lengths = await readDocLengths(s, kept);
        try {
          const scores = kept.map((_doc, i) => {
            admitCandidate(s.counters, s.limits);
            const freqOf = new Map((keptMembers[i] as Member[]).map((m) => [m.cursor, m.freq]));
            let score = 0;
            // Repeated phrase terms each contribute, as each is a separate Lance posting iterator.
            for (const [t, id] of ids.entries()) {
              score = f32(
                score +
                  f32(
                    (weights[t] as number) *
                      docWeight(
                        freqOf.get(cursorOf.get(id) as number) as number,
                        lengths.lengths[i] as number,
                        s.compiled.avgDocLength,
                      ),
                  ),
              );
            }
            return score;
          });
          yield { docs: kept, scores };
        } finally {
          lengths.release();
        }
      } finally {
        positions.release();
      }
    }
  } finally {
    for (const cursor of cursors) cursor.release();
  }
}

/**
 * Positions of one batch of candidates via the legacy `_position` list-of-lists layout:
 * per term one contiguous read of block offsets over the batch's entry span, then the
 * candidates' position blocks. Every position list is validated and admitted before decoding.
 */
async function readPositionBatch(
  s: StreamContext,
  spanBase: readonly number[],
  members: readonly Member[][],
  termCount: number,
): Promise<{ values: number[][][]; release: () => void }> {
  const { context, partition } = s;
  const entries = Array.from({ length: termCount }, (_v, t) =>
    members.map((m) => (m.find((member) => member.cursor === t) as Member).entry),
  );
  const docOffsets = await readListOffsets(
    context,
    partition.invert,
    partition.positionOuterItemColumn as number,
    entries.map((list, t) => ({
      start: (spanBase[t] as number) + (list[0] as number),
      end: (spanBase[t] as number) + (list.at(-1) as number) + 1,
    })),
  );
  const blockRanges = entries.flatMap((list, t) => {
    const offsets = docOffsets.values[t] as number[];
    return list.map((entry) => {
      const at = entry - (list[0] as number);
      return { start: offsets[at] as number, end: offsets[at + 1] as number };
    });
  });
  docOffsets.release();
  const blocks = await readBinaryRanges(
    context,
    partition.invert,
    partition.positionItemColumn as number,
    blockRanges,
  );
  const leases: MemoryLease[] = [];
  const release = () => {
    for (const lease of leases) lease.release();
  };
  try {
    const values: number[][][] = members.map(() => []);
    for (let t = 0; t < termCount; t += 1) {
      for (let c = 0; c < members.length; c += 1) {
        (values[c] as number[][])[t] = decodePositions(
          blocks.values[t * members.length + c] as Uint8Array[],
          (bytes) => leases.push(context.leaseDecodedMemory(bytes)),
        );
      }
    }
    return { values, release };
  } catch (error) {
    release();
    throw error;
  } finally {
    blocks.release();
  }
}

/** Pull-based view of a stream: the current document and score, loading batches on demand. */
class Peekable {
  private batch: ScoredBatch | undefined;
  private index = 0;
  private done = false;

  constructor(private readonly stream: DocStream) {}

  async head(): Promise<number> {
    while (!this.done && (this.batch === undefined || this.index >= this.batch.docs.length)) {
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
      docs.push(doc);
      scores.push(score);
      if (docs.length >= s.limits.documentsPerBatch) {
        yield { docs, scores };
        docs = [];
        scores = [];
      }
    }
    if (docs.length > 0) yield { docs, scores };
  } finally {
    await all([...must, ...should, ...mustNot].map((child) => child.close()));
  }
}

function streamFor(s: StreamContext, query: LanceFtsQuery): DocStream {
  if (query.kind === "boolean") return booleanStream(s, query);
  const leaf = s.compiled.leaves.get(query);
  if (leaf === undefined) corrupt("FTS query leaf was not compiled");
  return leaf.kind === "match" ? matchStream(s, leaf) : phraseStream(s, leaf);
}

/** Bounded top-k by score (ties: smaller key first), admitted before it can grow. */
class TopK {
  private readonly scores: number[] = [];
  private readonly keys: number[] = [];

  constructor(readonly capacity: number) {}

  /** True when (score, key) ranks strictly worse than (otherScore, otherKey). */
  private static worse(score: number, key: number, otherScore: number, otherKey: number): boolean {
    return score < otherScore || (score === otherScore && key > otherKey);
  }

  push(score: number, key: number): void {
    const n = this.scores.length;
    if (n < this.capacity) {
      this.scores.push(score);
      this.keys.push(key);
      this.up(n);
    } else if (n > 0 && TopK.worse(this.scores[0] as number, this.keys[0] as number, score, key)) {
      this.scores[0] = score;
      this.keys[0] = key;
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

  sorted(): [number, number][] {
    return this.keys
      .map((key, i): [number, number] => [key, this.scores[i] as number])
      .sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  }
}

/** Retained bytes charged per kept ranking entry (score, key, and their array slots). */
const RANKED_ENTRY_BYTES = 48;

/**
 * Pass 2: stream every partition's documents into a bounded top-k. Partitions run concurrently
 * in groups sized so that their posting batches fit the remaining memory budget.
 */
async function rank(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  compiled: Compiled,
  query: LanceFtsQuery,
  k: number,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<{ ranked: [number, number][]; total: number }> {
  const bufferedRows = context.budget.maxBufferedRows;
  if (bufferedRows !== undefined && k > bufferedRows) budget("buffered rows", bufferedRows, k);
  // Check the whole posting plan before any payload is read (cursors re-check per batch).
  if (counters.postingEntries + compiled.plannedEntries > limits.maxPostingEntries) {
    budget(
      "FTS posting entries",
      limits.maxPostingEntries,
      counters.postingEntries + compiled.plannedEntries,
    );
  }
  const heapLease = context.leaseDecodedMemory(k * RANKED_ENTRY_BYTES);
  try {
    counters.scans += 1;
    const top = new TopK(k);
    let total = 0;
    const leaves = leavesOf(query).length;
    // Per partition: one decoded posting batch per scoring term plus one document batch.
    const perPartition =
      Math.max(1, leaves) * limits.postingBlocksPerBatch * BLOCK_LEN * 8 * 2 +
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
        for await (const batch of streamFor(s, query)) {
          for (const [i, doc] of batch.docs.entries()) {
            top.push(batch.scores[i] as number, key(partitionIndex, doc));
          }
          total += batch.docs.length;
        }
      },
    );
    return { ranked: top.sorted(), total };
  } finally {
    heapLease.release();
  }
}

export async function searchFtsIndex(
  options: Source & {
    indexName: string;
    query: LanceFtsQuery;
    limit: number;
    limits?: Partial<LanceFtsLimits>;
  },
): Promise<{
  index: LanceFtsIndexInfo;
  hits: LanceFtsHit[];
  /** Resolve further ranked hits, for callers that must skip deleted rows. */
  more: (from: number, count: number) => Promise<LanceFtsHit[]>;
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
  const indexes = await loadFtsIndexes(options);
  const selected = indexes.find((index) => index.metadata.name === options.indexName);
  if (selected === undefined) validation(`Unknown Lance inverted index ${options.indexName}`);
  const index = await openFtsIndex(options, selected.metadata, selected.column, limits);
  const counters = new Counters();
  const compiled = await compile(
    options.context,
    index,
    options.indexCache,
    options.query,
    counters,
  );
  let k = options.limit;
  let { ranked, total } = await rank(
    options.context,
    index,
    compiled,
    options.query,
    k,
    limits,
    counters,
  );
  const resolve = async (from: number, count: number): Promise<LanceFtsHit[]> => {
    // Rows deleted after indexing can exhaust the kept top-k: rescan with a larger bound
    // rather than buffering every match.
    if (from + count > ranked.length && ranked.length < total) {
      k = Math.min(total, Math.max(2 * k, from + count));
      ({ ranked, total } = await rank(
        options.context,
        index,
        compiled,
        options.query,
        k,
        limits,
        counters,
      ));
    }
    const slice = ranked.slice(from, from + count);
    if (slice.length === 0) return [];
    const rowIds = await readRowIds(
      options.context,
      index,
      slice.map(([k]) => unkey(k)),
    );
    return slice.map(([, score], i) => ({ rowId: rowIds[i] as bigint, score }));
  };
  const hits = await resolve(0, options.limit);
  return {
    index: index.info,
    hits,
    more: resolve,
    get total() {
      return total;
    },
    stats: {
      partitions: index.partitions.length,
      tokensMatched: counters.tokensMatched,
      get postingEntriesDecoded() {
        return counters.postingEntries;
      },
      get candidatesScored() {
        return counters.candidates;
      },
      get phraseCandidatesChecked() {
        return counters.phraseCandidates;
      },
      get batches() {
        return counters.batches;
      },
      get scans() {
        return counters.scans;
      },
    } as LanceFtsSearchStats,
  };
}

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
      const ranges = docs.map(({ doc }) => ({ start: doc, end: doc + 1 }));
      const read = await readU64Ranges(context, partition.docs, partition.rowIdColumn, ranges);
      try {
        for (const [i, { at }] of docs.entries()) {
          out[at] = (read.values[i] as bigint[])[0] as bigint;
        }
      } finally {
        read.release();
      }
    }),
  );
  return out;
}

// ----- block decoding ----------------------------------------------------------------

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
    const scratch = new Uint32Array(BLOCK_LEN);
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
  const scratch = new Uint32Array(BLOCK_LEN);
  for (const block of blocks.slice(1)) {
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
      const clauses = [...(q.must ?? []), ...(q.should ?? []), ...(q.mustNot ?? [])];
      if ((q.must ?? []).length + (q.should ?? []).length === 0) {
        validation("A boolean FTS query needs at least one must or should clause");
      }
      for (const clause of clauses) validateQuery(clause, depth + 1);
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
