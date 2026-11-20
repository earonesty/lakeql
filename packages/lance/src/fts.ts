/**
 * Full-text search over Lance inverted indexes (`INVERTED` scalar index, on-disk v1/v2
 * partitioned layout with FST token sets and block-compressed posting lists).
 *
 * Query cost model (all reads are range requests, nothing is scanned):
 *   - open (cached): `metadata.lance`, and per partition one footer read per file, the FST token
 *     set, and the `_num_tokens` doc-length column (needed for BM25's average document length,
 *     exactly as Lance loads it);
 *   - per query: posting-list offsets and lengths for the matched tokens (one wave), their
 *     block offsets (one wave) and bytes (one wave); positions only for phrase candidates;
 *   - `_rowid` for the ranked results.
 * Dependent round trips are bounded by the nesting depth of the file layout, never by data size.
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
} from "./fts-tokenizer.js";
import type { LanceReadContext } from "./io.js";
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
  /** Largest serialized FST token set that may be fetched for one partition. */
  maxTokenSetBytes: number;
  /** Largest `_num_tokens` column (4 bytes per document) fetched for one partition. */
  maxDocLengthBytes: number;
  /** Posting entries (document, frequency pairs) decoded by one query. */
  maxPostingEntries: number;
  /** Distinct documents scored by one query. */
  maxCandidates: number;
  /** Phrase candidates whose positions are read. */
  maxPhraseCandidates: number;
}

export const DEFAULT_FTS_LIMITS: LanceFtsLimits = {
  maxTokenSetBytes: 64 * 1024 * 1024,
  maxDocLengthBytes: 64 * 1024 * 1024,
  maxPostingEntries: 4_000_000,
  maxCandidates: 1_000_000,
  maxPhraseCandidates: 100_000,
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
      /** Leading characters that must match exactly when expanding fuzzy terms. */
      prefixLength?: number;
      /** Most dictionary terms a fuzzy query term may expand to. Default 50. */
      maxExpansions?: number;
    }
  | { kind: "phrase"; text: string; slop?: number }
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
}

interface Partition {
  id: number;
  invert: InspectedLanceFile;
  docs: InspectedLanceFile;
  fst: FstMap;
  tokenCount: number;
  tokenSetBytes: number;
  docLengthBytes: number;
  numDocs: number;
  /** Per-document token counts, indexed by partition-local document id. */
  numTokens: Uint32Array;
  totalTokens: number;
  tailCodec: "fixed32" | "varint_delta";
  positions: "none" | "legacy_per_doc" | "shared_stream";
  maxScoreColumn: number;
  lengthColumn: number;
  postingColumn: number;
  postingItemColumn: number;
  rowIdColumn: number;
  positionColumn?: number;
  positionOuterItemColumn?: number;
  positionItemColumn?: number;
}

export interface OpenedFtsIndex {
  info: LanceFtsIndexInfo;
  tokenizer: LanceTokenizerParams;
  partitions: Partition[];
  numDocs: number;
  totalTokens: number;
}

/** Work the opening of an index required, re-checked against the caller's limits on reuse. */
function enforceOpenLimits(index: OpenedFtsIndex, limits: LanceFtsLimits): void {
  for (const partition of index.partitions) {
    if (partition.tokenSetBytes > limits.maxTokenSetBytes) {
      budget("FTS token set bytes", limits.maxTokenSetBytes, partition.tokenSetBytes);
    }
    if (partition.docLengthBytes > limits.maxDocLengthBytes) {
      budget("FTS document length bytes", limits.maxDocLengthBytes, partition.docLengthBytes);
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
  const partitions = await all(
    config.partitionIds.map((id) =>
      openPartition(context, config.directory, id, limits, config.sizes),
    ),
  );
  const numDocs = partitions.reduce((sum, p) => sum + p.numDocs, 0);
  const totalTokens = partitions.reduce((sum, p) => sum + p.totalTokens, 0);
  const info = describeIndex(metadata, column, tokenizer, undefined);
  const opened: OpenedFtsIndex = { info, tokenizer, partitions, numDocs, totalTokens };
  if (cacheKey !== undefined) {
    // Retained state: the FST bytes, the document-length column, and file directories.
    const bytes = partitions.reduce(
      (sum, p) => sum + p.tokenSetBytes + p.numTokens.byteLength + 8192,
      4096,
    );
    options.indexCache?.set(cacheKey, opened, bytes, { priority: 4 });
  }
  return opened;
}

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
  const postingItem = columnOfField(invert, childField(invert, posting.fieldId));
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
  // Token set: a single-row file holding the serialized FST.
  const fstColumn = resolveColumn(tokens, { name: "_token_fst_bytes" }).column;
  const fstPage = tokens.columns[fstColumn]?.pages[0];
  const fstBytes =
    fstPage?.encoding.kind === "binary" && fstPage.encoding.bytes.kind === "flat"
      ? (fstPage.bufferSizes[fstPage.encoding.bytes.bufferIndex] ?? 0)
      : 0;
  if (fstBytes > limits.maxTokenSetBytes) {
    budget("FTS token set bytes", limits.maxTokenSetBytes, fstBytes);
  }
  const numTokensColumn = resolveColumn(docs, { name: "_num_tokens" }).column;
  const docBytes = docs.rowCount * 4;
  if (docBytes > limits.maxDocLengthBytes) {
    budget("FTS document length bytes", limits.maxDocLengthBytes, docBytes);
  }
  context.accountDecodedMemory(fstBytes + docBytes);
  context.reserveDecodedRows(docs.rowCount);
  const [[fstItems], [numTokens]] = await all([
    readBinaryRanges(context, tokens, fstColumn, [{ start: 0, end: 1 }]),
    readU32Ranges(context, docs, numTokensColumn, [{ start: 0, end: docs.rowCount }]),
  ]);
  const fstData = (fstItems as Uint8Array[])[0];
  if (fstData === undefined) corrupt("Lance token set is empty");
  const fst = new FstMap(fstData.slice());
  let totalTokens = 0;
  for (const value of numTokens as Uint32Array) totalTokens += value;
  return {
    id,
    invert,
    docs,
    fst,
    tokenCount: fst.length,
    tokenSetBytes: fstBytes,
    docLengthBytes: docBytes,
    numDocs: docs.rowCount,
    numTokens: numTokens as Uint32Array,
    totalTokens,
    tailCodec: codec === "fixed32_v1" ? "fixed32" : "varint_delta",
    positions,
    maxScoreColumn: resolveColumn(invert, { name: "_max_score" }).column,
    lengthColumn: resolveColumn(invert, { name: "_length" }).column,
    postingColumn: posting.column,
    postingItemColumn: postingItem,
    rowIdColumn: resolveColumn(docs, { name: "_rowid" }).column,
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

// ----- query execution -------------------------------------------------------------

interface DocKey {
  partition: number;
  doc: number;
}

/** Map key packing a partition index and document id into one safe integer. */
function key(partition: number, doc: number): number {
  return partition * 2 ** 32 + doc;
}

function unkey(value: number): DocKey {
  const doc = value % 2 ** 32;
  return { partition: (value - doc) / 2 ** 32, doc };
}

interface TermPostings {
  partition: Partition;
  token: string;
  tokenId: number;
  /** Position of the term in the analyzed query (for phrase matching). */
  queryPosition: number;
  length: number;
  docIds: Uint32Array;
  freqs: Uint32Array;
}

class Counters {
  postingEntries = 0;
  candidates = 0;
  phraseCandidates = 0;
  tokensMatched = 0;
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
  const scores = await scoreQuery(options.context, index, options.query, limits, counters);
  // Ranking buffers every scored document: charge the row ceiling and memory before sorting.
  const bufferedRows = options.context.budget.maxBufferedRows;
  if (bufferedRows !== undefined && scores.size > bufferedRows) {
    budget("buffered rows", bufferedRows, scores.size);
  }
  options.context.accountDecodedMemory(scores.size * RANKED_ENTRY_BYTES);
  const ranked = [...scores.entries()].sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  const resolve = async (from: number, count: number): Promise<LanceFtsHit[]> => {
    const slice = ranked.slice(from, from + count);
    if (slice.length === 0) return [];
    const keys = slice.map(([k]) => unkey(k));
    const rowIds = await readRowIds(options.context, index, keys);
    return slice.map(([, score], i) => ({ rowId: rowIds[i] as bigint, score }));
  };
  const hits = await resolve(0, options.limit);
  return {
    index: index.info,
    hits,
    more: resolve,
    total: ranked.length,
    stats: {
      partitions: index.partitions.length,
      tokensMatched: counters.tokensMatched,
      postingEntriesDecoded: counters.postingEntries,
      candidatesScored: counters.candidates,
      phraseCandidatesChecked: counters.phraseCandidates,
    },
  };
}

/** Estimated retained bytes per scored document while accumulating and ranking. */
const SCORED_ENTRY_BYTES = 64;
const RANKED_ENTRY_BYTES = 48;
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

async function readRowIds(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  keys: readonly DocKey[],
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
      const values = await readU64Ranges(context, partition.docs, partition.rowIdColumn, ranges);
      for (const [i, { at }] of docs.entries()) out[at] = (values[i] as bigint[])[0] as bigint;
    }),
  );
  return out;
}

async function scoreQuery(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  query: LanceFtsQuery,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<Map<number, number>> {
  switch (query.kind) {
    case "boolean":
      return await scoreBoolean(context, index, query, limits, counters);
    case "phrase": {
      if (!index.tokenizer.withPosition || index.partitions.some((p) => p.positions === "none")) {
        unsupported("Phrase queries require an inverted index created with positions");
      }
      if (index.partitions.some((p) => p.positions === "shared_stream")) {
        unsupported("Lance shared position streams are not supported for phrase queries");
      }
      const tokens = analyze(query.text, index.tokenizer);
      return await scorePhrase(context, index, tokens, query.slop ?? 0, limits, counters);
    }
    case "match":
      return await scoreMatch(context, index, analyze(query.text, index.tokenizer), {
        operator: query.operator ?? "or",
        fuzziness: query.fuzziness ?? 0,
        prefixLength: query.prefixLength ?? 0,
        maxExpansions: query.maxExpansions ?? 50,
        limits,
        counters,
      });
  }
}

async function scoreBoolean(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  query: Extract<LanceFtsQuery, { kind: "boolean" }>,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<Map<number, number>> {
  const must = query.must ?? [];
  const should = query.should ?? [];
  const mustNot = query.mustNot ?? [];
  const run = (clauses: readonly LanceFtsQuery[]) =>
    all(clauses.map((clause) => scoreQuery(context, index, clause, limits, counters)));
  const [mustMaps, shouldMaps, notMaps] = await all([run(must), run(should), run(mustNot)]);
  const out = new Map<number, number>();
  const add = (map: Map<number, number>, only?: Set<number>): void => {
    for (const [doc, score] of map) {
      if (only !== undefined && !only.has(doc)) continue;
      out.set(doc, f32((out.get(doc) ?? 0) + score));
    }
  };
  if (mustMaps.length > 0) {
    let required: Set<number> | undefined;
    for (const map of mustMaps) {
      required =
        required === undefined
          ? new Set(map.keys())
          : new Set([...required].filter((doc) => map.has(doc)));
    }
    for (const map of mustMaps) add(map, required);
    for (const map of shouldMaps) add(map, required);
  } else {
    for (const map of shouldMaps) add(map);
  }
  for (const map of notMaps) for (const doc of map.keys()) out.delete(doc);
  return out;
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

interface MatchOptions {
  operator: "or" | "and";
  fuzziness: number;
  prefixLength: number;
  maxExpansions: number;
  limits: LanceFtsLimits;
  counters: Counters;
}

interface ResolvedTerm {
  partition: Partition;
  token: string;
  tokenId: number;
  queryPosition: number;
  /** Index of the originating query term (fuzzy expansions share one). */
  termIndex: number;
}

/**
 * Map analyzed query terms to dictionary entries, per partition. With `fuzziness > 0` every
 * term is expanded through a Levenshtein automaton; like Lance, `maxExpansions` caps the
 * expansions accumulated across all of a partition's terms, checked after each match is kept
 * (so every later term still contributes its first match).
 */
function resolveTerms(
  index: OpenedFtsIndex,
  tokens: readonly { text: string; position: number }[],
  fuzziness: number,
  prefixLength: number,
  maxExpansions: number,
  keepDuplicates: boolean,
  /** Receives, per partition, the query terms that matched before duplicate removal. */
  matchedTerms?: Map<Partition, Set<number>>,
): ResolvedTerm[] {
  const terms: ResolvedTerm[] = [];
  for (const partition of index.partitions) {
    const seen = new Set<number>();
    const matched = new Set<number>();
    matchedTerms?.set(partition, matched);
    let accumulated = 0;
    for (const [termIndex, token] of tokens.entries()) {
      const matches: { text: string; id: number }[] = [];
      if (fuzziness === 0) {
        const id = partition.fst.get(encoder.encode(token.text));
        if (id !== undefined) matches.push({ text: token.text, id });
      } else {
        const bytes = encoder.encode(token.text);
        const prefixEnd = Math.min(prefixLength, bytes.length);
        if (prefixEnd < bytes.length && ((bytes[prefixEnd] as number) & 0xc0) === 0x80) {
          validation(
            `FTS prefixLength ${prefixLength} splits a multi-byte character in "${token.text}"`,
          );
        }
        const prefix = bytes.subarray(0, prefixEnd);
        partition.fst.search(editDistanceAutomaton(token.text, fuzziness, prefix), (key, id) => {
          matches.push({ text: decoder.decode(key), id });
          accumulated += 1;
          return accumulated < maxExpansions;
        });
      }
      if (matches.length > 0) matched.add(termIndex);
      for (const match of matches) {
        if (!keepDuplicates) {
          if (seen.has(match.id)) continue;
          seen.add(match.id);
        }
        terms.push({
          partition,
          token: match.text,
          tokenId: match.id,
          queryPosition: token.position,
          termIndex,
        });
      }
    }
  }
  return terms;
}

async function loadPostings(
  context: LanceReadContext,
  terms: readonly ResolvedTerm[],
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<TermPostings[]> {
  if (terms.length === 0) return [];
  const byPartition = new Map<Partition, ResolvedTerm[]>();
  for (const term of terms) {
    const list = byPartition.get(term.partition) ?? [];
    list.push(term);
    byPartition.set(term.partition, list);
  }
  // Wave 1, all partitions: list bounds and posting lengths (independent reads).
  const plans = await all(
    [...byPartition].map(async ([partition, list]) => {
      const rows = list.map((term) => ({ start: term.tokenId, end: term.tokenId + 1 }));
      const [bounds, lengths] = await all([
        readListOffsets(context, partition.invert, partition.postingColumn, rows),
        readU32Ranges(context, partition.invert, partition.lengthColumn, rows),
      ]);
      return {
        partition,
        planned: list.map((term, i) => ({
          term,
          length: (lengths[i] as Uint32Array)[0] as number,
          blocks: {
            start: (bounds[i] as number[])[0] as number,
            end: (bounds[i] as number[])[1] as number,
          },
        })),
      };
    }),
  );
  // Check the aggregate plan before any payload read is issued.
  let entries = 0;
  for (const { planned } of plans) for (const item of planned) entries += item.length;
  counters.postingEntries += entries;
  if (counters.postingEntries > limits.maxPostingEntries) {
    budget("FTS posting entries", limits.maxPostingEntries, counters.postingEntries);
  }
  context.reserveDecodedRows(entries);
  context.accountDecodedMemory(entries * 8);
  // Waves 2-3: block offsets then block bytes, inside readBinaryRanges.
  const blocks = await all(
    plans.map(({ partition, planned }) =>
      readBinaryRanges(
        context,
        partition.invert,
        partition.postingItemColumn,
        planned.map((item) => item.blocks),
      ),
    ),
  );
  return plans.flatMap(({ partition, planned }, p) =>
    planned.map((item, i): TermPostings => {
      const decoded = decodePosting(
        (blocks[p] as Uint8Array[][])[i] as Uint8Array[],
        item.length,
        partition.tailCodec,
      );
      return {
        partition,
        token: item.term.token,
        tokenId: item.term.tokenId,
        queryPosition: item.term.queryPosition,
        length: item.length,
        docIds: decoded.docIds,
        freqs: decoded.freqs,
      };
    }),
  );
}

/** Decode a block-compressed posting list into parallel doc-id and frequency arrays. */
export function decodePosting(
  blocks: readonly Uint8Array[],
  length: number,
  tailCodec: "fixed32" | "varint_delta",
): { docIds: Uint32Array; freqs: Uint32Array } {
  const docIds = new Uint32Array(length);
  const freqs = new Uint32Array(length);
  const expectedBlocks = Math.ceil(length / BLOCK_LEN);
  if (blocks.length !== expectedBlocks) {
    corrupt("Lance posting list block count does not match its length", {
      blocks: blocks.length,
      length,
    });
  }
  const scratch = new Uint32Array(BLOCK_LEN);
  let written = 0;
  for (const [blockIndex, block] of blocks.entries()) {
    const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
    const full = (blockIndex + 1) * BLOCK_LEN <= length;
    if (full) {
      if (block.length < 10) corrupt("Lance posting block is truncated");
      const first = view.getUint32(4, true);
      const docBits = block[8] as number;
      let offset = 9;
      if (docBits > 32 || offset + packedBlockBytes(docBits) + 1 > block.length) {
        corrupt("Lance posting block is truncated");
      }
      unpackSortedBlock(block, offset, docBits, first, scratch);
      docIds.set(scratch, written);
      offset += packedBlockBytes(docBits);
      const freqBits = block[offset] as number;
      offset += 1;
      if (freqBits > 32 || offset + packedBlockBytes(freqBits) > block.length) {
        corrupt("Lance posting block is truncated");
      }
      unpackBlock(block, offset, freqBits, scratch);
      freqs.set(scratch, written);
      written += BLOCK_LEN;
      continue;
    }
    const count = length - written;
    let offset = 4;
    if (tailCodec === "fixed32") {
      if (block.length < 4 + count * 8) corrupt("Lance posting tail block is truncated");
      for (let i = 0; i < count; i += 1) {
        docIds[written + i] = view.getUint32(offset + i * 4, true);
        freqs[written + i] = view.getUint32(offset + (count + i) * 4, true);
      }
    } else {
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
        docIds[written + i] = previous;
      }
      for (let i = 0; i < count; i += 1) freqs[written + i] = readVarint();
    }
    written += count;
  }
  return { docIds, freqs };
}

// ----- match queries ---------------------------------------------------------------

/** Count one more scored document against the operation-wide candidate limit. */
function admitCandidate(counters: Counters, limits: LanceFtsLimits): void {
  counters.candidates += 1;
  if (counters.candidates > limits.maxCandidates) {
    budget("FTS candidates", limits.maxCandidates, counters.candidates);
  }
}

async function scoreMatch(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  allTokens: readonly { text: string; position: number }[],
  options: MatchOptions,
): Promise<Map<number, number>> {
  const scores = new Map<number, number>();
  const seenText = new Set<string>();
  // Lance scores each distinct query term once.
  const tokens = allTokens.filter((token) => !seenText.has(token.text) && seenText.add(token.text));
  if (tokens.length === 0) return scores;
  const matchedTerms = new Map<Partition, Set<number>>();
  let terms = resolveTerms(
    index,
    tokens,
    options.fuzziness,
    options.prefixLength,
    options.maxExpansions,
    false,
    matchedTerms,
  );
  if (options.operator === "and") {
    // A partition can only hold a document containing every term if each term resolved in it
    // (fuzzy expansions shared between terms count for both).
    const complete = new Set(
      index.partitions.filter((partition) => matchedTerms.get(partition)?.size === tokens.length),
    );
    terms = terms.filter((term) => complete.has(term.partition));
  }
  options.counters.tokensMatched += terms.length;
  const postings = await loadPostings(context, terms, options.limits, options.counters);
  // Corpus statistics are index-global, like Lance's `MemBM25Scorer`.
  const avgDocLength = f32(index.totalTokens / index.numDocs);
  const documentFrequency = new Map<string, number>();
  for (const posting of postings) {
    documentFrequency.set(
      posting.token,
      (documentFrequency.get(posting.token) ?? 0) + posting.length,
    );
  }
  const postingsPerPartition = new Map<Partition, number>();
  let entries = 0;
  for (const posting of postings) {
    postingsPerPartition.set(
      posting.partition,
      (postingsPerPartition.get(posting.partition) ?? 0) + 1,
    );
    entries += posting.length;
  }
  context.accountDecodedMemory(Math.min(entries, index.numDocs) * SCORED_ENTRY_BYTES);
  const partials = new Map<number, { score: number; count: number }>();
  for (const posting of postings) {
    const partitionIndex = index.partitions.indexOf(posting.partition);
    const queryWeight = idf(documentFrequency.get(posting.token) ?? 0, index.numDocs);
    for (let i = 0; i < posting.docIds.length; i += 1) {
      const doc = posting.docIds[i] as number;
      const k = key(partitionIndex, doc);
      let entry = partials.get(k);
      if (entry === undefined) {
        admitCandidate(options.counters, options.limits);
        entry = { score: 0, count: 0 };
        partials.set(k, entry);
      }
      const length = posting.partition.numTokens[doc];
      if (length === undefined) corrupt("Lance posting references a document outside the index");
      entry.score = f32(
        entry.score +
          f32(queryWeight * docWeight(posting.freqs[i] as number, length, avgDocLength)),
      );
      entry.count += 1;
    }
  }
  for (const [k, entry] of partials) {
    // AND needs the document in every posting list the partition resolved (every fuzzy
    // expansion included), as Lance's WAND does.
    const partition = index.partitions[unkey(k).partition] as Partition;
    if (options.operator === "and" && entry.count < (postingsPerPartition.get(partition) ?? 0)) {
      continue;
    }
    scores.set(k, entry.score);
  }
  return scores;
}

/** Document frequency of each distinct token, summed over every partition holding it. */
async function documentFrequencies(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  texts: readonly string[],
): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  await all(
    index.partitions.map(async (partition) => {
      const found = texts.flatMap((text) => {
        const id = partition.fst.get(encoder.encode(text));
        return id === undefined ? [] : [{ text, id }];
      });
      if (found.length === 0) return;
      const lengths = await readU32Ranges(
        context,
        partition.invert,
        partition.lengthColumn,
        found.map(({ id }) => ({ start: id, end: id + 1 })),
      );
      for (const [i, { text }] of found.entries()) {
        out.set(text, (out.get(text) ?? 0) + ((lengths[i] as Uint32Array)[0] as number));
      }
    }),
  );
  return out;
}

// ----- phrase queries --------------------------------------------------------------

async function scorePhrase(
  context: LanceReadContext,
  index: OpenedFtsIndex,
  tokens: readonly { text: string; position: number }[],
  slop: number,
  limits: LanceFtsLimits,
  counters: Counters,
): Promise<Map<number, number>> {
  const scores = new Map<number, number>();
  if (tokens.length === 0) return scores;
  const distinct = [...new Set(tokens.map((token) => token.text))];
  // A phrase needs every term in one partition. Document frequencies still count each
  // distinct term once over the whole index, including partitions that cannot hold the phrase.
  const perPartition: { partition: Partition; partitionIndex: number; ids: number[] }[] = [];
  for (const [partitionIndex, partition] of index.partitions.entries()) {
    const ids: number[] = [];
    for (const token of tokens) {
      const id = partition.fst.get(encoder.encode(token.text));
      if (id === undefined) break;
      ids.push(id);
    }
    if (ids.length === tokens.length) perPartition.push({ partition, partitionIndex, ids });
  }
  counters.tokensMatched += perPartition.reduce((sum, p) => sum + p.ids.length, 0);
  const loadTerms: ResolvedTerm[] = perPartition.flatMap(({ partition, ids }) => {
    const unique = new Map<number, number>();
    for (const [termIndex, id] of ids.entries()) if (!unique.has(id)) unique.set(id, termIndex);
    return [...unique].map(([tokenId, termIndex]) => ({
      partition,
      token: (tokens[termIndex] as { text: string }).text,
      tokenId,
      queryPosition: (tokens[termIndex] as { position: number }).position,
      termIndex,
    }));
  });
  const [postings, documentFrequency] = await all([
    loadPostings(context, loadTerms, limits, counters),
    documentFrequencies(context, index, distinct),
  ]);
  const avgDocLength = f32(index.totalTokens / index.numDocs);
  await all(
    perPartition.map(async ({ partition, partitionIndex, ids }) => {
      const byId = new Map(
        postings.filter((p) => p.partition === partition).map((p) => [p.tokenId, p]),
      );
      const lists = ids.map((id) => byId.get(id) as TermPostings);
      // Documents containing every phrase term, by merging the sorted lists.
      const candidates = intersectSorted(lists.map((list) => list.docIds));
      counters.phraseCandidates += candidates.length;
      if (counters.phraseCandidates > limits.maxPhraseCandidates) {
        budget("FTS phrase candidates", limits.maxPhraseCandidates, counters.phraseCandidates);
      }
      if (candidates.length === 0) return;
      const distinctLists = [...byId.values()];
      const positions = await readPositions(context, partition, distinctLists, candidates);
      const positionsOf = new Map(distinctLists.map((list, i) => [list.tokenId, i]));
      const queryPositions = tokens.map((token) => token.position);
      for (const [candidateIndex, doc] of candidates.entries()) {
        const perTerm = ids.map(
          (id) =>
            ((positions[candidateIndex] as number[][])[positionsOf.get(id) as number] ??
              []) as number[],
        );
        if (!phraseMatches(perTerm, queryPositions, slop)) continue;
        admitCandidate(counters, limits);
        let score = 0;
        for (const [termIndex, list] of lists.entries()) {
          const at = lowerBound(list.docIds, doc);
          const length = partition.numTokens[doc] as number;
          const text = (tokens[termIndex] as { text: string }).text;
          score = f32(
            score +
              f32(
                idf(documentFrequency.get(text) ?? 0, index.numDocs) *
                  docWeight(list.freqs[at] as number, length, avgDocLength),
              ),
          );
        }
        scores.set(key(partitionIndex, doc), score);
      }
    }),
  );
  return scores;
}

function lowerBound(values: Uint32Array, target: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const mid = (low + high) >>> 1;
    if ((values[mid] as number) < target) low = mid + 1;
    else high = mid;
  }
  return low;
}

function intersectSorted(lists: readonly Uint32Array[]): number[] {
  if (lists.length === 0) return [];
  const [first, ...rest] = lists as [Uint32Array, ...Uint32Array[]];
  const out: number[] = [];
  for (const doc of first) {
    if (
      rest.every((list) => {
        const at = lowerBound(list, doc);
        return at < list.length && list[at] === doc;
      })
    ) {
      if (out.at(-1) !== doc) out.push(doc);
    }
  }
  return out;
}

/** Positions per candidate, per phrase term, via the legacy `_position` list-of-lists layout. */
async function readPositions(
  context: LanceReadContext,
  partition: Partition,
  lists: readonly TermPostings[],
  candidates: readonly number[],
): Promise<number[][][]> {
  const column = partition.positionColumn as number;
  const outerItemColumn = partition.positionOuterItemColumn as number;
  const itemColumn = partition.positionItemColumn as number;
  // Wave 1: the per-document spans of each term's position lists.
  const termSpans = await readListOffsets(
    context,
    partition.invert,
    column,
    lists.map((list) => ({ start: list.tokenId, end: list.tokenId + 1 })),
  );
  // Wave 2: one contiguous read of the per-document block offsets of each term, covering the
  // span between its first and last candidate (8 bytes per posting entry at most).
  const spans = lists.map((list, t) => {
    const base = (termSpans[t] as number[])[0] as number;
    const first = lowerBound(list.docIds, candidates[0] as number);
    const last = lowerBound(list.docIds, candidates.at(-1) as number);
    return { base, first, last };
  });
  const docOffsets = await readListOffsets(
    context,
    partition.invert,
    outerItemColumn,
    spans.map((span) => ({ start: span.base + span.first, end: span.base + span.last + 1 })),
  );
  const blockRanges = lists.flatMap((list, t) => {
    const span = spans[t] as { base: number; first: number; last: number };
    const offsets = docOffsets[t] as number[];
    return candidates.map((doc) => {
      const at = lowerBound(list.docIds, doc) - span.first;
      return { start: offsets[at] as number, end: offsets[at + 1] as number };
    });
  });
  // Waves 3-4: block offsets and bytes; nearby blocks coalesce per the planning options.
  const blocks = await readBinaryRanges(context, partition.invert, itemColumn, blockRanges);
  const out: number[][][] = candidates.map(() => []);
  for (let t = 0; t < lists.length; t += 1) {
    for (let c = 0; c < candidates.length; c += 1) {
      const decoded = decodePositions(blocks[t * candidates.length + c] as Uint8Array[]);
      context.accountDecodedMemory(decoded.length * 8);
      (out[c] as number[][])[t] = decoded;
    }
  }
  return out;
}

export function decodePositions(blocks: readonly Uint8Array[]): number[] {
  const header = blocks[0];
  if (header === undefined || header.length < 4) corrupt("Lance position list header is missing");
  const total = new DataView(header.buffer, header.byteOffset, header.byteLength).getUint32(
    0,
    true,
  );
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
      const first = view.getUint32(0, true);
      unpackSortedBlock(block, 5, block[4] as number, first, scratch);
      for (let i = 0; i < BLOCK_LEN; i += 1) out.push(scratch[i] as number);
    } else {
      const count = total - out.length;
      if (block.length < count * 4) corrupt("Lance position block is truncated");
      for (let i = 0; i < count; i += 1) out.push(view.getUint32(i * 4, true));
    }
  }
  if (out.length !== total) corrupt("Lance position list length does not match its header");
  return out;
}

/**
 * Phrase matching over per-term position lists, ported from Lance's position cursors.
 *
 * Positions keep the gaps left by removed stop words. Exact phrases (`slop` 0) anchor the
 * shortest list's positions at a non-negative offset and gallop the other terms to
 * `base + position-in-query`; with slop, cursors sorted by query position advance to a common
 * relative position until each step is within `slop` of the previous. Cursor moves are binary
 * searches, so a document costs O(matches log positions), not O(positions^2).
 */
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
