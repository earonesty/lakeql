import {
  type CacheAdapter,
  LakeqlError,
  type ObjectHead,
  type ObjectStore,
  type QueryBudget,
  type Row,
} from "lakeql";
import { readDeletedRowOffsets } from "./deletions.js";
import { materializeFragmentRows } from "./file.js";
import {
  type LanceRangePlanningOptions,
  LanceReadContext,
  type MutableLanceReadStats,
} from "./io.js";
import {
  type LanceFragment,
  type LanceManifest,
  parseManifest,
  parseRowIdSequence,
} from "./proto.js";
import { resolveRequestedRowIds } from "./rowids.js";
import {
  type LanceScalarIndexInfo,
  type LanceScalarRange,
  type LanceScalarValue,
  loadScalarIndexes,
  lookupScalarRowIds,
  rangeScalarRowIds,
} from "./scalar.js";
import {
  type LanceVectorIndexInfo,
  type LanceVectorLimits,
  type LanceVectorMetric,
  loadVectorIndexes,
  searchVectorIndex,
} from "./vector.js";

export const PACKAGE = "lakeql-lance" as const;
export const SUPPORTED_LANCE_STORAGE_VERSION = "2.0" as const;
export const SUPPORTED_LANCE_PRODUCER = "pylance 8.0.0" as const;

const MAX_U64 = (1n << 64n) - 1n;
const MANIFEST_FOOTER_BYTES = 16;
const MANIFEST_TAIL_BYTES = 64 * 1024;
const KNOWN_READER_FLAGS = 1n | 2n | 4n | 8n | 16n;
const STABLE_ROW_IDS_FLAG = 2n;
const BASE_PATHS_FLAG = 16n;

export type LanceRowIdInput = bigint | number | string;

export interface OpenLanceDatasetOptions {
  store: ObjectStore;
  path: string;
  budget?: QueryBudget;
  /** Pin a manifest version. Omit to resolve the latest-version hint. */
  version?: LanceRowIdInput;
  metadataCache?: CacheAdapter<Uint8Array>;
  coalesceGapBytes?: number;
  maxCoalescedRangeBytes?: number;
  now?: () => number;
  vectorLimits?: Partial<LanceVectorLimits>;
}

export interface TakeLanceRowsOptions {
  /**
   * Snapshot identity stored alongside the external row IDs. This is required
   * so IDs from another immutable version cannot silently address other rows.
   */
  snapshotId: string;
  rowIds: readonly LanceRowIdInput[];
  select: readonly string[];
  onMissing?: "error" | "null";
}

export interface LanceTakeRowsResult {
  rows: (Row | null)[];
  missingRowIds: string[];
  deletedRowIds: string[];
  stats: LanceReadStats;
}

export interface LookupLanceRowsOptions {
  snapshotId: string;
  index: string;
  values: readonly LanceScalarValue[];
  select: readonly string[];
}

export interface LanceScalarLookupGroup {
  value: LanceScalarValue;
  rowIds: string[];
  rows: Row[];
}

export interface LanceScalarLookupResult {
  index: LanceScalarIndexInfo;
  groups: LanceScalarLookupGroup[];
  stats: LanceReadStats;
}

export interface RangeLanceRowsOptions {
  snapshotId: string;
  index: string;
  range: LanceScalarRange;
  select: readonly string[];
}

export interface LanceScalarRangeResult {
  index: LanceScalarIndexInfo;
  rowIds: string[];
  rows: Row[];
  stats: LanceReadStats;
}

export interface NearestLanceRowsOptions {
  snapshotId: string;
  index: string;
  vector: readonly number[];
  k: number;
  nprobes: number;
  select: readonly string[];
}

export interface LanceVectorMatch {
  rowId: string;
  distance: number;
  row: Row;
}

export interface LanceVectorSearchResult {
  index: LanceVectorIndexInfo;
  metric: LanceVectorMetric;
  matches: LanceVectorMatch[];
  partitionsSearched: number[];
  candidatesScored: number;
  stats: LanceReadStats;
}

export type {
  LanceScalarIndexInfo,
  LanceScalarRange,
  LanceScalarValue,
  LanceVectorIndexInfo,
  LanceVectorLimits,
  LanceVectorMetric,
};

export interface LanceReadStats {
  snapshotId: string;
  snapshotVersion: string;
  snapshotMetadataBytes: number;
  dataMetadataBytes: number;
  metadataMs: number;
  logicalBytesRequested: number;
  physicalBytesRequested: number;
  rangeRequests: number;
  fragmentsTouched: number;
  pagesTouched: number;
  rowsRequested: number;
  rowsDecoded: number;
  rowsMaterialized: number;
  selectedColumns: string[];
  cacheHits: number;
  cacheMisses: number;
  peakMemoryBytes: number;
  totalElapsedMs: number;
}

export class LanceDataset {
  readonly snapshotId: string;
  readonly version: string;
  readonly storageVersion = SUPPORTED_LANCE_STORAGE_VERSION;

  constructor(
    private readonly options: {
      store: ObjectStore;
      root: string;
      manifest: LanceManifest;
      manifestPath: string;
      manifestFileSize: number;
      snapshotId: string;
      budget: QueryBudget;
      now: () => number;
      planning: LanceRangePlanningOptions;
      openStats: MutableLanceReadStats;
      metadataMs: number;
      vectorLimits: LanceVectorLimits;
    },
  ) {
    this.snapshotId = options.snapshotId;
    this.version = options.manifest.version.toString();
  }

  async takeRows(options: TakeLanceRowsOptions): Promise<LanceTakeRowsResult> {
    const stats = cloneStats(this.options.openStats);
    const startedAt = this.options.now();
    const context = this.context(stats, startedAt);
    try {
      return await this.takeRowsWithStats(options, stats, context, startedAt);
    } finally {
      context.releaseDecodedMemory();
    }
  }

  async scalarIndexes(): Promise<LanceScalarIndexInfo[]> {
    const stats = cloneStats(this.options.openStats);
    const context = this.context(stats, this.options.now());
    try {
      return (
        await loadScalarIndexes({
          context,
          manifestPath: this.options.manifestPath,
          manifestFileSize: this.options.manifestFileSize,
          manifest: this.options.manifest,
        })
      ).map(({ info }) => info);
    } finally {
      context.releaseDecodedMemory();
    }
  }

  async lookupRows(options: LookupLanceRowsOptions): Promise<LanceScalarLookupResult> {
    if (options.snapshotId !== this.snapshotId) {
      snapshotMismatch(this.snapshotId, options.snapshotId, this.version);
    }
    const stats = cloneStats(this.options.openStats);
    const startedAt = this.options.now();
    const context = this.context(stats, startedAt);
    try {
      const lookup = await lookupScalarRowIds({
        context,
        root: this.options.root,
        manifestPath: this.options.manifestPath,
        manifestFileSize: this.options.manifestFileSize,
        manifest: this.options.manifest,
        indexName: options.index,
        values: options.values,
        budget: this.options.budget,
      });
      const rowIds = lookup.matches.flatMap((match) => match.rowIds);
      const materialized = await this.takeRowsWithStats(
        {
          snapshotId: this.snapshotId,
          rowIds,
          select: options.select,
          onMissing: "null",
        },
        stats,
        context,
        startedAt,
      );
      let offset = 0;
      const groups = lookup.matches.map((match) => {
        const rows = materialized.rows.slice(offset, offset + match.rowIds.length);
        offset += match.rowIds.length;
        const retained = rows.flatMap((row, index) =>
          row === null
            ? []
            : [
                {
                  row,
                  rowId: (match.rowIds[index] as bigint).toString(),
                },
              ],
        );
        return {
          value: match.value,
          rowIds: retained.map(({ rowId }) => rowId),
          rows: retained.map(({ row }) => row),
        };
      });
      return { index: lookup.index, groups, stats: materialized.stats };
    } finally {
      context.releaseDecodedMemory();
    }
  }

  async rangeRows(options: RangeLanceRowsOptions): Promise<LanceScalarRangeResult> {
    if (options.snapshotId !== this.snapshotId) {
      snapshotMismatch(this.snapshotId, options.snapshotId, this.version);
    }
    const stats = cloneStats(this.options.openStats);
    const startedAt = this.options.now();
    const context = this.context(stats, startedAt);
    try {
      const lookup = await rangeScalarRowIds({
        context,
        root: this.options.root,
        manifestPath: this.options.manifestPath,
        manifestFileSize: this.options.manifestFileSize,
        manifest: this.options.manifest,
        indexName: options.index,
        range: options.range,
        budget: this.options.budget,
      });
      const materialized = await this.takeRowsWithStats(
        {
          snapshotId: this.snapshotId,
          rowIds: lookup.rowIds,
          select: options.select,
          onMissing: "null",
        },
        stats,
        context,
        startedAt,
      );
      const retained = materialized.rows.flatMap((row, index) =>
        row === null
          ? []
          : [
              {
                row,
                rowId: (lookup.rowIds[index] as bigint).toString(),
              },
            ],
      );
      return {
        index: lookup.index,
        rowIds: retained.map(({ rowId }) => rowId),
        rows: retained.map(({ row }) => row),
        stats: materialized.stats,
      };
    } finally {
      context.releaseDecodedMemory();
    }
  }

  async vectorIndexes(): Promise<LanceVectorIndexInfo[]> {
    const stats = cloneStats(this.options.openStats);
    const context = this.context(stats, this.options.now());
    try {
      return await loadVectorIndexes({
        context,
        root: this.options.root,
        manifestPath: this.options.manifestPath,
        manifestFileSize: this.options.manifestFileSize,
        manifest: this.options.manifest,
        limits: this.options.vectorLimits,
      });
    } finally {
      context.releaseDecodedMemory();
    }
  }

  async nearest(options: NearestLanceRowsOptions): Promise<LanceVectorSearchResult> {
    if (options.snapshotId !== this.snapshotId) {
      snapshotMismatch(this.snapshotId, options.snapshotId, this.version);
    }
    const stats = cloneStats(this.options.openStats);
    const startedAt = this.options.now();
    const context = this.context(stats, startedAt);
    try {
      const search = await searchVectorIndex({
        context,
        root: this.options.root,
        manifestPath: this.options.manifestPath,
        manifestFileSize: this.options.manifestFileSize,
        manifest: this.options.manifest,
        indexName: options.index,
        vector: options.vector,
        k: options.k,
        nprobes: options.nprobes,
        budget: this.options.budget,
        limits: this.options.vectorLimits,
      });
      const materialized = await this.takeRowsWithStats(
        {
          snapshotId: this.snapshotId,
          rowIds: search.candidates.map(({ rowId }) => rowId),
          select: options.select,
          onMissing: "null",
        },
        stats,
        context,
        startedAt,
      );
      const matches = materialized.rows.flatMap((row, index) => {
        const candidate = search.candidates[index];
        return row === null || candidate === undefined
          ? []
          : [
              {
                rowId: candidate.rowId.toString(),
                distance: candidate.distance,
                row,
              },
            ];
      });
      return {
        index: search.index,
        metric: search.index.metric,
        matches,
        partitionsSearched: search.partitions,
        candidatesScored: search.candidatesScored,
        stats: materialized.stats,
      };
    } finally {
      context.releaseDecodedMemory();
    }
  }

  private async takeRowsWithStats(
    options: TakeLanceRowsOptions,
    stats: MutableLanceReadStats,
    context: LanceReadContext,
    startedAt: number,
  ): Promise<LanceTakeRowsResult> {
    if (options.snapshotId !== this.snapshotId) {
      snapshotMismatch(this.snapshotId, options.snapshotId, this.version);
    }
    const rowIds = options.rowIds.map(normalizeRowId);
    const select = validatedProjection(options.select);
    enforceRowShapeBudget(this.options.budget, rowIds);
    context.check();
    const uniqueRowIds = [...new Map(rowIds.map((rowId) => [rowId.toString(), rowId])).values()];
    context.reserveDecodedRows(uniqueRowIds.length);
    const addresses = new Map<string, { fragmentIndex: number; rowOffset: number }>();
    let unresolved = uniqueRowIds;
    for (const [fragmentIndex, fragment] of this.options.manifest.fragments.entries()) {
      if (unresolved.length === 0) break;
      const resolved = resolveRequestedRowIds(unresolved, [
        {
          physicalRows: fragment.physicalRows,
          segments: parseRowIdSequence(
            await readFragmentRowIds(context, this.options.root, fragment),
          ),
        },
      ]);
      for (const [rowId, address] of resolved) {
        addresses.set(rowId, { fragmentIndex, rowOffset: address.rowOffset });
      }
      unresolved = unresolved.filter((rowId) => !resolved.has(rowId.toString()));
    }
    const candidateOffsets = new Map<number, Set<number>>();
    for (const address of addresses.values()) {
      const offsets = candidateOffsets.get(address.fragmentIndex) ?? new Set<number>();
      offsets.add(address.rowOffset);
      candidateOffsets.set(address.fragmentIndex, offsets);
    }
    const deletedOffsetsByFragment = new Map<number, Set<number>>(
      await Promise.all(
        [...candidateOffsets].map(async ([fragmentIndex, offsets]) => {
          const fragment = this.options.manifest.fragments[fragmentIndex];
          if (fragment === undefined) corrupt("Resolved Lance row references a missing fragment");
          return [
            fragmentIndex,
            await readDeletedRowOffsets(context, this.options.root, fragment, offsets),
          ] as const;
        }),
      ),
    );
    const deleted = new Set<string>();
    for (const [rowId, address] of addresses) {
      if (deletedOffsetsByFragment.get(address.fragmentIndex)?.has(address.rowOffset)) {
        deleted.add(rowId);
        addresses.delete(rowId);
      }
    }
    const deletedRowIds = uniqueRowIds.filter((rowId) => deleted.has(rowId.toString())).map(String);
    const missingRowIds = uniqueRowIds
      .filter((rowId) => !addresses.has(rowId.toString()))
      .map(String);
    if (missingRowIds.length > 0 && (options.onMissing ?? "error") === "error") {
      throw new LakeqlError(
        "LAKEQL_OBJECT_NOT_FOUND",
        "One or more stable Lance row IDs are absent from the snapshot",
        {
          snapshotId: this.snapshotId,
          missingRowIds,
          deletedRowIds,
        },
      );
    }
    const fragmentOffsets = new Map<number, Set<number>>();
    for (const address of addresses.values()) {
      const offsets = fragmentOffsets.get(address.fragmentIndex) ?? new Set<number>();
      offsets.add(address.rowOffset);
      fragmentOffsets.set(address.fragmentIndex, offsets);
    }
    const materialized = new Map<string, Row>();
    await Promise.all(
      [...fragmentOffsets].map(async ([fragmentIndex, offsets]) => {
        const fragment = this.options.manifest.fragments[fragmentIndex];
        if (fragment === undefined) corrupt("Resolved Lance row references a missing fragment");
        const rows = await materializeFragmentRows({
          context,
          root: this.options.root,
          fragment,
          fragmentIndex,
          fields: this.options.manifest.fields,
          select,
          rowOffsets: [...offsets],
        });
        for (const [rowIdText, address] of addresses) {
          if (address.fragmentIndex !== fragmentIndex) continue;
          const row = rows.get(address.rowOffset);
          if (row === undefined) {
            corrupt("Lance row address did not materialize", {
              rowId: rowIdText,
              fragmentIndex,
              rowOffset: address.rowOffset,
            });
          }
          materialized.set(rowIdText, row);
        }
      }),
    );
    const rows = rowIds.map((rowId) => materialized.get(rowId.toString()) ?? null);
    context.check();
    return {
      rows,
      missingRowIds,
      deletedRowIds,
      stats: finalizeStats({
        mutable: stats,
        snapshotId: this.snapshotId,
        snapshotVersion: this.version,
        metadataMs: this.options.metadataMs,
        rowsRequested: rowIds.length,
        rowsMaterialized: rows.filter((row) => row !== null).length,
        selectedColumns: select,
        totalElapsedMs: this.options.now() - startedAt,
      }),
    };
  }

  private context(stats: MutableLanceReadStats, startedAt: number): LanceReadContext {
    return new LanceReadContext(
      this.options.store,
      this.options.budget,
      stats,
      startedAt,
      this.options.now,
      this.options.planning,
    );
  }
}

export async function openLanceDataset(options: OpenLanceDatasetOptions): Promise<LanceDataset> {
  const root = normalizeDatasetPath(options.path);
  const budget = options.budget ?? {};
  const now = options.now ?? performance.now.bind(performance);
  const startedAt = now();
  const stats = emptyStats();
  const planning = validatedPlanning(options);
  const vectorLimits = validatedVectorLimits(options.vectorLimits);
  const context = new LanceReadContext(options.store, budget, stats, startedAt, now, planning);
  const version =
    options.version === undefined
      ? await readLatestVersion(context, root)
      : normalizeRowId(options.version);
  if (version === 0n) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Lance dataset version must be positive");
  }
  const manifestPath = manifestPathForVersion(root, version);
  const manifestHead = await requiredHead(context, manifestPath);
  const cacheKey = manifestCacheKey(manifestPath, manifestHead);
  let manifestBytes: Uint8Array;
  const cached = await options.metadataCache?.get(cacheKey);
  if (cached === undefined) {
    stats.cacheMisses += options.metadataCache === undefined ? 0 : 1;
    manifestBytes = await readManifestBytes(context, manifestPath, manifestHead.size);
    await options.metadataCache?.set(cacheKey, { value: copyBytes(manifestBytes) });
  } else {
    stats.cacheHits += 1;
    manifestBytes = cached.value;
  }
  context.accountDecodedMemory(manifestBytes.byteLength);
  const manifest = parseManifest(manifestBytes);
  validateManifest(manifest, version);
  const digest = await sha256(manifestBytes);
  const snapshotId = `lance:${manifest.version}:sha256:${digest}`;
  context.check();
  const dataset = new LanceDataset({
    store: options.store,
    root,
    manifest,
    manifestPath,
    manifestFileSize: manifestHead.size,
    snapshotId,
    budget,
    now,
    planning,
    openStats: stats,
    metadataMs: now() - startedAt,
    vectorLimits,
  });
  context.releaseDecodedMemory();
  return dataset;
}

function validatedVectorLimits(limits: Partial<LanceVectorLimits> | undefined): LanceVectorLimits {
  const resolved = {
    maxDimension: limits?.maxDimension ?? 4_096,
    maxPartitionsSearched: limits?.maxPartitionsSearched ?? 64,
    maxCandidatesScored: limits?.maxCandidatesScored ?? 100_000,
  };
  for (const [name, value] of Object.entries(resolved)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Lance vector limit", {
        limit: name,
        value,
      });
    }
  }
  return resolved;
}

async function readLatestVersion(context: LanceReadContext, root: string): Promise<bigint> {
  const path = joinObjectPath(root, "_versions", "latest_version_hint.json");
  const head = await requiredHead(context, path);
  if (head.size <= 0 || head.size > 1024) {
    corrupt("Invalid Lance latest-version hint size", { path, size: head.size });
  }
  const lease = await context.readRange(path, { offset: 0, length: head.size }, "snapshot");
  try {
    const text = decodeUtf8(lease.slice({ offset: 0, length: head.size }));
    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch (cause) {
      corrupt("Invalid Lance latest-version hint JSON", {
        path,
        cause: cause instanceof Error ? cause.message : String(cause),
      });
    }
    if (
      typeof value !== "object" ||
      value === null ||
      !("version" in value) ||
      !Number.isSafeInteger((value as { version?: unknown }).version) ||
      ((value as { version: number }).version ?? 0) <= 0
    ) {
      corrupt("Invalid Lance latest-version hint", { path });
    }
    return BigInt((value as { version: number }).version);
  } finally {
    lease.release();
  }
}

async function readManifestBytes(
  context: LanceReadContext,
  path: string,
  fileSize: number,
): Promise<Uint8Array> {
  if (fileSize < MANIFEST_FOOTER_BYTES + 4) {
    corrupt("Lance manifest is smaller than its footer", { path, fileSize });
  }
  const tailOffset = Math.max(0, fileSize - MANIFEST_TAIL_BYTES);
  const tailRange = { offset: tailOffset, length: fileSize - tailOffset };
  const tailLease = await context.readRange(path, tailRange, "snapshot");
  let combined: Uint8Array;
  try {
    const tail = tailLease.slice(tailRange);
    const footer = tail.subarray(tail.byteLength - MANIFEST_FOOTER_BYTES);
    if (decodeAscii(footer.subarray(12, 16)) !== "LANC") {
      corrupt("Invalid Lance manifest magic", { path });
    }
    const manifestOffset = safeU64(dataView(footer).getBigUint64(0, true), "manifest offset");
    if (manifestOffset > fileSize - MANIFEST_FOOTER_BYTES - 4) {
      corrupt("Lance manifest offset is out of bounds", { path, manifestOffset, fileSize });
    }
    if (manifestOffset >= tailOffset) {
      combined = copyBytes(tail.subarray(manifestOffset - tailOffset));
    } else {
      const prefixRange = { offset: manifestOffset, length: tailOffset - manifestOffset };
      const prefixLease = await context.readRange(path, prefixRange, "snapshot");
      try {
        const prefix = prefixLease.slice(prefixRange);
        combined = new Uint8Array(prefix.byteLength + tail.byteLength);
        combined.set(prefix);
        combined.set(tail, prefix.byteLength);
      } finally {
        prefixLease.release();
      }
    }
  } finally {
    tailLease.release();
  }
  const view = dataView(combined);
  const messageLength = view.getUint32(0, true);
  if (messageLength + 4 + MANIFEST_FOOTER_BYTES !== combined.byteLength) {
    corrupt("Lance manifest recorded length is inconsistent", {
      path,
      messageLength,
      availableBytes: combined.byteLength,
    });
  }
  return copyBytes(combined.subarray(4, 4 + messageLength));
}

async function readFragmentRowIds(
  context: LanceReadContext,
  root: string,
  fragment: LanceFragment,
): Promise<Uint8Array> {
  if (fragment.inlineRowIds !== undefined) return fragment.inlineRowIds;
  if (fragment.externalRowIds === undefined) {
    corrupt("Stable-row-ID Lance fragment has no row-id sequence", {
      fragmentId: fragment.id.toString(),
    });
  }
  const external = fragment.externalRowIds;
  const path = joinObjectPath(root, external.path);
  const head = await requiredHead(context, path);
  if (external.offset + external.size > head.size) {
    corrupt("External Lance row-id sequence is out of bounds", {
      path,
      offset: external.offset,
      size: external.size,
      fileSize: head.size,
    });
  }
  const range = { offset: external.offset, length: external.size };
  const lease = await context.readRange(path, range, "snapshot");
  try {
    context.accountDecodedMemory(range.length);
    return copyBytes(lease.slice(range));
  } finally {
    lease.release();
  }
}

function validateManifest(manifest: LanceManifest, requestedVersion: bigint): void {
  if (manifest.version !== requestedVersion) {
    corrupt("Lance manifest version does not match its immutable path", {
      requestedVersion: requestedVersion.toString(),
      manifestVersion: manifest.version.toString(),
    });
  }
  if ((manifest.readerFeatureFlags & ~KNOWN_READER_FLAGS) !== 0n) {
    unsupported("Lance manifest requires unknown reader feature flags", {
      readerFeatureFlags: manifest.readerFeatureFlags.toString(),
    });
  }
  if ((manifest.readerFeatureFlags & STABLE_ROW_IDS_FLAG) === 0n) {
    unsupported("Lance dataset does not enable stable row IDs");
  }
  if ((manifest.readerFeatureFlags & BASE_PATHS_FLAG) !== 0n) {
    unsupported("Lance datasets with external base paths are not supported");
  }
  if (manifest.dataFileFormat !== "lance") {
    unsupported("Unsupported Lance dataset data-file format", {
      dataFileFormat: manifest.dataFileFormat,
    });
  }
  if (manifest.dataStorageVersion !== SUPPORTED_LANCE_STORAGE_VERSION) {
    unsupported("Unsupported Lance data storage version", {
      supported: SUPPORTED_LANCE_STORAGE_VERSION,
      actual: manifest.dataStorageVersion,
    });
  }
  if (manifest.fields.length === 0) corrupt("Lance manifest has no fields");
  const names = new Set<string>();
  const ids = new Set<number>();
  for (const field of manifest.fields) {
    if (field.name === "" || field.logicalType === "") corrupt("Lance field is incomplete");
    if (names.has(field.name) || ids.has(field.id)) {
      corrupt("Lance manifest has duplicate field identity", {
        name: field.name,
        id: field.id,
      });
    }
    names.add(field.name);
    ids.add(field.id);
  }
  for (const fragment of manifest.fragments) {
    if (!Number.isSafeInteger(fragment.physicalRows) || fragment.physicalRows < 0) {
      corrupt("Invalid Lance fragment physical row count", {
        fragmentId: fragment.id.toString(),
        physicalRows: fragment.physicalRows,
      });
    }
  }
}

function validatedProjection(select: readonly string[]): string[] {
  if (select.length === 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Lance projection must not be empty");
  }
  const values: string[] = [];
  const seen = new Set<string>();
  for (const value of select) {
    const name = value.trim();
    if (name === "" || seen.has(name)) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Lance projection", {
        column: value,
        duplicate: seen.has(name),
      });
    }
    seen.add(name);
    values.push(name);
  }
  return values;
}

function enforceRowShapeBudget(budget: QueryBudget, rowIds: readonly bigint[]): void {
  if (budget.maxOutputRows !== undefined && rowIds.length > budget.maxOutputRows) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Lance takeRows exceeded output row budget (${rowIds.length} > ${budget.maxOutputRows})`,
      { metric: "output rows", limit: budget.maxOutputRows, actual: rowIds.length },
    );
  }
}

function validatedPlanning(options: OpenLanceDatasetOptions): LanceRangePlanningOptions {
  const coalesceGapBytes = options.coalesceGapBytes ?? 1024;
  const maxCoalescedRangeBytes = options.maxCoalescedRangeBytes ?? 256 * 1024;
  if (
    !Number.isSafeInteger(coalesceGapBytes) ||
    coalesceGapBytes < 0 ||
    !Number.isSafeInteger(maxCoalescedRangeBytes) ||
    maxCoalescedRangeBytes <= 0
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Lance range planning options", {
      coalesceGapBytes,
      maxCoalescedRangeBytes,
    });
  }
  return { coalesceGapBytes, maxCoalescedRangeBytes };
}

function normalizeRowId(value: LanceRowIdInput): bigint {
  let normalized: bigint;
  if (typeof value === "bigint") normalized = value;
  else if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Numeric Lance row IDs must be safe integers; use bigint or a decimal string",
        { rowId: value },
      );
    }
    normalized = BigInt(value);
  } else {
    if (!/^(?:0|[1-9][0-9]*)$/u.test(value)) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid decimal Lance row ID", {
        rowId: value,
      });
    }
    normalized = BigInt(value);
  }
  if (normalized < 0n || normalized > MAX_U64) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Lance row ID is outside uint64", {
      rowId: normalized.toString(),
    });
  }
  return normalized;
}

function manifestPathForVersion(root: string, version: bigint): string {
  const inverted = MAX_U64 - version;
  return joinObjectPath(root, "_versions", `${inverted.toString().padStart(20, "0")}.manifest`);
}

function manifestCacheKey(path: string, head: ObjectHead): string {
  return `lance:manifest:${path}:${head.size}:${head.etag ?? ""}`;
}

async function requiredHead(context: LanceReadContext, path: string): Promise<ObjectHead> {
  context.check();
  const head = await context.store.head(path);
  context.check();
  if (head === null) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `Missing Lance object ${path}`, { path });
  }
  return head;
}

function normalizeDatasetPath(path: string): string {
  const normalized = path.replace(/^\/+|\/+$/gu, "");
  if (normalized === "" || normalized.split("/").some((part) => part === "." || part === "..")) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Lance dataset path", { path });
  }
  return normalized;
}

function joinObjectPath(...parts: string[]): string {
  return parts
    .flatMap((part) => part.split("/"))
    .filter((part) => part.length > 0)
    .join("/");
}

async function sha256(bytes: Uint8Array): Promise<string> {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", copy));
  return [...digest].map((value) => value.toString(16).padStart(2, "0")).join("");
}

function emptyStats(): MutableLanceReadStats {
  return {
    snapshotMetadataBytes: 0,
    dataMetadataBytes: 0,
    logicalBytesRequested: 0,
    physicalBytesRequested: 0,
    rangeRequests: 0,
    cacheHits: 0,
    cacheMisses: 0,
    peakMemoryBytes: 0,
    rowsDecoded: 0,
    fragments: new Set(),
    pages: new Set(),
  };
}

function cloneStats(stats: MutableLanceReadStats): MutableLanceReadStats {
  return {
    ...stats,
    fragments: new Set(stats.fragments),
    pages: new Set(stats.pages),
  };
}

function finalizeStats(options: {
  mutable: MutableLanceReadStats;
  snapshotId: string;
  snapshotVersion: string;
  metadataMs: number;
  rowsRequested: number;
  rowsMaterialized: number;
  selectedColumns: string[];
  totalElapsedMs: number;
}): LanceReadStats {
  return {
    snapshotId: options.snapshotId,
    snapshotVersion: options.snapshotVersion,
    snapshotMetadataBytes: options.mutable.snapshotMetadataBytes,
    dataMetadataBytes: options.mutable.dataMetadataBytes,
    metadataMs: options.metadataMs,
    logicalBytesRequested: options.mutable.logicalBytesRequested,
    physicalBytesRequested: options.mutable.physicalBytesRequested,
    rangeRequests: options.mutable.rangeRequests,
    fragmentsTouched: options.mutable.fragments.size,
    pagesTouched: options.mutable.pages.size,
    rowsRequested: options.rowsRequested,
    rowsDecoded: options.mutable.rowsDecoded,
    rowsMaterialized: options.rowsMaterialized,
    selectedColumns: options.selectedColumns,
    cacheHits: options.mutable.cacheHits,
    cacheMisses: options.mutable.cacheMisses,
    peakMemoryBytes: options.mutable.peakMemoryBytes,
    totalElapsedMs: options.totalElapsedMs,
  };
}

function safeU64(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    corrupt(`Lance ${label} exceeds JavaScript's safe integer range`, {
      value: value.toString(),
    });
  }
  return Number(value);
}

function dataView(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function decodeAscii(bytes: Uint8Array): string {
  return String.fromCharCode(...bytes);
}

function decodeUtf8(bytes: Uint8Array): string {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (cause) {
    corrupt("Invalid UTF-8 in Lance metadata", {
      cause: cause instanceof Error ? cause.message : String(cause),
    });
  }
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

function snapshotMismatch(expected: string, actual: string, version: string): never {
  throw new LakeqlError(
    "LAKEQL_LANCE_SNAPSHOT_MISMATCH",
    "Lance row IDs belong to a different immutable snapshot",
    {
      expectedSnapshotId: expected,
      actualSnapshotId: actual,
      snapshotVersion: version,
    },
  );
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}
