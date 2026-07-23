import { LakeqlError, type QueryBudget } from "lakeql";
import {
  inspectLanceFile,
  materializeInspectedLanceFileRows,
  readInspectedGlobalBuffer,
} from "./file.js";
import type { LanceReadContext } from "./io.js";
import type { LanceField, LanceIndexMetadata, LanceManifest } from "./proto.js";
import { parseIvfMetadata } from "./proto.js";
import { loadLanceIndexMetadata } from "./scalar.js";
import { insertVectorCandidate, lanceVectorDistance } from "./vector-math.js";

export type LanceVectorMetric = "l2" | "cosine" | "dot";

export interface LanceVectorIndexInfo {
  name: string;
  uuid: string;
  column: string;
  indexVersion: number;
  type: "IVF_FLAT";
  metric: LanceVectorMetric;
  dimension: number;
  partitions: number;
}

export interface LanceVectorLimits {
  maxDimension: number;
  maxPartitionsSearched: number;
  maxCandidatesScored: number;
}

export interface LanceVectorCandidate {
  rowId: bigint;
  distance: number;
}

interface OpenedVectorIndex {
  info: LanceVectorIndexInfo;
  auxiliary: Awaited<ReturnType<typeof inspectLanceFile>>;
  partitionOffsets: number[];
  partitionLengths: number[];
  centroids: Float32Array;
}

export async function loadVectorIndexes(options: {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  limits: LanceVectorLimits;
}): Promise<LanceVectorIndexInfo[]> {
  const metadata = await loadLanceIndexMetadata(options);
  const output: LanceVectorIndexInfo[] = [];
  for (const candidate of metadata) {
    if (candidate.detailsTypeUrl.toLowerCase() !== "/lance.index.pb.vectorindexdetails") continue;
    output.push((await openVectorIndex({ ...options, metadata: candidate })).info);
  }
  return output;
}

export async function searchVectorIndex(options: {
  context: LanceReadContext;
  root: string;
  manifestPath: string;
  manifestFileSize: number;
  manifest: LanceManifest;
  indexName: string;
  vector: readonly number[];
  k: number;
  nprobes: number;
  budget: QueryBudget;
  limits: LanceVectorLimits;
}): Promise<{
  index: LanceVectorIndexInfo;
  candidates: LanceVectorCandidate[];
  partitions: number[];
  candidatesScored: number;
}> {
  validateSearchShape(options);
  const metadata = (await loadLanceIndexMetadata(options)).find(
    (candidate) =>
      candidate.detailsTypeUrl.toLowerCase() === "/lance.index.pb.vectorindexdetails" &&
      candidate.name === options.indexName,
  );
  if (metadata === undefined) {
    throw new LakeqlError(
      "LAKEQL_OBJECT_NOT_FOUND",
      `Lance vector index ${options.indexName} does not exist`,
      { index: options.indexName },
    );
  }
  const opened = await openVectorIndex({ ...options, metadata });
  if (options.vector.length !== opened.info.dimension) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Lance query vector dimension mismatch", {
      expected: opened.info.dimension,
      actual: options.vector.length,
    });
  }
  if (options.nprobes > opened.info.partitions) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Lance nprobes exceeds the index partition count",
      { nprobes: options.nprobes, partitions: opened.info.partitions },
    );
  }
  const query = Float32Array.from(options.vector);
  const partitions = selectPartitions(opened, query, options.nprobes);
  const rowOffsets = partitions.flatMap((partition) => {
    const offset = opened.partitionOffsets[partition] as number;
    const length = opened.partitionLengths[partition] as number;
    return Array.from({ length }, (_value, index) => offset + index);
  });
  if (rowOffsets.length > options.limits.maxCandidatesScored) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Lance vector search candidate limit exceeded (${rowOffsets.length} > ${options.limits.maxCandidatesScored})`,
      {
        metric: "vector candidates",
        limit: options.limits.maxCandidatesScored,
        actual: rowOffsets.length,
      },
    );
  }
  options.context.reserveDecodedRows(rowOffsets.length);
  const candidates: LanceVectorCandidate[] = [];
  const bytesPerCandidate = opened.info.dimension * 4 + 64;
  const chunkRows = Math.min(
    1024,
    Math.floor((options.budget.maxMemoryBytes ?? Number.MAX_SAFE_INTEGER) / bytesPerCandidate),
  );
  if (chunkRows < 1) {
    shapeBudget(
      "vector candidate memory bytes",
      options.budget.maxMemoryBytes ?? 0,
      bytesPerCandidate,
    );
  }
  for (let start = 0; start < rowOffsets.length; start += chunkRows) {
    const chunk = rowOffsets.slice(start, start + chunkRows);
    const rows = await materializeInspectedLanceFileRows({
      context: options.context,
      file: opened.auxiliary,
      selections: [
        { field: opened.auxiliary.fields[0] as LanceField, columnIndex: 0 },
        { field: opened.auxiliary.fields[1] as LanceField, columnIndex: 1 },
      ],
      rowOffsets: chunk,
    });
    for (const rowOffset of chunk) {
      const row = rows.get(rowOffset);
      const id = row?._rowid;
      const vector = row?.flat;
      if ((typeof id !== "number" && typeof id !== "bigint") || !(vector instanceof Float32Array)) {
        corrupt("Lance IVF_FLAT auxiliary row is malformed", { rowOffset });
      }
      insertVectorCandidate(
        candidates,
        {
          rowId: BigInt(id),
          distance: lanceVectorDistance(query, vector, opened.info.metric),
        },
        options.k,
      );
    }
  }
  return {
    index: opened.info,
    candidates,
    partitions,
    candidatesScored: rowOffsets.length,
  };
}

async function openVectorIndex(options: {
  context: LanceReadContext;
  root: string;
  manifest: LanceManifest;
  metadata: LanceIndexMetadata;
  limits: LanceVectorLimits;
}): Promise<OpenedVectorIndex> {
  const { metadata } = options;
  if (metadata.indexVersion !== 1) {
    unsupported("Unsupported Lance vector index version", {
      index: metadata.name,
      indexVersion: metadata.indexVersion,
    });
  }
  if (metadata.fields.length !== 1) {
    corrupt("Lance vector index must address exactly one field", {
      index: metadata.name,
      fields: metadata.fields,
    });
  }
  const field = options.manifest.fields.find((candidate) => candidate.id === metadata.fields[0]);
  if (field === undefined) corrupt("Lance vector index references an unknown field");
  const declaredDimension = vectorDimension(field.logicalType);
  if (declaredDimension === undefined) {
    unsupported("Lance vector index field must be a float32 fixed-size list", {
      column: field.name,
      logicalType: field.logicalType,
    });
  }
  if (declaredDimension > options.limits.maxDimension) {
    shapeBudget("vector dimension", options.limits.maxDimension, declaredDimension);
  }
  const files = new Map(metadata.files.map((file) => [file.path, file]));
  const indexMetadata = files.get("index.idx");
  const auxiliaryMetadata = files.get("auxiliary.idx");
  if (indexMetadata === undefined || auxiliaryMetadata === undefined) {
    corrupt("Lance vector index is missing required files", { index: metadata.name });
  }
  const root = joinObjectPath(options.root, "_indices", metadata.uuid);
  const index = await inspectLanceFile(
    options.context,
    joinObjectPath(root, indexMetadata.path),
    indexMetadata.sizeBytes || undefined,
  );
  const indexDescription = parseVectorIndexDescription(index.schemaMetadata["lance:index"]);
  if (indexDescription.type !== "IVF_FLAT") {
    unsupported("Unsupported Lance vector index type", {
      index: metadata.name,
      type: indexDescription.type,
    });
  }
  if (
    index.fields.length !== 1 ||
    index.fields[0]?.name !== "__flat_marker" ||
    index.rowCount !== 0
  ) {
    corrupt("Unsupported Lance IVF_FLAT index-file schema");
  }
  const ivfReference = parsePositiveReference(index.schemaMetadata["lance:ivf"], "IVF metadata");
  const ivf = parseIvfMetadata(
    await readInspectedGlobalBuffer(options.context, index, ivfReference - 1),
  );
  if (ivf.dimension !== declaredDimension) {
    corrupt("Lance IVF centroid dimension disagrees with the indexed field", {
      fieldDimension: declaredDimension,
      centroidDimension: ivf.dimension,
    });
  }
  const auxiliary = await inspectLanceFile(
    options.context,
    joinObjectPath(root, auxiliaryMetadata.path),
    auxiliaryMetadata.sizeBytes || undefined,
  );
  const auxiliaryIvfReference = parsePositiveReference(
    auxiliary.schemaMetadata["lance:ivf"],
    "auxiliary IVF metadata",
  );
  const auxiliaryIvf = parseIvfMetadata(
    await readInspectedGlobalBuffer(options.context, auxiliary, auxiliaryIvfReference - 1),
  );
  if (auxiliaryIvf.numPartitions !== ivf.numPartitions) {
    corrupt("Lance IVF index and auxiliary partition counts disagree");
  }
  validateIvfFlatAuxiliary(
    auxiliary.fields,
    auxiliary.rowCount,
    auxiliaryIvf.offsets,
    auxiliaryIvf.lengths,
    field,
  );
  return {
    info: {
      name: metadata.name,
      uuid: metadata.uuid,
      column: field.name,
      indexVersion: metadata.indexVersion,
      type: "IVF_FLAT",
      metric: indexDescription.metric,
      dimension: ivf.dimension,
      partitions: ivf.numPartitions,
    },
    auxiliary,
    partitionOffsets: auxiliaryIvf.offsets,
    partitionLengths: auxiliaryIvf.lengths,
    centroids: ivf.centroids,
  };
}

function validateSearchShape(options: {
  vector: readonly number[];
  k: number;
  nprobes: number;
  budget: QueryBudget;
  limits: LanceVectorLimits;
}): void {
  if (
    options.vector.length === 0 ||
    options.vector.length > options.limits.maxDimension ||
    options.vector.some((value) => !Number.isFinite(value) || !Number.isFinite(Math.fround(value)))
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid Lance query vector");
  }
  if (!Number.isSafeInteger(options.k) || options.k <= 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Lance vector k must be a positive integer");
  }
  if (!Number.isSafeInteger(options.nprobes) || options.nprobes <= 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Lance vector nprobes must be a positive integer",
    );
  }
  const rowLimit = Math.min(
    options.budget.maxOutputRows ?? Number.MAX_SAFE_INTEGER,
    options.budget.maxBufferedRows ?? Number.MAX_SAFE_INTEGER,
  );
  if (options.k > rowLimit) shapeBudget("vector result rows", rowLimit, options.k);
  if (options.nprobes > options.limits.maxPartitionsSearched) {
    shapeBudget("vector partitions", options.limits.maxPartitionsSearched, options.nprobes);
  }
}

function selectPartitions(
  opened: OpenedVectorIndex,
  query: Float32Array,
  nprobes: number,
): number[] {
  return Array.from({ length: opened.info.partitions }, (_value, partition) => ({
    partition,
    distance: lanceVectorDistance(
      query,
      opened.centroids.subarray(
        partition * opened.info.dimension,
        (partition + 1) * opened.info.dimension,
      ),
      opened.info.metric,
    ),
  }))
    .sort((left, right) => left.distance - right.distance || left.partition - right.partition)
    .slice(0, nprobes)
    .map(({ partition }) => partition);
}

export function parseVectorIndexDescription(bytes: Uint8Array | undefined): {
  type: string;
  metric: LanceVectorMetric;
} {
  if (bytes === undefined) corrupt("Lance vector index has no index description");
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    corrupt("Lance vector index description is invalid JSON");
  }
  if (typeof value !== "object" || value === null) {
    corrupt("Lance vector index description must be an object");
  }
  const record = value as Record<string, unknown>;
  const type = record.type;
  const metric = record.distance_type;
  if (typeof type !== "string" || (metric !== "l2" && metric !== "cosine" && metric !== "dot")) {
    corrupt("Lance vector index description is incomplete");
  }
  return { type, metric };
}

export function parsePositiveReference(bytes: Uint8Array | undefined, label: string): number {
  if (bytes === undefined) corrupt(`Lance ${label} reference is absent`);
  const value = Number(new TextDecoder().decode(bytes));
  if (!Number.isSafeInteger(value) || value <= 0) {
    corrupt(`Lance ${label} reference is invalid`);
  }
  return value;
}

export function validateIvfFlatAuxiliary(
  fields: LanceField[],
  rowCount: number,
  offsets: number[],
  lengths: number[],
  indexedField: LanceField,
): void {
  if (
    fields.length !== 2 ||
    fields[0]?.name !== "_rowid" ||
    fields[0]?.logicalType !== "uint64" ||
    fields[1]?.name !== "flat" ||
    fields[1]?.logicalType !== indexedField.logicalType
  ) {
    corrupt("Unsupported Lance IVF_FLAT auxiliary schema");
  }
  let expectedOffset = 0;
  for (let index = 0; index < offsets.length; index += 1) {
    if (offsets[index] !== expectedOffset) {
      corrupt("Lance IVF partitions are not contiguous and ordered", { partition: index });
    }
    expectedOffset += lengths[index] as number;
  }
  if (expectedOffset !== rowCount) {
    corrupt("Lance IVF partition rows disagree with the auxiliary file", {
      partitionRows: expectedOffset,
      rowCount,
    });
  }
}

function vectorDimension(logicalType: string): number | undefined {
  const match = /^fixed_size_list:float:(\d+)$/u.exec(logicalType);
  if (match === null) return undefined;
  const value = Number(match[1]);
  return Number.isSafeInteger(value) && value > 0 ? value : undefined;
}

function shapeBudget(metric: string, limit: number, actual: number): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Lance ${metric} limit exceeded (${actual} > ${limit})`,
    { metric, limit, actual },
  );
}

function joinObjectPath(...parts: string[]): string {
  return parts
    .flatMap((part) => part.split("/"))
    .filter((part) => part.length > 0)
    .join("/");
}

function unsupported(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_UNSUPPORTED_LANCE_FEATURE", message, details);
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}
