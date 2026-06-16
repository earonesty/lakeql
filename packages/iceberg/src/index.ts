import {
  type Expr,
  jsonSafeValue,
  LakeqlError,
  matches,
  type ObjectStore,
  type ObjectStoreReadControls,
  type OutputManifest,
  type Row,
  readControlSignal,
  stableStringify,
  throwIfAborted,
  withObjectStoreReadControls,
} from "lakeql-core";

export const PACKAGE = "lakeql-iceberg" as const;

interface AvroLongBuffer extends Uint8Array {
  readBigInt64LE(): bigint;
  writeBigInt64LE(value: bigint): number;
}

declare const Buffer: {
  alloc(size: number): AvroLongBuffer;
  from(buffer: ArrayBufferLike, byteOffset?: number, length?: number): Uint8Array;
};

export type IcebergReadMode = "strict" | "ignore-deletes" | "ignore-unsupported-deletes";

export interface LoadIcebergTableOptions extends ObjectStoreReadControls {
  store: ObjectStore;
  metadataPath: string;
}

export interface LoadIcebergTableFromObjectStoreOptions extends ObjectStoreReadControls {
  store: ObjectStore;
  tableLocation: string;
}

export interface IcebergRestCatalogOptions {
  url: string;
  namespace: string | string[];
  table: string;
  prefix?: string;
  token?: string;
  fetch?: typeof fetch;
}

export interface IcebergGlueCatalogOptions {
  region: string;
  namespace: string | string[];
  table: string;
  catalogId?: string;
}

export interface IcebergNessieCatalogOptions {
  url: string;
  namespace: string | string[];
  table: string;
  ref?: string;
  token?: string;
}

export interface LoadIcebergTableFromRestOptions
  extends IcebergRestCatalogOptions,
    ObjectStoreReadControls {
  store: ObjectStore;
}

export interface PlanIcebergFilesOptions {
  snapshotId?: number;
  asOfTimestampMs?: number;
  ref?: string;
  where?: Expr;
  select?: string[];
  readMode?: IcebergReadMode;
}

export interface ProjectIcebergRowOptions {
  snapshotId?: number;
  asOfTimestampMs?: number;
  ref?: string;
  select?: string[];
}

export interface IcebergAppendFile {
  path: string;
  partition?: Record<string, string>;
  recordCount: number;
  fileSizeInBytes: number;
}

export interface IcebergAppendOptions {
  files: IcebergAppendFile[];
  jobId?: string;
  nowMs?: number;
  nextSnapshotId?: number;
  catalog?: IcebergCommitCatalog;
}

export interface IcebergAppendOutputManifestOptions
  extends Omit<IcebergAppendOptions, "files" | "jobId"> {
  manifest: OutputManifest;
}

export interface IcebergCommitCatalog {
  commitAppend(input: IcebergCommitInput): Promise<boolean | IcebergCommitResult>;
}

export interface IcebergCatalog extends IcebergCommitCatalog {
  loadTable(store: ObjectStore): Promise<IcebergTable>;
  listTables(): Promise<IcebergTableIdentifier[]>;
}

export interface IcebergTableIdentifier {
  namespace: string[];
  name: string;
}

export interface IcebergCommitResult {
  committed: boolean;
  metadataPath?: string;
}

export interface IcebergCommitInput {
  store: ObjectStore;
  currentMetadataPath: string;
  nextMetadataPath: string;
  expectedSnapshotId: number;
  nextSnapshotId: number;
  manifestPath: string;
  manifest: Manifest;
  metadata: MetadataFile;
}

export interface IcebergAppendResult {
  snapshotId: number;
  previousSnapshotId: number;
  metadataPath: string;
  manifestPath: string;
  files: PlannedIcebergFile[];
}

export interface IcebergField {
  id: number;
  name: string;
  sourceId?: number;
  type: string;
  required: boolean;
}

export interface PlannedIcebergFile {
  path: string;
  sequenceNumber: number;
  partition: Record<string, string>;
  recordCount: number;
  fileSizeInBytes?: number;
  projectedFieldIds: number[];
  snapshotId: number;
  deleteFiles?: IcebergDeleteFile[];
}

export interface IcebergDeleteFile {
  content: "position-delete" | "equality-delete" | "deletion-vector";
  path: string;
}

export interface IcebergPlan {
  snapshotId: number;
  schemaId: number;
  manifestsRead: number;
  manifestsSkipped: number;
  filesPlanned: number;
  filesSkipped: number;
  deleteFilesPlanned: number;
  deleteFilesIgnored: number;
  files: PlannedIcebergFile[];
}

export interface IcebergPositionDelete {
  path: string;
  position: number;
}

export interface IcebergEqualityDelete {
  columns: string[];
  row: Row;
}

export interface IcebergDeletionVector {
  path: string;
  positions: number[];
}

export interface ApplyIcebergDeletesOptions {
  dataFilePath: string;
  rows: Row[];
  rowOffset?: number;
  positionDeletes?: IcebergPositionDelete[];
  equalityDeletes?: IcebergEqualityDelete[];
  deletionVectors?: IcebergDeletionVector[];
}

export interface DecodedIcebergDeletes {
  positionDeletes?: IcebergPositionDelete[];
  equalityDeletes?: IcebergEqualityDelete[];
  deletionVectors?: IcebergDeletionVector[];
}

export interface IcebergRowBatch {
  rowOffset: number;
  rows: Row[];
}

export interface ScanPlannedIcebergRowsOptions extends ObjectStoreReadControls {
  plan: IcebergPlan | PlannedIcebergFile[];
  readDataFile(file: PlannedIcebergFile): Promise<Row[] | AsyncIterable<Row[] | IcebergRowBatch>>;
  readDeleteFile(
    deleteFile: IcebergDeleteFile,
    dataFile: PlannedIcebergFile,
  ): Promise<DecodedIcebergDeletes>;
}

export interface MetadataFile {
  "format-version": number;
  "table-uuid": string;
  location: string;
  "current-snapshot-id": number;
  refs?: Record<string, { type: "branch" | "tag"; "snapshot-id": number }>;
  schemas: {
    "schema-id": number;
    fields: IcebergField[];
  }[];
  snapshots: Snapshot[];
  "partition-specs"?: IcebergPartitionSpec[];
  "sort-orders"?: IcebergSortOrder[];
}

export interface IcebergPartitionSpec {
  "spec-id": number;
  fields: IcebergPartitionField[];
}

export interface IcebergPartitionField {
  "source-id": number;
  "field-id": number;
  name: string;
  transform: string;
}

export interface IcebergSortOrder {
  "order-id": number;
  fields: unknown[];
}

export interface Snapshot {
  "snapshot-id": number;
  "timestamp-ms": number;
  "schema-id": number;
  "manifest-list"?: string;
  manifests?: Manifest[];
}

export interface Manifest {
  path: string;
  files: ManifestFile[];
  deleteFiles?: ManifestDeleteFile[];
}

export interface ManifestFile {
  path: string;
  sequenceNumber: number;
  partition?: Record<string, string>;
  recordCount: number;
  fileSizeInBytes?: number;
  deleteFiles?: ManifestDeleteFile[];
}

export interface ManifestDeleteFile {
  content: string;
  path: string;
  partition?: Record<string, string>;
}

interface ConditionalObjectStore extends ObjectStore {
  conditionalPut(
    path: string,
    body: Uint8Array | ReadableStream<Uint8Array>,
    options: { contentType?: string; expectedEtag: string | null },
  ): Promise<boolean>;
}

export class IcebergTable {
  private readonly store: ObjectStore;
  readonly metadataPath: string;
  readonly metadata: MetadataFile;

  constructor(store: ObjectStore, metadataPath: string, metadata: MetadataFile) {
    this.store = store;
    this.metadataPath = metadataPath;
    this.metadata = metadata;
  }

  snapshot(options: PlanIcebergFilesOptions = {}): Snapshot {
    if (options.snapshotId !== undefined) return this.snapshotById(options.snapshotId);
    if (options.ref !== undefined) {
      const ref = this.metadata.refs?.[options.ref];
      if (!ref) {
        throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Unknown Iceberg ref ${options.ref}`, {
          ref: options.ref,
        });
      }
      return this.snapshotById(ref["snapshot-id"]);
    }
    if (options.asOfTimestampMs !== undefined) {
      const snapshot = [...this.metadata.snapshots]
        .filter((candidate) => candidate["timestamp-ms"] <= (options.asOfTimestampMs as number))
        .sort((a, b) => b["timestamp-ms"] - a["timestamp-ms"])[0];
      if (!snapshot) {
        throw new LakeqlError(
          "LAKEQL_CATALOG_ERROR",
          "No Iceberg snapshot at requested timestamp",
          {
            asOfTimestampMs: options.asOfTimestampMs,
          },
        );
      }
      return snapshot;
    }
    return this.snapshotById(this.metadata["current-snapshot-id"]);
  }

  schema(schemaId: number): IcebergField[] {
    const schema = this.metadata.schemas.find((candidate) => candidate["schema-id"] === schemaId);
    if (!schema) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Unknown Iceberg schema ${schemaId}`, {
        schemaId,
      });
    }
    return schema.fields;
  }

  projectRow(row: Row, options: ProjectIcebergRowOptions = {}): Row {
    const snapshot = this.snapshot(options);
    const fields = this.schema(snapshot["schema-id"]);
    projectedIds(fields, options.select);
    const selected = options.select === undefined ? undefined : new Set(options.select);
    const out: Row = {};
    for (const field of fields) {
      if (selected !== undefined && !selected.has(field.name)) continue;
      const sourceName =
        field.name in row
          ? field.name
          : field.sourceId !== undefined
            ? this.fieldNameById(field.sourceId)
            : field.name;
      out[field.name] = sourceName !== undefined && sourceName in row ? row[sourceName] : null;
    }
    return out;
  }

  planFiles(options: PlanIcebergFilesOptions = {}): IcebergPlan {
    const snapshot = this.snapshot(options);
    const fields = this.schema(snapshot["schema-id"]);
    const projectedFieldIds = projectedIds(fields, options.select);
    const readMode = options.readMode ?? "strict";
    const files: PlannedIcebergFile[] = [];
    let manifestsSkipped = 0;
    let filesSkipped = 0;
    let deleteFilesPlanned = 0;
    let deleteFilesIgnored = 0;

    const manifests = snapshotManifests(snapshot);
    for (const manifest of manifests) {
      const manifestMayMatch = manifest.files.some((file) =>
        partitionMayMatch(options.where, file.partition ?? {}),
      );
      if (!manifestMayMatch) {
        manifestsSkipped += 1;
        filesSkipped += manifest.files.length;
        continue;
      }

      for (const file of manifest.files) {
        if (!partitionMayMatch(options.where, file.partition ?? {})) {
          filesSkipped += 1;
          continue;
        }
        const supportedDeleteFiles = supportedIcebergDeleteFiles(file.deleteFiles);
        const unsupportedDeleteFiles = unsupportedIcebergDeleteFiles(file.deleteFiles);
        if (unsupportedDeleteFiles.length > 0 && readMode === "strict") {
          throw new LakeqlError(
            "LAKEQL_UNSUPPORTED_DELETE_FILES",
            "Snapshot contains delete files unsupported by strict Iceberg planning",
            {
              path: file.path,
              deleteFiles: file.deleteFiles,
              supportedDeleteFiles,
              unsupportedDeleteFiles,
            },
          );
        }
        const planned: PlannedIcebergFile = {
          path: file.path,
          sequenceNumber: file.sequenceNumber,
          partition: file.partition ?? {},
          recordCount: file.recordCount,
          projectedFieldIds,
          snapshotId: snapshot["snapshot-id"],
        };
        if (file.fileSizeInBytes !== undefined) planned.fileSizeInBytes = file.fileSizeInBytes;
        if (
          (readMode === "strict" || readMode === "ignore-unsupported-deletes") &&
          supportedDeleteFiles.length > 0
        ) {
          planned.deleteFiles = supportedDeleteFiles;
          deleteFilesPlanned += supportedDeleteFiles.length;
        }
        if (readMode === "ignore-unsupported-deletes") {
          deleteFilesIgnored += unsupportedDeleteFiles.length;
        } else if (readMode === "ignore-deletes") {
          deleteFilesIgnored += (file.deleteFiles ?? []).length;
        }
        files.push(planned);
      }
    }

    files.sort((a, b) => a.sequenceNumber - b.sequenceNumber || a.path.localeCompare(b.path));
    return {
      snapshotId: snapshot["snapshot-id"],
      schemaId: snapshot["schema-id"],
      manifestsRead: manifests.length - manifestsSkipped,
      manifestsSkipped,
      filesPlanned: files.length,
      filesSkipped,
      deleteFilesPlanned,
      deleteFilesIgnored,
      files,
    };
  }

  async appendFiles(options: IcebergAppendOptions): Promise<IcebergAppendResult> {
    if (options.files.length === 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Iceberg append requires at least one file");
    }
    if (this.metadata["format-version"] !== 2) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Iceberg append requires format-version 2 metadata",
        { formatVersion: this.metadata["format-version"] },
      );
    }
    const currentSnapshot = this.snapshot();
    const nextSnapshotId =
      options.nextSnapshotId ?? randomSnapshotId(this.metadata.snapshots.map(snapshotIdOf));
    validateNewSnapshotId(nextSnapshotId, this.metadata.snapshots);
    const nextSequenceNumber = maxSequenceNumber(this.metadata) + 1;
    const manifestPath = appendManifestPath(this.metadataPath, options.jobId, nextSnapshotId);
    const manifest: Manifest = {
      path: manifestPath,
      files: options.files.map((file, index) => ({
        path: file.path,
        sequenceNumber: nextSequenceNumber + index,
        partition: sortStringRecord(file.partition ?? {}),
        recordCount: file.recordCount,
        fileSizeInBytes: file.fileSizeInBytes,
      })),
    };
    const nextSnapshot: Snapshot = {
      "snapshot-id": nextSnapshotId,
      "timestamp-ms": options.nowMs ?? Date.now(),
      "schema-id": currentSnapshot["schema-id"],
      manifests: [...snapshotManifests(currentSnapshot).map(cloneManifest), manifest],
    };
    const metadata = cloneMetadata(this.metadata);
    metadata["current-snapshot-id"] = nextSnapshotId;
    metadata.snapshots.push(nextSnapshot);
    metadata.refs = {
      ...(metadata.refs ?? {}),
      main: { type: "branch", "snapshot-id": nextSnapshotId },
    };

    const nextMetadataPath = nextMetadataPathFor(this.metadataPath, nextSnapshotId);
    const catalog = options.catalog ?? new ObjectStoreIcebergCommitCatalog();
    const commit = await catalog.commitAppend({
      store: this.store,
      currentMetadataPath: this.metadataPath,
      nextMetadataPath,
      expectedSnapshotId: currentSnapshot["snapshot-id"],
      nextSnapshotId,
      manifestPath,
      manifest,
      metadata,
    });
    const committed = typeof commit === "boolean" ? commit : commit.committed;
    if (!committed) {
      throw new LakeqlError("LAKEQL_ICEBERG_COMMIT_CONFLICT", "Iceberg append commit conflict", {
        metadataPath: this.metadataPath,
        expectedSnapshotId: currentSnapshot["snapshot-id"],
        nextSnapshotId,
      });
    }
    const committedMetadataPath =
      typeof commit === "boolean" ? nextMetadataPath : (commit.metadataPath ?? nextMetadataPath);

    return {
      snapshotId: nextSnapshotId,
      previousSnapshotId: currentSnapshot["snapshot-id"],
      metadataPath: committedMetadataPath,
      manifestPath,
      files: manifest.files.map((file) => ({
        path: file.path,
        sequenceNumber: file.sequenceNumber,
        partition: file.partition ?? {},
        recordCount: file.recordCount,
        projectedFieldIds: [],
        snapshotId: nextSnapshotId,
      })),
    };
  }

  async appendOutputManifest(
    options: IcebergAppendOutputManifestOptions,
  ): Promise<IcebergAppendResult> {
    const files = options.manifest.entries.map((entry): IcebergAppendFile => {
      if (entry.iceberg === undefined) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Output manifest entry is missing Iceberg file metadata",
          {
            taskId: entry.taskId,
            outputPath: entry.outputPath,
          },
        );
      }
      return {
        path: entry.outputPath,
        partition: entry.iceberg.partitionValues,
        recordCount: entry.iceberg.recordCount,
        fileSizeInBytes: entry.iceberg.fileSizeInBytes,
      };
    });
    return await this.appendFiles({
      files,
      jobId: options.manifest.jobId,
      ...(options.nowMs !== undefined ? { nowMs: options.nowMs } : {}),
      ...(options.nextSnapshotId !== undefined ? { nextSnapshotId: options.nextSnapshotId } : {}),
      ...(options.catalog !== undefined ? { catalog: options.catalog } : {}),
    });
  }

  private snapshotById(snapshotId: number): Snapshot {
    const snapshot = this.metadata.snapshots.find(
      (candidate) => candidate["snapshot-id"] === snapshotId,
    );
    if (!snapshot) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Unknown Iceberg snapshot ${snapshotId}`, {
        snapshotId,
      });
    }
    return snapshot;
  }

  private fieldNameById(fieldId: number): string | undefined {
    for (const schema of this.metadata.schemas) {
      const field = schema.fields.find((candidate) => candidate.id === fieldId);
      if (field) return field.name;
    }
    return undefined;
  }
}

export function planFiles(table: IcebergTable, options: PlanIcebergFilesOptions = {}): IcebergPlan {
  return table.planFiles(options);
}

export class ObjectStoreIcebergCommitCatalog implements IcebergCommitCatalog {
  async commitAppend(input: IcebergCommitInput): Promise<IcebergCommitResult> {
    if (!supportsConditionalPut(input.store)) {
      throw new LakeqlError(
        "LAKEQL_CATALOG_ERROR",
        "Object-store Iceberg append requires conditional put support",
        { metadataPath: input.currentMetadataPath },
      );
    }
    const currentBytes = await input.store.get(input.currentMetadataPath);
    if (!currentBytes) {
      throw new LakeqlError(
        "LAKEQL_OBJECT_NOT_FOUND",
        `No object at ${input.currentMetadataPath}`,
        {
          path: input.currentMetadataPath,
        },
      );
    }
    const current = validateMetadata(JSON.parse(new TextDecoder().decode(currentBytes)) as unknown);
    if (current["current-snapshot-id"] !== input.expectedSnapshotId) {
      return { committed: false };
    }
    const versionHintPath = metadataVersionHintPath(input.nextMetadataPath);
    const versionHintHead = await input.store.head(versionHintPath);
    await input.store.put(
      input.manifestPath,
      new TextEncoder().encode(`${stableStringify(input.manifest)}\n`),
      { contentType: "application/json" },
    );
    await input.store.put(
      input.nextMetadataPath,
      new TextEncoder().encode(`${JSON.stringify(input.metadata, null, 2)}\n`),
      { contentType: "application/json" },
    );
    const updated = await input.store.conditionalPut(
      versionHintPath,
      new TextEncoder().encode(`${input.nextSnapshotId}\n`),
      { contentType: "text/plain", expectedEtag: versionHintHead?.etag ?? null },
    );
    if (!updated) return { committed: false };
    return { committed: true, metadataPath: input.nextMetadataPath };
  }
}

export class IcebergRestCatalog implements IcebergCatalog {
  private readonly url: string;
  private readonly namespace: string[];
  private readonly table: string;
  private readonly prefix: string[];
  private readonly token: string | undefined;
  private readonly fetchFn: typeof fetch;

  constructor(options: IcebergRestCatalogOptions) {
    this.url = options.url;
    this.namespace = namespaceParts(options.namespace);
    this.table = requiredNonEmptyString(options.table, "table");
    this.prefix = catalogPrefixParts(options.prefix);
    this.token = options.token;
    this.fetchFn = options.fetch ?? fetch;
  }

  async loadTable(store: ObjectStore): Promise<IcebergTable> {
    const response = await this.requestJson(this.tableUrl(), { method: "GET" });
    if (!isRecord(response) || typeof response["metadata-location"] !== "string") {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Invalid Iceberg REST load table response", {
        url: this.tableUrl(),
      });
    }
    return new IcebergTable(
      store,
      response["metadata-location"],
      await hydrateMetadataManifests(store, validateMetadata(response.metadata)),
    );
  }

  async listTables(): Promise<IcebergTableIdentifier[]> {
    return validateListTablesResponse(
      await this.requestJson(this.namespaceTablesUrl(), {
        method: "GET",
      }),
    );
  }

  async commitAppend(input: IcebergCommitInput): Promise<IcebergCommitResult> {
    await input.store.put(
      input.manifestPath,
      new TextEncoder().encode(`${stableStringify(input.manifest)}\n`),
      { contentType: "application/json" },
    );
    await input.store.put(
      input.nextMetadataPath,
      new TextEncoder().encode(`${JSON.stringify(input.metadata, null, 2)}\n`),
      { contentType: "application/json" },
    );

    const response = await this.fetchFn(this.tableUrl(), {
      method: "POST",
      headers: this.headers(true),
      body: JSON.stringify({
        identifier: { namespace: this.namespace, name: this.table },
        requirements: [
          {
            type: "assert-ref-snapshot-id",
            ref: "main",
            "snapshot-id": input.expectedSnapshotId,
          },
        ],
        updates: [
          {
            action: "add-snapshot",
            snapshot: restAppendSnapshot(input),
          },
          {
            action: "set-snapshot-ref",
            "ref-name": "main",
            type: "branch",
            "snapshot-id": input.nextSnapshotId,
          },
        ],
      }),
    });
    if (response.status === 409) return { committed: false };
    if (!response.ok) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg REST table commit failed", {
        url: this.tableUrl(),
        status: response.status,
        statusText: response.statusText,
      });
    }
    const metadataPath = await commitResponseMetadataPath(response);
    return {
      committed: true,
      ...(metadataPath !== undefined ? { metadataPath } : {}),
    };
  }

  private async requestJson(url: string, init: RequestInit): Promise<unknown> {
    const response = await this.fetchFn(url, {
      ...init,
      headers: this.headers(init.body !== undefined),
    });
    if (!response.ok) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg REST catalog request failed", {
        url,
        status: response.status,
        statusText: response.statusText,
      });
    }
    try {
      return await response.json();
    } catch (cause) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg REST catalog response is not JSON", {
        url,
        cause,
      });
    }
  }

  private headers(hasBody: boolean): Headers {
    const headers = new Headers({ accept: "application/json" });
    if (hasBody) headers.set("content-type", "application/json");
    if (this.token !== undefined) headers.set("authorization", `Bearer ${this.token}`);
    return headers;
  }

  private tableUrl(): string {
    return restCatalogUrl(this.url, [...this.namespaceTableSegments(), this.table]);
  }

  private namespaceTablesUrl(): string {
    return restCatalogUrl(this.url, this.namespaceTableSegments());
  }

  private namespaceTableSegments(): string[] {
    return ["v1", ...this.prefix, "namespaces", this.namespace.join("\u001f"), "tables"];
  }
}

export function icebergRestCatalog(options: IcebergRestCatalogOptions): IcebergRestCatalog {
  return new IcebergRestCatalog(options);
}

export class IcebergUnsupportedCatalog implements IcebergCatalog {
  private readonly catalog: string;
  private readonly namespace: string[];
  private readonly table: string;

  constructor(catalog: string, namespace: string | string[], table: string) {
    this.catalog = requiredNonEmptyString(catalog, "catalog");
    this.namespace = namespaceParts(namespace);
    this.table = requiredNonEmptyString(table, "table");
  }

  async loadTable(_store: ObjectStore): Promise<IcebergTable> {
    throw this.unsupported("loadTable");
  }

  async listTables(): Promise<IcebergTableIdentifier[]> {
    throw this.unsupported("listTables");
  }

  async commitAppend(_input: IcebergCommitInput): Promise<IcebergCommitResult> {
    throw this.unsupported("commitAppend");
  }

  private unsupported(operation: string): LakeqlError {
    return new LakeqlError(
      "LAKEQL_CATALOG_ERROR",
      `${this.catalog} Iceberg catalog is not implemented`,
      {
        catalog: this.catalog,
        namespace: this.namespace,
        table: this.table,
        operation,
      },
    );
  }
}

export function icebergGlueCatalog(options: IcebergGlueCatalogOptions): IcebergCatalog {
  requiredNonEmptyString(options.region, "region");
  return new IcebergUnsupportedCatalog("Glue", options.namespace, options.table);
}

export function icebergNessieCatalog(options: IcebergNessieCatalogOptions): IcebergCatalog {
  requiredNonEmptyString(options.url, "url");
  if (options.ref !== undefined) requiredNonEmptyString(options.ref, "ref");
  return new IcebergUnsupportedCatalog("Nessie", options.namespace, options.table);
}

export async function loadIcebergTable(options: LoadIcebergTableOptions): Promise<IcebergTable> {
  const readControls = loadReadControls(options);
  const store = withObjectStoreReadControls(options.store, readControls);
  const bytes = await store.get(options.metadataPath);
  if (!bytes) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No object at ${options.metadataPath}`, {
      path: options.metadataPath,
    });
  }
  throwIfAborted(readControls.signal);
  const text = new TextDecoder().decode(bytes);
  try {
    return new IcebergTable(
      store,
      options.metadataPath,
      await hydrateMetadataManifests(store, validateMetadata(JSON.parse(text)), readControls),
    );
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError(
      "LAKEQL_CATALOG_ERROR",
      `Invalid Iceberg metadata at ${options.metadataPath}`,
      {
        path: options.metadataPath,
        cause,
      },
    );
  }
}

export async function loadIcebergTableFromObjectStore(
  options: LoadIcebergTableFromObjectStoreOptions,
): Promise<IcebergTable> {
  const readControls = loadReadControls(options);
  const store = withObjectStoreReadControls(options.store, readControls);
  const tableLocation = trimTrailingSlash(options.tableLocation);
  const metadataPrefix = `${tableLocation}/metadata/`;
  const versionHintPath = `${metadataPrefix}version-hint.text`;
  const hintedVersion = await readVersionHint(store, versionHintPath, readControls);
  const metadataPath =
    hintedVersion === undefined
      ? await latestMetadataPathFromList(store, metadataPrefix, readControls)
      : `${metadataPrefix}v${hintedVersion}.metadata.json`;
  return await loadIcebergTable({ store, metadataPath, ...readControls });
}

export async function loadIcebergTableFromRest(
  options: LoadIcebergTableFromRestOptions,
): Promise<IcebergTable> {
  return await icebergRestCatalog(options).loadTable(
    withObjectStoreReadControls(options.store, loadReadControls(options)),
  );
}

function loadReadControls(options: ObjectStoreReadControls): ObjectStoreReadControls {
  const controls: ObjectStoreReadControls = {};
  if (options.maxConcurrentReads !== undefined)
    controls.maxConcurrentReads = options.maxConcurrentReads;
  const signal = readControlSignal(options);
  if (signal !== undefined) controls.signal = signal;
  return controls;
}

export function applyIcebergDeletes(options: ApplyIcebergDeletesOptions): Row[] {
  const rowOffset = options.rowOffset ?? 0;
  const positionDeletes = new Set<number>();
  for (const deletion of options.positionDeletes ?? []) {
    if (deletion.path !== options.dataFilePath) continue;
    positionDeletes.add(validateDeletePosition(deletion.position, deletion.path));
  }
  for (const deletionVector of options.deletionVectors ?? []) {
    if (deletionVector.path !== options.dataFilePath) continue;
    for (const position of deletionVector.positions) {
      positionDeletes.add(validateDeletePosition(position, deletionVector.path));
    }
  }

  const equalityDeleteKeys = new Map<string, Set<string>>();
  for (const deletion of options.equalityDeletes ?? []) {
    if (deletion.columns.length === 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Iceberg equality delete requires columns");
    }
    const columnsKey = stableStringify(deletion.columns);
    let keys = equalityDeleteKeys.get(columnsKey);
    if (!keys) {
      keys = new Set<string>();
      equalityDeleteKeys.set(columnsKey, keys);
    }
    keys.add(equalityKey(deletion.row, deletion.columns));
  }

  return options.rows.filter((row, index) => {
    if (positionDeletes.has(rowOffset + index)) return false;
    for (const [columnsKey, keys] of equalityDeleteKeys) {
      const columns = JSON.parse(columnsKey) as string[];
      if (keys.has(equalityKey(row, columns))) return false;
    }
    return true;
  });
}

export async function* scanPlannedIcebergRows(
  options: ScanPlannedIcebergRowsOptions,
): AsyncIterable<Row[]> {
  const files = Array.isArray(options.plan) ? options.plan : options.plan.files;
  const signal = readControlSignal(options);
  for (const file of files) {
    throwIfAborted(signal);
    const deletes = await decodedDeletesForFile(file, options, signal);
    throwIfAborted(signal);
    const data = await options.readDataFile(file);
    throwIfAborted(signal);
    let rowOffset = 0;
    for await (const batch of rowBatches(data)) {
      throwIfAborted(signal);
      const rows = Array.isArray(batch) ? batch : batch.rows;
      const absoluteRowOffset = Array.isArray(batch) ? rowOffset : batch.rowOffset;
      const visibleRows = hasDeletes(deletes)
        ? applyIcebergDeletes({
            dataFilePath: file.path,
            rows,
            rowOffset: absoluteRowOffset,
            ...deletes,
          })
        : rows;
      rowOffset = absoluteRowOffset + rows.length;
      if (visibleRows.length > 0) yield visibleRows;
      throwIfAborted(signal);
    }
  }
}

async function decodedDeletesForFile(
  file: PlannedIcebergFile,
  options: ScanPlannedIcebergRowsOptions,
  signal: AbortSignal | undefined,
): Promise<DecodedIcebergDeletes> {
  const out: DecodedIcebergDeletes = {};
  for (const deleteFile of file.deleteFiles ?? []) {
    throwIfAborted(signal);
    const decoded = await options.readDeleteFile(deleteFile, file);
    throwIfAborted(signal);
    pushDeletes(out, decoded);
  }
  return out;
}

function pushDeletes(target: DecodedIcebergDeletes, source: DecodedIcebergDeletes): void {
  if (source.positionDeletes !== undefined) {
    target.positionDeletes = [...(target.positionDeletes ?? []), ...source.positionDeletes];
  }
  if (source.equalityDeletes !== undefined) {
    target.equalityDeletes = [...(target.equalityDeletes ?? []), ...source.equalityDeletes];
  }
  if (source.deletionVectors !== undefined) {
    target.deletionVectors = [...(target.deletionVectors ?? []), ...source.deletionVectors];
  }
}

function hasDeletes(deletes: DecodedIcebergDeletes): boolean {
  return (
    (deletes.positionDeletes?.length ?? 0) > 0 ||
    (deletes.equalityDeletes?.length ?? 0) > 0 ||
    (deletes.deletionVectors?.length ?? 0) > 0
  );
}

async function* rowBatches(
  rows: Row[] | AsyncIterable<Row[] | IcebergRowBatch>,
): AsyncIterable<Row[] | IcebergRowBatch> {
  if (isAsyncIterable(rows)) {
    yield* rows;
  } else {
    yield rows;
  }
}

function isAsyncIterable(value: unknown): value is AsyncIterable<Row[] | IcebergRowBatch> {
  return (
    typeof value === "object" &&
    value !== null &&
    Symbol.asyncIterator in value &&
    typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
  );
}

function validateDeletePosition(position: number, path: string): number {
  if (!Number.isInteger(position) || position < 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Iceberg delete position must be non-negative",
      {
        path,
        position,
      },
    );
  }
  return position;
}

function nextMetadataPathFor(metadataPath: string, snapshotId: number): string {
  const slash = metadataPath.lastIndexOf("/");
  const prefix = slash === -1 ? "" : `${metadataPath.slice(0, slash + 1)}`;
  return `${prefix}v${snapshotId}.metadata.json`;
}

function randomSnapshotId(existingIds: readonly number[]): number {
  const existing = new Set(existingIds);
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const id = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER) + 1;
    if (!existing.has(id)) return id;
  }
  throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Unable to allocate unique Iceberg snapshot id");
}

function validateNewSnapshotId(snapshotId: number, snapshots: readonly Snapshot[]): void {
  if (!Number.isSafeInteger(snapshotId) || snapshotId <= 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Iceberg snapshot id must be a positive safe integer",
      { snapshotId },
    );
  }
  if (snapshots.some((snapshot) => snapshotIdOf(snapshot) === snapshotId)) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Iceberg snapshot id already exists", {
      snapshotId,
    });
  }
}

function snapshotIdOf(snapshot: Snapshot): number {
  return snapshot["snapshot-id"];
}

function metadataVersionHintPath(metadataPath: string): string {
  const slash = metadataPath.lastIndexOf("/");
  const prefix = slash === -1 ? "" : `${metadataPath.slice(0, slash + 1)}`;
  return `${prefix}version-hint.text`;
}

function supportsConditionalPut(store: ObjectStore): store is ConditionalObjectStore {
  return typeof (store as Partial<ConditionalObjectStore>).conditionalPut === "function";
}

async function hydrateMetadataManifests(
  store: ObjectStore,
  metadata: MetadataFile,
  controls: ObjectStoreReadControls = {},
): Promise<MetadataFile> {
  const hydrated = cloneMetadata(metadata);
  const tablePrefix = tableLocationObjectPrefix(hydrated.location);
  for (const snapshot of hydrated.snapshots) {
    throwIfAborted(controls.signal);
    const manifestReferences =
      snapshot.manifests ??
      (snapshot["manifest-list"] !== undefined
        ? await readManifestList(
            store,
            validateManifestSourcedPath(snapshot["manifest-list"], tablePrefix),
          )
        : []);
    const manifests = await Promise.all(
      manifestReferences.map(async (manifest) => {
        const manifestPath = validateManifestSourcedPath(manifest.path, tablePrefix);
        if (Array.isArray(manifest.files))
          return validateManifestPaths(manifest, manifestPath, tablePrefix);
        return await readManifest(store, manifestPath, tablePrefix);
      }),
    );
    throwIfAborted(controls.signal);
    snapshot.manifests = mergeDeleteManifests(manifests);
  }
  return hydrated;
}

function mergeDeleteManifests(manifests: Manifest[]): Manifest[] {
  const deleteFiles = manifests.flatMap((manifest) => manifest.deleteFiles ?? []);
  const dataManifests = manifests.filter((manifest) => manifest.files.length > 0);
  if (deleteFiles.length === 0) return dataManifests;
  return dataManifests.map((manifest) => ({
    ...manifest,
    files: manifest.files.map((file) => {
      const applicable = deleteFiles.filter((deleteFile) => deleteFileMayApply(deleteFile, file));
      if (applicable.length === 0) return file;
      return {
        ...file,
        deleteFiles: [...(file.deleteFiles ?? []), ...applicable.map(publicDeleteFile)],
      };
    }),
  }));
}

function deleteFileMayApply(deleteFile: ManifestDeleteFile, file: ManifestFile): boolean {
  if (deleteFile.partition === undefined || Object.keys(deleteFile.partition).length === 0) {
    return true;
  }
  const filePartition = file.partition ?? {};
  return Object.entries(deleteFile.partition).every(([key, value]) => filePartition[key] === value);
}

function publicDeleteFile(deleteFile: ManifestDeleteFile): ManifestDeleteFile {
  return { content: deleteFile.content, path: deleteFile.path };
}

async function readManifestList(store: ObjectStore, path: string): Promise<Manifest[]> {
  const bytes = await store.get(path);
  if (!bytes) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No Iceberg manifest list at ${path}`, {
      path,
    });
  }
  try {
    return avroObjectContainer(bytes)
      ? validateAvroManifestList(await decodeAvroObjectContainer(bytes), path)
      : validateManifestList(JSON.parse(new TextDecoder().decode(bytes)), path);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Invalid Iceberg manifest list at ${path}`, {
      path,
      cause,
    });
  }
}

async function readManifest(store: ObjectStore, path: string, tablePrefix = ""): Promise<Manifest> {
  const bytes = await store.get(path);
  if (!bytes) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No Iceberg manifest at ${path}`, { path });
  }
  try {
    const manifest = avroObjectContainer(bytes)
      ? validateAvroManifest(await decodeAvroObjectContainer(bytes), path)
      : validateManifest(JSON.parse(new TextDecoder().decode(bytes)), path);
    return validateManifestPaths(manifest, path, tablePrefix);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", `Invalid Iceberg manifest at ${path}`, {
      path,
      cause,
    });
  }
}

function validateAvroManifestList(records: unknown[], path: string): Manifest[] {
  return records.map((record) => {
    if (!isRecord(record) || typeof record.manifest_path !== "string") {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg Avro manifest list entry is invalid", {
        path,
      });
    }
    validateManifestContent(record.content, path);
    return { path: record.manifest_path } as Manifest;
  });
}

function validateAvroManifest(records: unknown[], path: string): Manifest {
  const files: ManifestFile[] = [];
  const deleteFiles: ManifestDeleteFile[] = [];
  for (const record of records) {
    if (!isRecord(record)) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg Avro manifest entry is invalid", {
        path,
      });
    }
    if (typeof record.status === "number" && record.status === 2) continue;
    const dataFile = record.data_file;
    if (!isRecord(dataFile)) {
      throw new LakeqlError(
        "LAKEQL_CATALOG_ERROR",
        "Iceberg Avro manifest entry is missing data_file",
        {
          path,
        },
      );
    }
    const content = typeof dataFile.content === "number" ? dataFile.content : 0;
    const recordCount = safeAvroNumber(dataFile.record_count);
    if (typeof dataFile.file_path !== "string" || recordCount === undefined) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg Avro data file has invalid fields", {
        path,
      });
    }
    if (content !== 0) {
      deleteFiles.push({
        content: avroDeleteContent(content, dataFile),
        path: dataFile.file_path,
        partition: avroPartitionValues(dataFile.partition),
      });
      continue;
    }
    const sequenceNumber =
      safeAvroNumber(record.sequence_number) ?? safeAvroNumber(record.file_sequence_number) ?? 0;
    const file: ManifestFile = {
      path: dataFile.file_path,
      sequenceNumber,
      partition: avroPartitionValues(dataFile.partition),
      recordCount,
    };
    const fileSizeInBytes = safeAvroNumber(dataFile.file_size_in_bytes);
    if (fileSizeInBytes !== undefined) {
      file.fileSizeInBytes = fileSizeInBytes;
    }
    files.push(file);
  }
  const manifest: Manifest = { path, files };
  if (deleteFiles.length > 0) manifest.deleteFiles = deleteFiles;
  return validateManifestPaths(manifest, path);
}

function avroDeleteContent(content: number, dataFile: Record<string, unknown>): string {
  if (
    content === 1 &&
    (String(dataFile.file_format).toLowerCase() === "puffin" ||
      dataFile.content_offset !== undefined ||
      dataFile.content_size_in_bytes !== undefined)
  ) {
    return "deletion-vector";
  }
  if (content === 1) return "position-delete";
  if (content === 2) return "equality-delete";
  return `unsupported-delete-${content}`;
}

function avroPartitionValues(value: unknown): Record<string, string> {
  if (!isRecord(value)) return {};
  const out: Record<string, string> = {};
  for (const [key, inner] of Object.entries(value)) {
    if (inner === null || inner === undefined) continue;
    out[key] = String(jsonSafeValue(inner));
  }
  return sortStringRecord(out);
}

function safeAvroNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (typeof value === "bigint") {
    const numberValue = Number(value);
    if (Number.isSafeInteger(numberValue) && BigInt(numberValue) === value) return numberValue;
  }
  return undefined;
}

function avroObjectContainer(bytes: Uint8Array): boolean {
  return (
    bytes.length >= 4 &&
    bytes[0] === 0x4f &&
    bytes[1] === 0x62 &&
    bytes[2] === 0x6a &&
    bytes[3] === 0x01
  );
}

async function decodeAvroObjectContainer(bytes: Uint8Array): Promise<unknown[]> {
  const avro = await loadAvro();
  const avroBigIntLongType = avro.types.LongType.__with({
    fromBuffer: (buffer: AvroLongBuffer) => buffer.readBigInt64LE(),
    toBuffer: (value: bigint | number) => {
      const buffer = Buffer.alloc(8);
      buffer.writeBigInt64LE(BigInt(value));
      return buffer;
    },
    fromJSON: (value: string | number | bigint) => BigInt(value),
    toJSON: (value: bigint | number) => value.toString(),
    isValid: (value: unknown) =>
      typeof value === "bigint" || (typeof value === "number" && Number.isSafeInteger(value)),
    compare: (left: bigint | number, right: bigint | number) => {
      const leftBigInt = BigInt(left);
      const rightBigInt = BigInt(right);
      return leftBigInt === rightBigInt ? 0 : leftBigInt < rightBigInt ? -1 : 1;
    },
  });
  const avroLongTypeHook = (schema: unknown): unknown =>
    schema === "long" || (isRecord(schema) && schema.type === "long")
      ? avroBigIntLongType
      : undefined;
  const decoder = new avro.streams.BlockDecoder({
    parseHook: (schema) => avro.Type.forSchema(schema, { typeHook: avroLongTypeHook }),
  }) as AvroBlockDecoder;
  const records: unknown[] = [];
  const done = new Promise<unknown[]>((resolve, reject) => {
    decoder.on("data", (record: unknown) => records.push(record));
    decoder.on("end", () => resolve(records));
    decoder.on("error", reject);
  });
  decoder.end(Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength));
  return done;
}

async function loadAvro(): Promise<{
  types: { LongType: { __with(methods: Record<string, unknown>): unknown } };
  Type: {
    forSchema(schema: unknown, options: { typeHook: (schema: unknown) => unknown }): unknown;
  };
  streams: { BlockDecoder: new (options: { parseHook: (schema: unknown) => unknown }) => unknown };
}> {
  const module = (await import("avsc")) as {
    default?: {
      types: { LongType: { __with(methods: Record<string, unknown>): unknown } };
      Type: {
        forSchema(schema: unknown, options: { typeHook: (schema: unknown) => unknown }): unknown;
      };
      streams: {
        BlockDecoder: new (options: { parseHook: (schema: unknown) => unknown }) => unknown;
      };
    };
    types: { LongType: { __with(methods: Record<string, unknown>): unknown } };
    Type: {
      forSchema(schema: unknown, options: { typeHook: (schema: unknown) => unknown }): unknown;
    };
    streams: {
      BlockDecoder: new (options: { parseHook: (schema: unknown) => unknown }) => unknown;
    };
  };
  return module.default ?? module;
}

interface AvroBlockDecoder {
  on(event: "data", listener: (record: unknown) => void): this;
  on(event: "end", listener: () => void): this;
  on(event: "error", listener: (error: Error) => void): this;
  end(chunk: Uint8Array): void;
}

function validateManifestList(value: unknown, path: string): Manifest[] {
  const manifests = Array.isArray(value)
    ? value
    : isRecord(value) && Array.isArray(value.manifests)
      ? value.manifests
      : undefined;
  if (manifests === undefined) {
    throw new LakeqlError(
      "LAKEQL_CATALOG_ERROR",
      "Iceberg manifest list has invalid required fields",
      {
        path,
      },
    );
  }
  return manifests.map((manifest) => validateManifestReference(manifest, path));
}

function validateManifest(value: unknown, path: string): Manifest {
  if (!isRecord(value) || typeof value.path !== "string" || !Array.isArray(value.files)) {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg manifest has invalid required fields", {
      path,
    });
  }
  return value as unknown as Manifest;
}

function validateManifestReference(value: unknown, path: string): Manifest {
  if (!isRecord(value) || typeof value.path !== "string") {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg manifest list entry is invalid", {
      path,
    });
  }
  validateManifestContent(value.content, path);
  if (Array.isArray(value.files)) return value as unknown as Manifest;
  return { path: value.path } as Manifest;
}

function validateManifestContent(content: unknown, path: string): void {
  if (content === undefined || content === null) return;
  if (content === 0 || content === 1 || content === "data" || content === "deletes") return;
  throw new LakeqlError(
    "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
    "Iceberg manifest list contains an unsupported manifest content type",
    {
      path,
      content,
    },
  );
}

function validateManifestPaths(manifest: Manifest, path: string, tablePrefix = ""): Manifest {
  validateManifestSourcedPath(manifest.path, tablePrefix);
  for (const file of manifest.files) {
    validateManifestSourcedPath(file.path, tablePrefix);
    for (const deleteFile of file.deleteFiles ?? []) {
      validateManifestSourcedPath(deleteFile.path, tablePrefix);
    }
  }
  for (const deleteFile of manifest.deleteFiles ?? []) {
    validateManifestSourcedPath(deleteFile.path, tablePrefix);
  }
  return { ...manifest, path };
}

function validateManifestSourcedPath(path: string, tablePrefix = ""): string {
  if (/^(?:[a-z][a-z0-9+.-]*:)?\/\//iu.test(path) || path.startsWith("/")) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      `Iceberg manifest path must be relative: ${path}`,
      {
        path,
      },
    );
  }
  for (const segment of path.split("/")) {
    let decoded: string;
    try {
      decoded = decodeURIComponent(segment);
    } catch {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        `Iceberg manifest path is invalid: ${path}`,
        {
          path,
        },
      );
    }
    if (decoded === "." || decoded === "..") {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        `Iceberg manifest path contains traversal: ${path}`,
        {
          path,
        },
      );
    }
  }
  if (tablePrefix !== "" && path !== tablePrefix && !path.startsWith(`${tablePrefix}/`)) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      `Iceberg manifest path escapes table location: ${path}`,
      {
        path,
        tableLocation: tablePrefix,
      },
    );
  }
  return path;
}

function tableLocationObjectPrefix(location: string): string {
  const trimmed = trimTrailingSlash(location.trim());
  if (trimmed === "" || !trimmed.includes("/")) return "";
  if (/^[a-z][a-z0-9+.-]*:\/\//iu.test(trimmed)) {
    const url = new URL(trimmed);
    return trimSlashes(decodeURIComponent(url.pathname));
  }
  return trimSlashes(trimmed);
}

function trimSlashes(value: string): string {
  return value.replace(/^\/+|\/+$/gu, "");
}

async function readVersionHint(
  store: ObjectStore,
  path: string,
  controls: ObjectStoreReadControls = {},
): Promise<number | undefined> {
  const bytes = await store.get(path);
  if (!bytes) return undefined;
  throwIfAborted(controls.signal);
  const text = new TextDecoder().decode(bytes).trim();
  const version = Number(text);
  if (!Number.isInteger(version) || version < 0) {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Invalid Iceberg version hint", {
      path,
      versionHint: text,
    });
  }
  return version;
}

async function latestMetadataPathFromList(
  store: ObjectStore,
  metadataPrefix: string,
  controls: ObjectStoreReadControls = {},
): Promise<string> {
  let latest: { path: string; version: number } | undefined;
  for await (const object of store.list(metadataPrefix)) {
    throwIfAborted(controls.signal);
    const name = object.path.slice(metadataPrefix.length);
    const match = /^v(\d+)\.metadata\.json$/u.exec(name);
    if (!match) continue;
    const version = Number(match[1]);
    if (!Number.isSafeInteger(version)) continue;
    if (latest === undefined || version > latest.version) latest = { path: object.path, version };
  }
  if (latest === undefined) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", "No Iceberg metadata files found", {
      prefix: metadataPrefix,
    });
  }
  return latest.path;
}

function trimTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function restCatalogUrl(baseUrl: string, segments: string[]): string {
  const url = new URL(baseUrl);
  const basePath = url.pathname.replace(/\/+$/u, "");
  const encodedSegments = segments.map((segment) => encodeURIComponent(segment));
  url.pathname = [basePath, ...encodedSegments].filter((segment) => segment.length > 0).join("/");
  return url.toString();
}

function namespaceParts(namespace: string | string[]): string[] {
  const parts = Array.isArray(namespace) ? namespace : namespace.split(".");
  const out = parts.map((part) => requiredNonEmptyString(part, "namespace"));
  if (out.length === 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Iceberg REST namespace is required");
  }
  return out;
}

function catalogPrefixParts(prefix: string | undefined): string[] {
  if (prefix === undefined || prefix.trim() === "") return [];
  return prefix
    .split("/")
    .filter((part) => part.length > 0)
    .map((part) => requiredNonEmptyString(part, "prefix"));
}

function requiredNonEmptyString(value: string, field: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", `Iceberg REST ${field} is required`);
  }
  return trimmed;
}

async function commitResponseMetadataPath(response: Response): Promise<string | undefined> {
  if (response.status === 204) return undefined;
  try {
    const body = (await response.json()) as unknown;
    if (isRecord(body) && typeof body["metadata-location"] === "string") {
      return body["metadata-location"];
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function restAppendSnapshot(input: IcebergCommitInput): Record<string, unknown> {
  const snapshot = input.metadata.snapshots.at(-1);
  if (snapshot === undefined) {
    throw new LakeqlError(
      "LAKEQL_CATALOG_ERROR",
      "Iceberg append metadata is missing next snapshot",
    );
  }
  return {
    "snapshot-id": snapshot["snapshot-id"],
    "parent-snapshot-id": input.expectedSnapshotId,
    "timestamp-ms": snapshot["timestamp-ms"],
    "schema-id": snapshot["schema-id"],
    "manifest-list": input.manifestPath,
    summary: {
      operation: "append",
      "added-data-files": String(input.manifest.files.length),
      "added-records": String(
        input.manifest.files.reduce((sum, file) => sum + file.recordCount, 0),
      ),
    },
  };
}

function equalityKey(row: Row, columns: string[]): string {
  return stableStringify(
    columns.map((column) => {
      if (!(column in row)) {
        throw new LakeqlError(
          "LAKEQL_UNKNOWN_COLUMN",
          `Unknown Iceberg equality delete column ${column}`,
          {
            column,
          },
        );
      }
      return jsonSafeValue(row[column]);
    }),
  );
}

function appendManifestPath(
  metadataPath: string,
  jobId: string | undefined,
  snapshotId: number,
): string {
  const slash = metadataPath.lastIndexOf("/");
  const prefix = slash === -1 ? "" : `${metadataPath.slice(0, slash + 1)}`;
  return `${prefix}${jobId ?? "append"}-${snapshotId}.manifest.json`;
}

function maxSequenceNumber(metadata: MetadataFile): number {
  let max = 0;
  for (const snapshot of metadata.snapshots) {
    for (const manifest of snapshotManifests(snapshot)) {
      for (const file of manifest.files) max = Math.max(max, file.sequenceNumber);
    }
  }
  return max;
}

function supportedIcebergDeleteFiles(
  deleteFiles: ManifestDeleteFile[] | undefined,
): IcebergDeleteFile[] {
  const supported: IcebergDeleteFile[] = [];
  for (const deleteFile of deleteFiles ?? []) {
    if (deleteFile.content === "position-delete" || deleteFile.content === "equality-delete") {
      supported.push({ content: deleteFile.content, path: deleteFile.path });
    }
  }
  return supported;
}

function unsupportedIcebergDeleteFiles(
  deleteFiles: ManifestDeleteFile[] | undefined,
): ManifestDeleteFile[] {
  const supported = new Set(["position-delete", "equality-delete"]);
  return (deleteFiles ?? []).filter((deleteFile) => !supported.has(deleteFile.content));
}

function cloneMetadata(metadata: MetadataFile): MetadataFile {
  const cloned: MetadataFile = {
    "format-version": metadata["format-version"],
    "table-uuid": metadata["table-uuid"],
    location: metadata.location,
    "current-snapshot-id": metadata["current-snapshot-id"],
    schemas: metadata.schemas.map((schema) => ({
      "schema-id": schema["schema-id"],
      fields: schema.fields.map((field) => ({ ...field })),
    })),
    snapshots: metadata.snapshots.map((snapshot) => {
      const cloned: Snapshot = {
        "snapshot-id": snapshot["snapshot-id"],
        "timestamp-ms": snapshot["timestamp-ms"],
        "schema-id": snapshot["schema-id"],
      };
      if (snapshot["manifest-list"] !== undefined)
        cloned["manifest-list"] = snapshot["manifest-list"];
      if (snapshot.manifests !== undefined) {
        cloned.manifests = snapshot.manifests.map(cloneManifestOrReference);
      }
      return cloned;
    }),
  };
  if (metadata.refs) cloned.refs = cloneRefs(metadata.refs);
  if (metadata["partition-specs"]) {
    cloned["partition-specs"] = metadata["partition-specs"].map((spec) => ({
      "spec-id": spec["spec-id"],
      fields: spec.fields.map((field) => ({ ...field })),
    }));
  }
  if (metadata["sort-orders"]) {
    cloned["sort-orders"] = metadata["sort-orders"].map((order) => ({
      "order-id": order["order-id"],
      fields: order.fields.map((field) => field),
    }));
  }
  return cloned;
}

function cloneRefs(
  refs: Record<string, { type: "branch" | "tag"; "snapshot-id": number }>,
): Record<string, { type: "branch" | "tag"; "snapshot-id": number }> {
  const out: Record<string, { type: "branch" | "tag"; "snapshot-id": number }> = {};
  for (const [name, ref] of Object.entries(refs)) {
    out[name] = { type: ref.type, "snapshot-id": ref["snapshot-id"] };
  }
  return out;
}

function cloneManifest(manifest: Manifest): Manifest {
  const cloned: Manifest = {
    path: manifest.path,
    files: manifest.files.map((file) => {
      const cloned: ManifestFile = {
        path: file.path,
        sequenceNumber: file.sequenceNumber,
        partition: sortStringRecord(file.partition ?? {}),
        recordCount: file.recordCount,
      };
      if (file.fileSizeInBytes !== undefined) cloned.fileSizeInBytes = file.fileSizeInBytes;
      if (file.deleteFiles !== undefined) {
        cloned.deleteFiles = file.deleteFiles.map((deleteFile) => ({ ...deleteFile }));
      }
      return cloned;
    }),
  };
  if (manifest.deleteFiles !== undefined) {
    cloned.deleteFiles = manifest.deleteFiles.map((deleteFile) => ({ ...deleteFile }));
  }
  return cloned;
}

function cloneManifestOrReference(manifest: Manifest): Manifest {
  if (!Array.isArray((manifest as { files?: unknown }).files)) {
    return { path: manifest.path } as Manifest;
  }
  return cloneManifest(manifest);
}

function snapshotManifests(snapshot: Snapshot): Manifest[] {
  return snapshot.manifests ?? [];
}

function sortStringRecord(record: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of Object.keys(record).sort()) out[key] = record[key] ?? "";
  return out;
}

function validateListTablesResponse(value: unknown): IcebergTableIdentifier[] {
  if (!isRecord(value) || !Array.isArray(value.identifiers)) {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Invalid Iceberg REST list tables response");
  }
  return value.identifiers.map((identifier) => {
    if (
      !isRecord(identifier) ||
      !Array.isArray(identifier.namespace) ||
      !identifier.namespace.every((part) => typeof part === "string") ||
      typeof identifier.name !== "string"
    ) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Invalid Iceberg REST table identifier");
    }
    return { namespace: identifier.namespace, name: identifier.name };
  });
}

function validateMetadata(value: unknown): MetadataFile {
  if (!isRecord(value))
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg metadata must be an object");
  if (value["format-version"] !== 1 && value["format-version"] !== 2) {
    throw new LakeqlError(
      "LAKEQL_CATALOG_ERROR",
      "Only Iceberg format-version 1 and 2 metadata is supported for reads",
      {
        formatVersion: value["format-version"],
      },
    );
  }
  if (!Array.isArray(value.snapshots) || !Array.isArray(value.schemas)) {
    throw new LakeqlError(
      "LAKEQL_CATALOG_ERROR",
      "Iceberg metadata is missing snapshots or schemas",
    );
  }
  if (!isMetadataFile(value)) {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg metadata has invalid required fields");
  }
  rejectUnsupportedMetadataFeatures(value);
  return value;
}

function rejectUnsupportedMetadataFeatures(metadata: Record<string, unknown>): void {
  rejectAdvertisedFeatureFlags(metadata);
  rejectUnsupportedPartitionTransforms(metadata["partition-specs"]);
  rejectUnsupportedSortOrders(metadata["sort-orders"]);
}

function rejectAdvertisedFeatureFlags(metadata: Record<string, unknown>): void {
  for (const key of ["features", "table-features", "format-version-features"]) {
    const features = metadata[key];
    if (features === undefined || (Array.isArray(features) && features.length === 0)) continue;
    throw new LakeqlError(
      "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
      "Iceberg metadata advertises unsupported table-format features",
      {
        featureProperty: key,
        features,
      },
    );
  }
}

function rejectUnsupportedPartitionTransforms(partitionSpecs: unknown): void {
  if (partitionSpecs === undefined) return;
  if (!Array.isArray(partitionSpecs)) {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg partition-specs must be an array");
  }
  for (const spec of partitionSpecs) {
    if (!isRecord(spec) || !Array.isArray(spec.fields)) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg partition spec is invalid");
    }
    for (const field of spec.fields) {
      if (!isRecord(field) || typeof field.transform !== "string") {
        throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg partition field is invalid");
      }
      if (field.transform !== "identity" && field.transform !== "void") {
        throw new LakeqlError(
          "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
          "Iceberg partition transform is not supported for strict planning",
          {
            specId: spec["spec-id"],
            fieldName: field.name,
            transform: field.transform,
          },
        );
      }
    }
  }
}

function rejectUnsupportedSortOrders(sortOrders: unknown): void {
  if (sortOrders === undefined) return;
  if (!Array.isArray(sortOrders)) {
    throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg sort-orders must be an array");
  }
  for (const order of sortOrders) {
    if (!isRecord(order) || !Array.isArray(order.fields)) {
      throw new LakeqlError("LAKEQL_CATALOG_ERROR", "Iceberg sort order is invalid");
    }
    if (order.fields.length > 0) {
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
        "Iceberg sorted table metadata is not supported for strict planning",
        {
          orderId: order["order-id"],
          fields: order.fields,
        },
      );
    }
  }
}

function isMetadataFile(value: unknown): value is MetadataFile {
  if (!isRecord(value)) return false;
  return (
    (value["format-version"] === 1 || value["format-version"] === 2) &&
    typeof value["table-uuid"] === "string" &&
    typeof value.location === "string" &&
    typeof value["current-snapshot-id"] === "number" &&
    Array.isArray(value.refs) === false &&
    Array.isArray(value.schemas) &&
    Array.isArray(value.snapshots)
  );
}

function projectedIds(fields: IcebergField[], select: string[] | undefined): number[] {
  if (!select) return fields.map((field) => field.id).sort((a, b) => a - b);
  return select
    .map((name) => {
      const field = fields.find((candidate) => candidate.name === name);
      if (!field) {
        throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown Iceberg column ${name}`, {
          column: name,
        });
      }
      return field.sourceId ?? field.id;
    })
    .sort((a, b) => a - b);
}

function partitionMayMatch(expr: Expr | undefined, partition: Record<string, string>): boolean {
  if (!expr) return true;
  const columns = new Set<string>();
  collectColumns(expr, columns);
  if (columns.size === 0 || [...columns].some((column) => !(column in partition))) return true;
  try {
    return matches(expr, partition);
  } catch (cause) {
    if (cause instanceof LakeqlError && cause.code === "LAKEQL_TYPE_ERROR") return true;
    throw cause;
  }
}

function collectColumns(expr: Expr, columns: Set<string>): void {
  switch (expr.kind) {
    case "column":
      columns.add(expr.name);
      return;
    case "literal":
      return;
    case "compare":
      collectColumns(expr.left, columns);
      collectColumns(expr.right, columns);
      return;
    case "in":
      collectColumns(expr.target, columns);
      for (const value of expr.values) collectColumns(value, columns);
      return;
    case "between":
      collectColumns(expr.target, columns);
      collectColumns(expr.low, columns);
      collectColumns(expr.high, columns);
      return;
    case "null-check":
      collectColumns(expr.target, columns);
      return;
    case "logical":
      for (const operand of expr.operands) collectColumns(operand, columns);
      return;
    case "not":
      collectColumns(expr.operand, columns);
      return;
    case "like":
      collectColumns(expr.target, columns);
      return;
    case "call":
      for (const arg of expr.args) collectColumns(arg, columns);
      return;
    case "arithmetic":
      collectColumns(expr.left, columns);
      collectColumns(expr.right, columns);
      return;
    case "case":
      for (const branch of expr.whens) {
        collectColumns(branch.when, columns);
        collectColumns(branch.value, columns);
      }
      if (expr.else !== undefined) collectColumns(expr.else, columns);
      return;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
