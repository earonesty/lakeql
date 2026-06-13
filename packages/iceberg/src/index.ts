import {
  type Expr,
  jsonSafeValue,
  LaQLError,
  matches,
  type ObjectStore,
  type OutputManifest,
  type Row,
  stableStringify,
} from "@laql/core";

export const PACKAGE = "@laql/iceberg" as const;

export type IcebergReadMode = "strict" | "ignore-deletes" | "ignore-unsupported-deletes";

export interface LoadIcebergTableOptions {
  store: ObjectStore;
  metadataPath: string;
}

export interface LoadIcebergTableFromObjectStoreOptions {
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

export interface LoadIcebergTableFromRestOptions extends IcebergRestCatalogOptions {
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
  catalog?: IcebergCommitCatalog;
}

export interface IcebergAppendOutputManifestOptions
  extends Omit<IcebergAppendOptions, "files" | "jobId"> {
  manifest: OutputManifest;
}

export interface IcebergCommitCatalog {
  commitAppend(input: IcebergCommitInput): Promise<boolean>;
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

export interface ScanPlannedIcebergRowsOptions {
  plan: IcebergPlan | PlannedIcebergFile[];
  readDataFile(file: PlannedIcebergFile): Promise<Row[] | AsyncIterable<Row[]>>;
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
}

export interface Snapshot {
  "snapshot-id": number;
  "timestamp-ms": number;
  "schema-id": number;
  manifests: Manifest[];
}

export interface Manifest {
  path: string;
  files: ManifestFile[];
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
        throw new LaQLError("LAQL_CATALOG_ERROR", `Unknown Iceberg ref ${options.ref}`, {
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
        throw new LaQLError("LAQL_CATALOG_ERROR", "No Iceberg snapshot at requested timestamp", {
          asOfTimestampMs: options.asOfTimestampMs,
        });
      }
      return snapshot;
    }
    return this.snapshotById(this.metadata["current-snapshot-id"]);
  }

  schema(schemaId: number): IcebergField[] {
    const schema = this.metadata.schemas.find((candidate) => candidate["schema-id"] === schemaId);
    if (!schema) {
      throw new LaQLError("LAQL_CATALOG_ERROR", `Unknown Iceberg schema ${schemaId}`, { schemaId });
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

    for (const manifest of snapshot.manifests) {
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
          throw new LaQLError(
            "LAQL_UNSUPPORTED_DELETE_FILES",
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
      manifestsRead: snapshot.manifests.length - manifestsSkipped,
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
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg append requires at least one file");
    }
    const currentSnapshot = this.snapshot();
    const nextSnapshotId =
      Math.max(...this.metadata.snapshots.map((snapshot) => snapshot["snapshot-id"])) + 1;
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
      manifests: [...currentSnapshot.manifests.map(cloneManifest), manifest],
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
    const committed = await catalog.commitAppend({
      store: this.store,
      currentMetadataPath: this.metadataPath,
      nextMetadataPath,
      expectedSnapshotId: currentSnapshot["snapshot-id"],
      nextSnapshotId,
      manifestPath,
      manifest,
      metadata,
    });
    if (!committed) {
      throw new LaQLError("LAQL_ICEBERG_COMMIT_CONFLICT", "Iceberg append commit conflict", {
        metadataPath: this.metadataPath,
        expectedSnapshotId: currentSnapshot["snapshot-id"],
        nextSnapshotId,
      });
    }

    return {
      snapshotId: nextSnapshotId,
      previousSnapshotId: currentSnapshot["snapshot-id"],
      metadataPath: nextMetadataPath,
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
        throw new LaQLError(
          "LAQL_VALIDATION_ERROR",
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
      ...(options.catalog !== undefined ? { catalog: options.catalog } : {}),
    });
  }

  private snapshotById(snapshotId: number): Snapshot {
    const snapshot = this.metadata.snapshots.find(
      (candidate) => candidate["snapshot-id"] === snapshotId,
    );
    if (!snapshot) {
      throw new LaQLError("LAQL_CATALOG_ERROR", `Unknown Iceberg snapshot ${snapshotId}`, {
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

export class ObjectStoreIcebergCommitCatalog implements IcebergCommitCatalog {
  async commitAppend(input: IcebergCommitInput): Promise<boolean> {
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
    await input.store.put(
      metadataVersionHintPath(input.nextMetadataPath),
      new TextEncoder().encode(`${input.nextSnapshotId}\n`),
      { contentType: "text/plain" },
    );
    return true;
  }
}

export class IcebergRestCatalog implements IcebergCommitCatalog {
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
      throw new LaQLError("LAQL_CATALOG_ERROR", "Invalid Iceberg REST load table response", {
        url: this.tableUrl(),
      });
    }
    return new IcebergTable(
      store,
      response["metadata-location"],
      await hydrateMetadataManifests(store, validateMetadata(response.metadata)),
    );
  }

  async commitAppend(input: IcebergCommitInput): Promise<boolean> {
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
    if (response.status === 409) return false;
    if (!response.ok) {
      throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg REST table commit failed", {
        url: this.tableUrl(),
        status: response.status,
        statusText: response.statusText,
      });
    }
    return true;
  }

  private async requestJson(url: string, init: RequestInit): Promise<unknown> {
    const response = await this.fetchFn(url, {
      ...init,
      headers: this.headers(init.body !== undefined),
    });
    if (!response.ok) {
      throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg REST catalog request failed", {
        url,
        status: response.status,
        statusText: response.statusText,
      });
    }
    try {
      return await response.json();
    } catch (cause) {
      throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg REST catalog response is not JSON", {
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
    return restCatalogUrl(this.url, [
      "v1",
      ...this.prefix,
      "namespaces",
      this.namespace.join("\u001f"),
      "tables",
      this.table,
    ]);
  }
}

export function icebergRestCatalog(options: IcebergRestCatalogOptions): IcebergRestCatalog {
  return new IcebergRestCatalog(options);
}

export async function loadIcebergTable(options: LoadIcebergTableOptions): Promise<IcebergTable> {
  const bytes = await options.store.get(options.metadataPath);
  if (!bytes) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No object at ${options.metadataPath}`, {
      path: options.metadataPath,
    });
  }
  const text = new TextDecoder().decode(bytes);
  try {
    return new IcebergTable(
      options.store,
      options.metadataPath,
      await hydrateMetadataManifests(options.store, validateMetadata(JSON.parse(text))),
    );
  } catch (cause) {
    if (cause instanceof LaQLError) throw cause;
    throw new LaQLError(
      "LAQL_CATALOG_ERROR",
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
  const tableLocation = trimTrailingSlash(options.tableLocation);
  const metadataPrefix = `${tableLocation}/metadata/`;
  const versionHintPath = `${metadataPrefix}version-hint.text`;
  const hintedVersion = await readVersionHint(options.store, versionHintPath);
  const metadataPath =
    hintedVersion === undefined
      ? await latestMetadataPathFromList(options.store, metadataPrefix)
      : `${metadataPrefix}v${hintedVersion}.metadata.json`;
  return await loadIcebergTable({ store: options.store, metadataPath });
}

export async function loadIcebergTableFromRest(
  options: LoadIcebergTableFromRestOptions,
): Promise<IcebergTable> {
  return await icebergRestCatalog(options).loadTable(options.store);
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
      throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg equality delete requires columns");
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
  for (const file of files) {
    const deletes = await decodedDeletesForFile(file, options);
    const data = await options.readDataFile(file);
    let rowOffset = 0;
    for await (const rows of rowBatches(data)) {
      const visibleRows = hasDeletes(deletes)
        ? applyIcebergDeletes({
            dataFilePath: file.path,
            rows,
            rowOffset,
            ...deletes,
          })
        : rows;
      rowOffset += rows.length;
      if (visibleRows.length > 0) yield visibleRows;
    }
  }
}

async function decodedDeletesForFile(
  file: PlannedIcebergFile,
  options: ScanPlannedIcebergRowsOptions,
): Promise<DecodedIcebergDeletes> {
  const out: DecodedIcebergDeletes = {};
  for (const deleteFile of file.deleteFiles ?? []) {
    const decoded = await options.readDeleteFile(deleteFile, file);
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

async function* rowBatches(rows: Row[] | AsyncIterable<Row[]>): AsyncIterable<Row[]> {
  if (isAsyncIterable(rows)) {
    yield* rows;
  } else {
    yield rows;
  }
}

function isAsyncIterable(value: unknown): value is AsyncIterable<Row[]> {
  return (
    typeof value === "object" &&
    value !== null &&
    Symbol.asyncIterator in value &&
    typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
  );
}

function validateDeletePosition(position: number, path: string): number {
  if (!Number.isInteger(position) || position < 0) {
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg delete position must be non-negative", {
      path,
      position,
    });
  }
  return position;
}

function nextMetadataPathFor(metadataPath: string, snapshotId: number): string {
  const slash = metadataPath.lastIndexOf("/");
  const prefix = slash === -1 ? "" : `${metadataPath.slice(0, slash + 1)}`;
  return `${prefix}v${snapshotId}.metadata.json`;
}

function metadataVersionHintPath(metadataPath: string): string {
  const slash = metadataPath.lastIndexOf("/");
  const prefix = slash === -1 ? "" : `${metadataPath.slice(0, slash + 1)}`;
  return `${prefix}version-hint.text`;
}

async function hydrateMetadataManifests(
  store: ObjectStore,
  metadata: MetadataFile,
): Promise<MetadataFile> {
  const hydrated = cloneMetadata(metadata);
  for (const snapshot of hydrated.snapshots) {
    snapshot.manifests = await Promise.all(
      snapshot.manifests.map(async (manifest) => {
        if (Array.isArray(manifest.files)) return manifest;
        return await readManifest(store, manifest.path);
      }),
    );
  }
  return hydrated;
}

async function readManifest(store: ObjectStore, path: string): Promise<Manifest> {
  const bytes = await store.get(path);
  if (!bytes) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", `No Iceberg manifest at ${path}`, { path });
  }
  try {
    return validateManifest(JSON.parse(new TextDecoder().decode(bytes)), path);
  } catch (cause) {
    if (cause instanceof LaQLError) throw cause;
    throw new LaQLError("LAQL_CATALOG_ERROR", `Invalid Iceberg manifest at ${path}`, {
      path,
      cause,
    });
  }
}

function validateManifest(value: unknown, path: string): Manifest {
  if (!isRecord(value) || typeof value.path !== "string" || !Array.isArray(value.files)) {
    throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg manifest has invalid required fields", {
      path,
    });
  }
  return value as unknown as Manifest;
}

async function readVersionHint(store: ObjectStore, path: string): Promise<number | undefined> {
  const bytes = await store.get(path);
  if (!bytes) return undefined;
  const text = new TextDecoder().decode(bytes).trim();
  const version = Number(text);
  if (!Number.isInteger(version) || version < 0) {
    throw new LaQLError("LAQL_CATALOG_ERROR", "Invalid Iceberg version hint", {
      path,
      versionHint: text,
    });
  }
  return version;
}

async function latestMetadataPathFromList(
  store: ObjectStore,
  metadataPrefix: string,
): Promise<string> {
  let latest: { path: string; version: number } | undefined;
  for await (const object of store.list(metadataPrefix)) {
    const name = object.path.slice(metadataPrefix.length);
    const match = /^v(\d+)\.metadata\.json$/u.exec(name);
    if (!match) continue;
    const version = Number(match[1]);
    if (!Number.isSafeInteger(version)) continue;
    if (latest === undefined || version > latest.version) latest = { path: object.path, version };
  }
  if (latest === undefined) {
    throw new LaQLError("LAQL_OBJECT_NOT_FOUND", "No Iceberg metadata files found", {
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
    throw new LaQLError("LAQL_VALIDATION_ERROR", "Iceberg REST namespace is required");
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
    throw new LaQLError("LAQL_VALIDATION_ERROR", `Iceberg REST ${field} is required`);
  }
  return trimmed;
}

function restAppendSnapshot(input: IcebergCommitInput): Record<string, unknown> {
  const snapshot = input.metadata.snapshots.at(-1);
  if (snapshot === undefined) {
    throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg append metadata is missing next snapshot");
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
        throw new LaQLError(
          "LAQL_UNKNOWN_COLUMN",
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
    for (const manifest of snapshot.manifests) {
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
    if (
      deleteFile.content === "position-delete" ||
      deleteFile.content === "equality-delete" ||
      deleteFile.content === "deletion-vector"
    ) {
      supported.push({ content: deleteFile.content, path: deleteFile.path });
    }
  }
  return supported;
}

function unsupportedIcebergDeleteFiles(
  deleteFiles: ManifestDeleteFile[] | undefined,
): ManifestDeleteFile[] {
  const supported = new Set(["position-delete", "equality-delete", "deletion-vector"]);
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
    snapshots: metadata.snapshots.map((snapshot) => ({
      "snapshot-id": snapshot["snapshot-id"],
      "timestamp-ms": snapshot["timestamp-ms"],
      "schema-id": snapshot["schema-id"],
      manifests: snapshot.manifests.map(cloneManifestOrReference),
    })),
  };
  if (metadata.refs) cloned.refs = cloneRefs(metadata.refs);
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
  return {
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
}

function cloneManifestOrReference(manifest: Manifest): Manifest {
  if (!Array.isArray((manifest as { files?: unknown }).files)) {
    return { path: manifest.path } as Manifest;
  }
  return cloneManifest(manifest);
}

function sortStringRecord(record: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of Object.keys(record).sort()) out[key] = record[key] ?? "";
  return out;
}

function validateMetadata(value: unknown): MetadataFile {
  if (!isRecord(value))
    throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg metadata must be an object");
  if (value["format-version"] !== 2) {
    throw new LaQLError(
      "LAQL_CATALOG_ERROR",
      "Only Iceberg format-version 2 metadata is supported",
      {
        formatVersion: value["format-version"],
      },
    );
  }
  if (!Array.isArray(value.snapshots) || !Array.isArray(value.schemas)) {
    throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg metadata is missing snapshots or schemas");
  }
  if (!isMetadataFile(value)) {
    throw new LaQLError("LAQL_CATALOG_ERROR", "Iceberg metadata has invalid required fields");
  }
  return value;
}

function isMetadataFile(value: unknown): value is MetadataFile {
  if (!isRecord(value)) return false;
  return (
    value["format-version"] === 2 &&
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
        throw new LaQLError("LAQL_UNKNOWN_COLUMN", `Unknown Iceberg column ${name}`, {
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
  return matches(expr, partition);
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
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
