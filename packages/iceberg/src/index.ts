import { type Expr, LaQLError, matches, type ObjectStore } from "@laql/core";

export const PACKAGE = "@laql/iceberg" as const;

export type IcebergReadMode = "strict" | "ignore-deletes" | "ignore-unsupported-deletes";

export interface LoadIcebergTableOptions {
  store: ObjectStore;
  metadataPath: string;
}

export interface PlanIcebergFilesOptions {
  snapshotId?: number;
  asOfTimestampMs?: number;
  ref?: string;
  where?: Expr;
  select?: string[];
  readMode?: IcebergReadMode;
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
}

export interface IcebergPlan {
  snapshotId: number;
  schemaId: number;
  manifestsRead: number;
  manifestsSkipped: number;
  filesPlanned: number;
  filesSkipped: number;
  files: PlannedIcebergFile[];
}

interface MetadataFile {
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

interface Snapshot {
  "snapshot-id": number;
  "timestamp-ms": number;
  "schema-id": number;
  manifests: Manifest[];
}

interface Manifest {
  path: string;
  files: ManifestFile[];
}

interface ManifestFile {
  path: string;
  sequenceNumber: number;
  partition?: Record<string, string>;
  recordCount: number;
  deleteFiles?: { content: string; path: string }[];
}

export class IcebergTable {
  readonly metadataPath: string;
  readonly metadata: MetadataFile;

  constructor(metadataPath: string, metadata: MetadataFile) {
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

  planFiles(options: PlanIcebergFilesOptions = {}): IcebergPlan {
    const snapshot = this.snapshot(options);
    const fields = this.schema(snapshot["schema-id"]);
    const projectedFieldIds = projectedIds(fields, options.select);
    const readMode = options.readMode ?? "strict";
    const files: PlannedIcebergFile[] = [];
    let manifestsSkipped = 0;
    let filesSkipped = 0;

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
        if (file.deleteFiles && file.deleteFiles.length > 0 && readMode === "strict") {
          throw new LaQLError(
            "LAQL_UNSUPPORTED_DELETE_FILES",
            "Snapshot contains delete files unsupported by this planner mode",
            { path: file.path, deleteFiles: file.deleteFiles },
          );
        }
        files.push({
          path: file.path,
          sequenceNumber: file.sequenceNumber,
          partition: file.partition ?? {},
          recordCount: file.recordCount,
          projectedFieldIds,
          snapshotId: snapshot["snapshot-id"],
        });
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
      files,
    };
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
    return new IcebergTable(options.metadataPath, validateMetadata(JSON.parse(text)));
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
