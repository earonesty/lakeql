import { LaQLError } from "./errors.js";
import type { TaskInput } from "./query.js";
import type { Bookmark } from "./types.js";

export interface TaskManifestTask {
  id: string;
  input: TaskInput;
  outputRole: "rows" | "data-file" | "manifest";
}

export interface TaskManifest {
  version: 1;
  jobId: string;
  planFingerprint: string;
  snapshot: string;
  tasks: TaskManifestTask[];
}

export interface OutputManifestEntry {
  taskId: string;
  outputPath: string;
  partitionValues: Record<string, string>;
  rowCount: number;
  byteSize: number;
  contentHash?: string;
  etag?: string;
  iceberg?: {
    recordCount: number;
    fileSizeInBytes: number;
    partitionValues: Record<string, string>;
  };
}

export interface OutputManifest {
  version: 1;
  jobId: string;
  planFingerprint: string;
  entries: OutputManifestEntry[];
}

export type TaskState = "planned" | "running" | "output-written" | "manifest-recorded" | "complete";

export interface TaskCheckpoint {
  taskId: string;
  state: TaskState;
  idempotencyKey: string;
  updatedAtMs: number;
  output?: OutputManifestEntry;
}

export interface CheckpointAdapter {
  get(taskId: string): Promise<TaskCheckpoint | undefined>;
  put(checkpoint: TaskCheckpoint): Promise<void>;
  list(jobId?: string): AsyncIterable<TaskCheckpoint>;
}

export interface BookmarkPosition {
  fileIndex: number;
  rowGroup: number;
  rowOffset: number;
  taskId?: string;
  outputManifestCursor?: number;
}

export interface BookmarkInit {
  planFingerprint: string;
  snapshot: string;
  position: BookmarkPosition;
}

export function createTaskManifest(input: {
  jobId: string;
  snapshot?: string;
  tasks: TaskInput[];
  outputRole?: TaskManifestTask["outputRole"];
}): TaskManifest {
  const snapshot = input.snapshot ?? snapshotFromTasks(input.tasks);
  const tasks = input.tasks.map((task, index) => ({
    id: taskId(input.jobId, index, task),
    input: normalizeTaskInput(task),
    outputRole: input.outputRole ?? "rows",
  }));
  const planFingerprint = fingerprint({
    version: 1,
    snapshot,
    tasks: tasks.map((task) => task.input),
  });
  return {
    version: 1,
    jobId: input.jobId,
    planFingerprint,
    snapshot,
    tasks,
  };
}

export function createOutputManifest(input: {
  jobId: string;
  planFingerprint: string;
  entries: OutputManifestEntry[];
}): OutputManifest {
  return {
    version: 1,
    jobId: input.jobId,
    planFingerprint: input.planFingerprint,
    entries: input.entries.map(normalizeOutputEntry),
  };
}

export function createBookmark(init: BookmarkInit): Bookmark {
  const position: Bookmark["position"] = {
    fileIndex: init.position.fileIndex,
    rowGroup: init.position.rowGroup,
    rowOffset: init.position.rowOffset,
  };
  if (init.position.taskId !== undefined) position.taskId = init.position.taskId;
  if (init.position.outputManifestCursor !== undefined) {
    position.outputManifestCursor = init.position.outputManifestCursor;
  }
  return {
    version: 1,
    planFingerprint: init.planFingerprint,
    snapshot: init.snapshot,
    position,
  };
}

export function assertBookmarkMatches(bookmark: Bookmark, planFingerprint: string): void {
  if (bookmark.planFingerprint !== planFingerprint) {
    throw new LaQLError("LAQL_BOOKMARK_STALE", "Bookmark does not match the current query plan", {
      bookmarkPlanFingerprint: bookmark.planFingerprint,
      planFingerprint,
    });
  }
}

export async function signPaginationToken(
  bookmark: Bookmark,
  secret: string | Uint8Array,
): Promise<string> {
  const payload = stableStringify(bookmark);
  const signature = await hmac(payload, secret);
  return `${base64UrlEncode(new TextEncoder().encode(payload))}.${signature}`;
}

export async function verifyPaginationToken(
  token: string,
  secret: string | Uint8Array,
): Promise<Bookmark> {
  const [payloadPart, signaturePart, extra] = token.split(".");
  if (!payloadPart || !signaturePart || extra !== undefined) {
    throwInvalidBookmark("Pagination token must contain a payload and signature");
  }
  const payloadBytes = base64UrlDecode(payloadPart);
  const payload = new TextDecoder().decode(payloadBytes);
  const expected = await hmac(payload, secret);
  if (signaturePart !== expected) throwInvalidBookmark("Pagination token signature is invalid");
  const parsed: unknown = JSON.parse(payload);
  return parseBookmark(parsed);
}

export class MemoryCheckpointAdapter implements CheckpointAdapter {
  private readonly checkpoints = new Map<string, TaskCheckpoint>();
  private readonly taskJobs = new Map<string, string>();

  async get(taskId: string): Promise<TaskCheckpoint | undefined> {
    const checkpoint = this.checkpoints.get(taskId);
    return checkpoint ? cloneCheckpoint(checkpoint) : undefined;
  }

  async put(checkpoint: TaskCheckpoint): Promise<void> {
    this.checkpoints.set(checkpoint.taskId, cloneCheckpoint(checkpoint));
    this.taskJobs.set(checkpoint.taskId, jobIdFromTaskId(checkpoint.taskId));
  }

  async *list(jobId?: string): AsyncIterable<TaskCheckpoint> {
    const checkpoints = [...this.checkpoints.values()].sort((a, b) =>
      a.taskId.localeCompare(b.taskId),
    );
    for (const checkpoint of checkpoints) {
      if (jobId !== undefined && this.taskJobs.get(checkpoint.taskId) !== jobId) continue;
      yield cloneCheckpoint(checkpoint);
    }
  }
}

export function memoryCheckpointAdapter(): MemoryCheckpointAdapter {
  return new MemoryCheckpointAdapter();
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(toStableJson(value));
}

export function fingerprint(value: unknown): string {
  return `fp_${fnv1a64(stableStringify(value)).toString(16).padStart(16, "0")}`;
}

function normalizeTaskInput(task: TaskInput): TaskInput {
  const normalized: TaskInput = {
    path: task.path,
    rowGroupRanges: [...task.rowGroupRanges]
      .map((range) => ({ start: range.start, end: range.end }))
      .sort((a, b) => a.start - b.start || a.end - b.end),
    partitionValues: sortRecord(task.partitionValues),
  };
  if (task.etag !== undefined) normalized.etag = task.etag;
  if (task.projectedColumns !== undefined)
    normalized.projectedColumns = [...task.projectedColumns].sort();
  if (task.residualPredicate !== undefined) normalized.residualPredicate = task.residualPredicate;
  return normalized;
}

function normalizeOutputEntry(entry: OutputManifestEntry): OutputManifestEntry {
  const normalized: OutputManifestEntry = {
    taskId: entry.taskId,
    outputPath: entry.outputPath,
    partitionValues: sortRecord(entry.partitionValues),
    rowCount: entry.rowCount,
    byteSize: entry.byteSize,
  };
  if (entry.contentHash !== undefined) normalized.contentHash = entry.contentHash;
  if (entry.etag !== undefined) normalized.etag = entry.etag;
  if (entry.iceberg !== undefined) {
    normalized.iceberg = {
      recordCount: entry.iceberg.recordCount,
      fileSizeInBytes: entry.iceberg.fileSizeInBytes,
      partitionValues: sortRecord(entry.iceberg.partitionValues),
    };
  }
  return normalized;
}

function snapshotFromTasks(tasks: TaskInput[]): string {
  return fingerprint(
    tasks.map((task) => ({
      path: task.path,
      etag: task.etag ?? null,
    })),
  );
}

function taskId(jobId: string, index: number, task: TaskInput): string {
  return `${jobId}-task-${String(index).padStart(6, "0")}-${fingerprint(task).slice(3, 11)}`;
}

function jobIdFromTaskId(taskId: string): string {
  const marker = "-task-";
  const index = taskId.indexOf(marker);
  return index === -1 ? "" : taskId.slice(0, index);
}

function toStableJson(value: unknown): unknown {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    if (typeof value === "number" && !Number.isFinite(value)) return String(value);
    return value;
  }
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return base64UrlEncode(value);
  if (Array.isArray(value)) return value.map(toStableJson);
  if (typeof value === "object" && value !== null) {
    const out: Record<string, unknown> = {};
    for (const [key, inner] of Object.entries(value).sort(([a], [b]) => a.localeCompare(b))) {
      if (inner !== undefined) out[key] = toStableJson(inner);
    }
    return out;
  }
  return String(value);
}

function sortRecord(record: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of Object.keys(record).sort()) out[key] = record[key] ?? "";
  return out;
}

function fnv1a64(input: string): bigint {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  for (const byte of new TextEncoder().encode(input)) {
    hash ^= BigInt(byte);
    hash = BigInt.asUintN(64, hash * prime);
  }
  return hash;
}

async function hmac(payload: string, secret: string | Uint8Array): Promise<string> {
  const keyBytes = typeof secret === "string" ? new TextEncoder().encode(secret) : secret;
  const key = await crypto.subtle.importKey(
    "raw",
    bytesToArrayBuffer(keyBytes),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    bytesToArrayBuffer(new TextEncoder().encode(payload)),
  );
  return base64UrlEncode(new Uint8Array(signature));
}

function bytesToArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(copy).set(bytes);
  return copy;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function base64UrlDecode(value: string): Uint8Array {
  const padded = value
    .replaceAll("-", "+")
    .replaceAll("_", "/")
    .padEnd(Math.ceil(value.length / 4) * 4, "=");
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function parseBookmark(value: unknown): Bookmark {
  if (!isRecord(value) || value.version !== 1) throwInvalidBookmark("Bookmark version is invalid");
  if (typeof value.planFingerprint !== "string" || typeof value.snapshot !== "string") {
    throwInvalidBookmark("Bookmark identity is invalid");
  }
  const position = value.position;
  const fileIndex = isRecord(position) ? position.fileIndex : undefined;
  const rowGroup = isRecord(position) ? position.rowGroup : undefined;
  const rowOffset = isRecord(position) ? position.rowOffset : undefined;
  if (
    !isRecord(position) ||
    !isNonNegativeInteger(fileIndex) ||
    !isNonNegativeInteger(rowGroup) ||
    !isNonNegativeInteger(rowOffset)
  ) {
    throwInvalidBookmark("Bookmark position is invalid");
  }
  return {
    version: 1,
    planFingerprint: value.planFingerprint,
    snapshot: value.snapshot,
    position: parseBookmarkPosition(position, fileIndex, rowGroup, rowOffset),
  };
}

function parseBookmarkPosition(
  position: Record<string, unknown>,
  fileIndex: number,
  rowGroup: number,
  rowOffset: number,
): Bookmark["position"] {
  const parsed: Bookmark["position"] = {
    fileIndex,
    rowGroup,
    rowOffset,
  };
  if (position.taskId !== undefined) {
    if (typeof position.taskId !== "string") throwInvalidBookmark("Bookmark task id is invalid");
    parsed.taskId = position.taskId;
  }
  if (position.outputManifestCursor !== undefined) {
    if (!isNonNegativeInteger(position.outputManifestCursor)) {
      throwInvalidBookmark("Bookmark output manifest cursor is invalid");
    }
    parsed.outputManifestCursor = position.outputManifestCursor;
  }
  return parsed;
}

function cloneCheckpoint(checkpoint: TaskCheckpoint): TaskCheckpoint {
  const clone: TaskCheckpoint = {
    taskId: checkpoint.taskId,
    state: checkpoint.state,
    idempotencyKey: checkpoint.idempotencyKey,
    updatedAtMs: checkpoint.updatedAtMs,
  };
  if (checkpoint.output !== undefined) clone.output = normalizeOutputEntry(checkpoint.output);
  return clone;
}

function throwInvalidBookmark(message: string): never {
  throw new LaQLError("LAQL_BOOKMARK_INVALID", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}
