import { LakeqlError } from "./errors.js";
import type { TaskInput } from "./query.js";
import type { ObjectStore } from "./store.js";
import type { Bookmark, BookmarkQuery } from "./types.js";
import { isWindowExprSnapshot } from "./window-snapshot.js";

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
  outputs?: OutputManifestEntry[];
}

export interface CheckpointAdapter {
  get(taskId: string): Promise<TaskCheckpoint | undefined>;
  put(checkpoint: TaskCheckpoint): Promise<void>;
  list(jobId?: string): AsyncIterable<TaskCheckpoint>;
}

export interface TaskTransitionInput {
  taskId: string;
  nextState: TaskState;
  idempotencyKey: string;
  nowMs: number;
  staleTimeoutMs?: number;
  output?: OutputManifestEntry;
  outputs?: OutputManifestEntry[];
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
  query?: BookmarkQuery;
  position: BookmarkPosition;
  writeState?: Bookmark["writeState"];
  operatorState?: Bookmark["operatorState"];
}

export function createTaskManifest(input: {
  jobId: string;
  snapshot?: string;
  tasks: TaskInput[];
  outputRole?: TaskManifestTask["outputRole"];
}): TaskManifest {
  const normalizedTasks = input.tasks.map(normalizeTaskInput);
  const snapshot = input.snapshot ?? snapshotFromTasks(normalizedTasks);
  const tasks = normalizedTasks.map((task, index) => ({
    id: taskId(input.jobId, index, task),
    input: task,
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

export async function createOutputManifestFromCheckpoints(input: {
  jobId: string;
  planFingerprint: string;
  checkpoints: CheckpointAdapter;
}): Promise<OutputManifest> {
  const entries: OutputManifestEntry[] = [];
  for await (const checkpoint of input.checkpoints.list(input.jobId)) {
    if (checkpoint.outputs !== undefined) {
      entries.push(...checkpoint.outputs);
    } else if (checkpoint.output !== undefined) {
      entries.push(checkpoint.output);
    }
  }
  entries.sort(
    (left, right) =>
      left.taskId.localeCompare(right.taskId) || left.outputPath.localeCompare(right.outputPath),
  );
  return createOutputManifest({
    jobId: input.jobId,
    planFingerprint: input.planFingerprint,
    entries,
  });
}

export async function writeOutputManifest(
  store: ObjectStore,
  path: string,
  manifest: OutputManifest,
): Promise<{ path: string; byteSize: number; etag?: string }> {
  const bytes = new TextEncoder().encode(`${stableStringify(manifest)}\n`);
  await store.put(path, bytes, { contentType: "application/json" });
  const head = await store.head(path);
  const result: { path: string; byteSize: number; etag?: string } = {
    path,
    byteSize: head?.size ?? bytes.byteLength,
  };
  if (head?.etag !== undefined) result.etag = head.etag;
  return result;
}

export async function readOutputManifest(
  store: ObjectStore,
  path: string,
): Promise<OutputManifest> {
  const bytes = await store.get(path);
  if (!bytes) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No output manifest at ${path}`, { path });
  }
  try {
    return parseOutputManifest(JSON.parse(new TextDecoder().decode(bytes)));
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output manifest is invalid JSON", {
      path,
      cause,
    });
  }
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
  const bookmark: Bookmark = {
    version: 1,
    planFingerprint: init.planFingerprint,
    snapshot: init.snapshot,
    position,
  };
  if (init.query !== undefined) bookmark.query = normalizeBookmarkQuery(init.query);
  if (init.writeState !== undefined)
    bookmark.writeState = normalizeBookmarkWriteState(init.writeState);
  if (init.operatorState !== undefined) {
    bookmark.operatorState = normalizeBookmarkOperatorState(init.operatorState);
  }
  return bookmark;
}

export function assertBookmarkMatches(bookmark: Bookmark, planFingerprint: string): void {
  if (bookmark.planFingerprint !== planFingerprint) {
    throw new LakeqlError(
      "LAKEQL_BOOKMARK_STALE",
      "Bookmark does not match the current query plan",
      {
        bookmarkPlanFingerprint: bookmark.planFingerprint,
        planFingerprint,
      },
    );
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
  if (!constantTimeEqual(signaturePart, expected)) {
    throwInvalidBookmark("Pagination token signature is invalid");
  }
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

export async function advanceTaskCheckpoint(
  checkpoints: CheckpointAdapter,
  input: TaskTransitionInput,
): Promise<TaskCheckpoint> {
  const checkpoint = transitionTaskCheckpoint(await checkpoints.get(input.taskId), input);
  await checkpoints.put(checkpoint);
  return checkpoint;
}

export function transitionTaskCheckpoint(
  existing: TaskCheckpoint | undefined,
  input: TaskTransitionInput,
): TaskCheckpoint {
  if (!existing) return createCheckpoint(input);
  if (existing.taskId !== input.taskId) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Task checkpoint id does not match transition",
      {
        existingTaskId: existing.taskId,
        taskId: input.taskId,
      },
    );
  }
  if (existing.state === input.nextState && existing.idempotencyKey === input.idempotencyKey) {
    return cloneCheckpoint(existing);
  }
  const stale =
    input.staleTimeoutMs !== undefined && input.nowMs - existing.updatedAtMs > input.staleTimeoutMs;
  if (existing.idempotencyKey !== input.idempotencyKey && !stale) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Task transition idempotency key mismatch", {
      taskId: input.taskId,
      existingIdempotencyKey: existing.idempotencyKey,
      idempotencyKey: input.idempotencyKey,
    });
  }
  if (!transitionAllowed(existing.state, input.nextState, stale)) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Task state transition is not allowed", {
      taskId: input.taskId,
      from: existing.state,
      to: input.nextState,
    });
  }
  const checkpoint = createCheckpoint(input);
  if (
    checkpoint.output === undefined &&
    checkpoint.outputs === undefined &&
    existing.output !== undefined &&
    stateRank(input.nextState) > stateRank(existing.state)
  ) {
    checkpoint.output = normalizeOutputEntry(existing.output);
  }
  if (
    checkpoint.output === undefined &&
    checkpoint.outputs === undefined &&
    existing.outputs !== undefined &&
    stateRank(input.nextState) > stateRank(existing.state)
  ) {
    checkpoint.outputs = existing.outputs.map(normalizeOutputEntry);
  }
  return checkpoint;
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
  if (task.size !== undefined) normalized.size = task.size;
  if (task.rowGroupCount !== undefined) normalized.rowGroupCount = task.rowGroupCount;
  if (task.projectedColumns !== undefined)
    normalized.projectedColumns = [...task.projectedColumns].sort();
  if (task.residualPredicate !== undefined) normalized.residualPredicate = task.residualPredicate;
  if (task.window !== undefined) {
    normalized.window =
      task.window.available === true
        ? {
            topology: task.window.topology,
            available: true,
            bucketCount: task.window.bucketCount,
            partitionBy: task.window.partitionBy,
          }
        : {
            topology: task.window.topology,
            available: false,
            bucketCount: 1,
            reason: task.window.reason,
          };
  }
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

function parseOutputManifest(value: unknown): OutputManifest {
  if (
    !isRecord(value) ||
    value.version !== 1 ||
    typeof value.jobId !== "string" ||
    value.jobId.length === 0 ||
    typeof value.planFingerprint !== "string" ||
    value.planFingerprint.length === 0 ||
    !Array.isArray(value.entries)
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output manifest has invalid required fields");
  }
  return createOutputManifest({
    jobId: value.jobId,
    planFingerprint: value.planFingerprint,
    entries: value.entries.map(parseOutputEntry),
  });
}

function parseOutputEntry(value: unknown): OutputManifestEntry {
  if (
    !isRecord(value) ||
    typeof value.taskId !== "string" ||
    value.taskId.length === 0 ||
    typeof value.outputPath !== "string" ||
    value.outputPath.length === 0 ||
    !isStringRecord(value.partitionValues) ||
    !isNonNegativeInteger(value.rowCount) ||
    !isNonNegativeInteger(value.byteSize)
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output manifest entry is invalid");
  }
  const entry: OutputManifestEntry = {
    taskId: value.taskId,
    outputPath: value.outputPath,
    partitionValues: value.partitionValues,
    rowCount: value.rowCount,
    byteSize: value.byteSize,
  };
  if (value.contentHash !== undefined) {
    if (typeof value.contentHash !== "string" || value.contentHash.length === 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output manifest content hash is invalid");
    }
    entry.contentHash = value.contentHash;
  }
  if (value.etag !== undefined) {
    if (typeof value.etag !== "string" || value.etag.length === 0) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output manifest etag is invalid");
    }
    entry.etag = value.etag;
  }
  if (value.iceberg !== undefined) entry.iceberg = parseOutputIcebergMetadata(value.iceberg);
  return entry;
}

function parseOutputIcebergMetadata(value: unknown): NonNullable<OutputManifestEntry["iceberg"]> {
  if (
    !isRecord(value) ||
    !isNonNegativeInteger(value.recordCount) ||
    !isNonNegativeInteger(value.fileSizeInBytes) ||
    !isStringRecord(value.partitionValues)
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output manifest Iceberg metadata is invalid");
  }
  return {
    recordCount: value.recordCount,
    fileSizeInBytes: value.fileSizeInBytes,
    partitionValues: value.partitionValues,
  };
}

function snapshotFromTasks(tasks: TaskInput[]): string {
  return fingerprint(
    tasks.map((task) => ({
      path: task.path,
      etag: task.etag ?? null,
      size: task.size ?? null,
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

function createCheckpoint(input: TaskTransitionInput): TaskCheckpoint {
  const checkpoint: TaskCheckpoint = {
    taskId: input.taskId,
    state: input.nextState,
    idempotencyKey: input.idempotencyKey,
    updatedAtMs: input.nowMs,
  };
  if (input.output !== undefined) checkpoint.output = normalizeOutputEntry(input.output);
  if (input.outputs !== undefined) {
    checkpoint.outputs = input.outputs.map(normalizeOutputEntry);
  }
  return checkpoint;
}

function transitionAllowed(from: TaskState, to: TaskState, stale: boolean): boolean {
  if (stale && to === "running") return true;
  return stateRank(to) === stateRank(from) + 1;
}

function stateRank(state: TaskState): number {
  const order: TaskState[] = [
    "planned",
    "running",
    "output-written",
    "manifest-recorded",
    "complete",
  ];
  return order.indexOf(state);
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

function sortValueRecord<T>(record: Record<string, T>): Record<string, T> {
  const out: Record<string, T> = {};
  for (const key of Object.keys(record).sort()) {
    const value = record[key];
    if (value !== undefined) out[key] = value;
  }
  return out;
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return isRecord(value) && Object.values(value).every((inner) => typeof inner === "string");
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

function constantTimeEqual(left: string, right: string): boolean {
  const length = Math.max(left.length, right.length);
  let diff = left.length ^ right.length;
  for (let index = 0; index < length; index += 1) {
    diff |= (left.charCodeAt(index) || 0) ^ (right.charCodeAt(index) || 0);
  }
  return diff === 0;
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
  const bookmark: Bookmark = {
    version: 1,
    planFingerprint: value.planFingerprint,
    snapshot: value.snapshot,
    position: parseBookmarkPosition(position, fileIndex, rowGroup, rowOffset),
  };
  if (value.query !== undefined) bookmark.query = parseBookmarkQuery(value.query);
  if (value.writeState !== undefined)
    bookmark.writeState = parseBookmarkWriteState(value.writeState);
  if (value.operatorState !== undefined) {
    bookmark.operatorState = parseBookmarkOperatorState(value.operatorState);
  }
  return bookmark;
}

function normalizeBookmarkWriteState(
  state: NonNullable<Bookmark["writeState"]>,
): NonNullable<Bookmark["writeState"]> {
  const normalized: NonNullable<Bookmark["writeState"]> = {};
  if (state.taskState !== undefined) normalized.taskState = state.taskState;
  if (state.idempotencyKey !== undefined) normalized.idempotencyKey = state.idempotencyKey;
  if (state.multipart !== undefined) {
    normalized.multipart = {
      uploadId: state.multipart.uploadId,
      path: state.multipart.path,
      parts: state.multipart.parts
        .map((part) => ({
          partNumber: part.partNumber,
          etag: part.etag,
          byteSize: part.byteSize,
        }))
        .sort((left, right) => left.partNumber - right.partNumber),
    };
  }
  return normalized;
}

function normalizeBookmarkOperatorState(
  state: NonNullable<Bookmark["operatorState"]>,
): NonNullable<Bookmark["operatorState"]> {
  const normalized: NonNullable<Bookmark["operatorState"]> = {};
  if (state.limitEmitted !== undefined) normalized.limitEmitted = state.limitEmitted;
  if (state.groupBy !== undefined) normalized.groupBy = cloneInlineOrSpillState(state.groupBy);
  if (state.topK !== undefined) normalized.topK = cloneInlineOrSpillState(state.topK);
  if (state.sort !== undefined) normalized.sort = cloneInlineOrSpillState(state.sort);
  if (state.sketches !== undefined) {
    normalized.sketches = {};
    for (const [key, value] of Object.entries(state.sketches).sort(([a], [b]) =>
      a.localeCompare(b),
    )) {
      normalized.sketches[key] = cloneBytes(value);
    }
  }
  return normalized;
}

function cloneInlineOrSpillState(
  value: Uint8Array | { spillRef: string },
): Uint8Array | { spillRef: string } {
  return value instanceof Uint8Array ? cloneBytes(value) : { spillRef: value.spillRef };
}

function cloneBytes(bytes: Uint8Array): Uint8Array {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

function normalizeBookmarkQuery(query: BookmarkQuery): BookmarkQuery {
  const normalized: BookmarkQuery = { source: query.source };
  if (query.select !== undefined) normalized.select = [...query.select];
  if (query.projections !== undefined) normalized.projections = sortValueRecord(query.projections);
  if (query.where !== undefined) normalized.where = query.where;
  if (query.distinct !== undefined) normalized.distinct = query.distinct;
  if (query.windows !== undefined) normalized.windows = sortValueRecord(query.windows);
  if (query.qualify !== undefined) normalized.qualify = query.qualify;
  if (query.orderBy !== undefined) {
    normalized.orderBy = query.orderBy.map((term) => {
      const normalizedTerm: NonNullable<BookmarkQuery["orderBy"]>[number] = {
        column: term.column,
      };
      if (term.direction !== undefined) normalizedTerm.direction = term.direction;
      if (term.nulls !== undefined) normalizedTerm.nulls = term.nulls;
      return normalizedTerm;
    });
  }
  if (query.limit !== undefined) normalized.limit = query.limit;
  if (query.offset !== undefined) normalized.offset = query.offset;
  if (query.batchSize !== undefined) normalized.batchSize = query.batchSize;
  if (query.hive !== undefined) normalized.hive = query.hive;
  return normalized;
}

function parseBookmarkOperatorState(value: unknown): NonNullable<Bookmark["operatorState"]> {
  if (!isRecord(value)) throwInvalidBookmark("Bookmark operator state is invalid");
  const state: NonNullable<Bookmark["operatorState"]> = {};
  if (value.limitEmitted !== undefined) {
    state.limitEmitted = parseBookmarkNonNegativeInteger(value.limitEmitted, "limitEmitted");
  }
  if (value.groupBy !== undefined) state.groupBy = parseInlineOrSpillState(value.groupBy);
  if (value.topK !== undefined) state.topK = parseInlineOrSpillState(value.topK);
  if (value.sort !== undefined) state.sort = parseInlineOrSpillState(value.sort);
  if (value.sketches !== undefined) {
    if (!isRecord(value.sketches)) throwInvalidBookmark("Bookmark sketches state is invalid");
    state.sketches = {};
    for (const [key, inner] of Object.entries(value.sketches)) {
      if (typeof key !== "string" || key.length === 0) {
        throwInvalidBookmark("Bookmark sketches state is invalid");
      }
      state.sketches[key] = parseBase64UrlBytes(inner, "Bookmark sketches state is invalid");
    }
  }
  return state;
}

function parseInlineOrSpillState(value: unknown): Uint8Array | { spillRef: string } {
  if (typeof value === "string")
    return parseBase64UrlBytes(value, "Bookmark operator state is invalid");
  if (!isRecord(value) || typeof value.spillRef !== "string" || value.spillRef.length === 0) {
    throwInvalidBookmark("Bookmark operator state is invalid");
  }
  return { spillRef: value.spillRef };
}

function parseBookmarkWriteState(value: unknown): NonNullable<Bookmark["writeState"]> {
  if (!isRecord(value)) throwInvalidBookmark("Bookmark write state is invalid");
  const state: NonNullable<Bookmark["writeState"]> = {};
  if (value.taskState !== undefined) {
    if (!isTaskState(value.taskState)) throwInvalidBookmark("Bookmark write task state is invalid");
    state.taskState = value.taskState;
  }
  if (value.idempotencyKey !== undefined) {
    if (typeof value.idempotencyKey !== "string" || value.idempotencyKey.length === 0) {
      throwInvalidBookmark("Bookmark write idempotency key is invalid");
    }
    state.idempotencyKey = value.idempotencyKey;
  }
  if (value.multipart !== undefined) state.multipart = parseMultipartWriteState(value.multipart);
  return normalizeBookmarkWriteState(state);
}

function parseMultipartWriteState(
  value: unknown,
): NonNullable<NonNullable<Bookmark["writeState"]>["multipart"]> {
  if (!isRecord(value)) throwInvalidBookmark("Bookmark multipart state is invalid");
  if (typeof value.uploadId !== "string" || value.uploadId.length === 0) {
    throwInvalidBookmark("Bookmark multipart upload id is invalid");
  }
  if (typeof value.path !== "string" || value.path.length === 0) {
    throwInvalidBookmark("Bookmark multipart path is invalid");
  }
  if (!Array.isArray(value.parts)) throwInvalidBookmark("Bookmark multipart parts are invalid");
  return {
    uploadId: value.uploadId,
    path: value.path,
    parts: value.parts.map(parseMultipartPart),
  };
}

function parseMultipartPart(
  value: unknown,
): NonNullable<NonNullable<Bookmark["writeState"]>["multipart"]>["parts"][number] {
  if (!isRecord(value)) throwInvalidBookmark("Bookmark multipart part is invalid");
  if (!isPositiveInteger(value.partNumber)) {
    throwInvalidBookmark("Bookmark multipart part number is invalid");
  }
  if (typeof value.etag !== "string" || value.etag.length === 0) {
    throwInvalidBookmark("Bookmark multipart part etag is invalid");
  }
  if (!isNonNegativeInteger(value.byteSize)) {
    throwInvalidBookmark("Bookmark multipart part byte size is invalid");
  }
  return {
    partNumber: value.partNumber,
    etag: value.etag,
    byteSize: value.byteSize,
  };
}

function parseBase64UrlBytes(value: unknown, message: string): Uint8Array {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]*$/u.test(value)) throwInvalidBookmark(message);
  try {
    return base64UrlDecode(value);
  } catch {
    throwInvalidBookmark(message);
  }
}

function parseBookmarkQuery(value: unknown): BookmarkQuery {
  if (!isRecord(value) || typeof value.source !== "string" || value.source.length === 0) {
    throwInvalidBookmark("Bookmark query is invalid");
  }
  const query: BookmarkQuery = { source: value.source };
  if (value.select !== undefined) query.select = parseStringArray(value.select, "select");
  if (value.projections !== undefined) {
    if (!isRecord(value.projections)) throwInvalidBookmark("Bookmark projections are invalid");
    query.projections = {};
    for (const [key, expr] of Object.entries(value.projections)) {
      if (typeof key !== "string" || key.length === 0) {
        throwInvalidBookmark("Bookmark projections are invalid");
      }
      query.projections[key] = parseBookmarkExpr(expr);
    }
  }
  if (value.where !== undefined) query.where = parseBookmarkExpr(value.where);
  if (value.distinct !== undefined) {
    if (typeof value.distinct !== "boolean") throwInvalidBookmark("Bookmark distinct is invalid");
    query.distinct = value.distinct;
  }
  if (value.windows !== undefined) {
    if (!isRecord(value.windows)) throwInvalidBookmark("Bookmark windows are invalid");
    query.windows = {};
    for (const [key, expr] of Object.entries(value.windows)) {
      if (typeof key !== "string" || key.length === 0 || !isWindowExprSnapshot(expr)) {
        throwInvalidBookmark("Bookmark windows are invalid");
      }
      query.windows[key] = expr as unknown as NonNullable<BookmarkQuery["windows"]>[string];
    }
  }
  if (value.qualify !== undefined) query.qualify = parseBookmarkExpr(value.qualify);
  if (value.orderBy !== undefined) query.orderBy = parseBookmarkOrderBy(value.orderBy);
  if (value.limit !== undefined)
    query.limit = parseBookmarkNonNegativeInteger(value.limit, "limit");
  if (value.offset !== undefined)
    query.offset = parseBookmarkNonNegativeInteger(value.offset, "offset");
  if (value.batchSize !== undefined) {
    if (!isPositiveInteger(value.batchSize)) throwInvalidBookmark("Bookmark batch size is invalid");
    query.batchSize = value.batchSize;
  }
  if (value.hive !== undefined) {
    if (typeof value.hive !== "boolean") throwInvalidBookmark("Bookmark hive flag is invalid");
    query.hive = value.hive;
  }
  return query;
}

function parseStringArray(value: unknown, field: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throwInvalidBookmark(`Bookmark ${field} is invalid`);
  }
  return [...value];
}

function parseBookmarkOrderBy(value: unknown): NonNullable<BookmarkQuery["orderBy"]> {
  if (!Array.isArray(value)) throwInvalidBookmark("Bookmark orderBy is invalid");
  return value.map((term) => {
    if (!isRecord(term) || typeof term.column !== "string" || term.column.length === 0) {
      throwInvalidBookmark("Bookmark orderBy is invalid");
    }
    const parsed: NonNullable<BookmarkQuery["orderBy"]>[number] = { column: term.column };
    if (term.direction !== undefined) {
      if (term.direction !== "asc" && term.direction !== "desc") {
        throwInvalidBookmark("Bookmark orderBy direction is invalid");
      }
      parsed.direction = term.direction;
    }
    if (term.nulls !== undefined) {
      if (term.nulls !== "first" && term.nulls !== "last") {
        throwInvalidBookmark("Bookmark orderBy nulls is invalid");
      }
      parsed.nulls = term.nulls;
    }
    return parsed;
  });
}

function parseBookmarkExpr(value: unknown): NonNullable<BookmarkQuery["where"]> {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throwInvalidBookmark("Bookmark where expression is invalid");
  }
  return value as unknown as NonNullable<BookmarkQuery["where"]>;
}

function parseBookmarkNonNegativeInteger(value: unknown, field: string): number {
  if (!isNonNegativeInteger(value)) throwInvalidBookmark(`Bookmark ${field} is invalid`);
  return value;
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
  if (checkpoint.outputs !== undefined)
    clone.outputs = checkpoint.outputs.map(normalizeOutputEntry);
  return clone;
}

function throwInvalidBookmark(message: string): never {
  throw new LakeqlError("LAKEQL_BOOKMARK_INVALID", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

function isTaskState(value: unknown): value is NonNullable<Bookmark["writeState"]>["taskState"] {
  return (
    value === "planned" ||
    value === "running" ||
    value === "output-written" ||
    value === "manifest-recorded" ||
    value === "complete"
  );
}
