import { LakeqlError } from "./errors.js";
import { jsonSafeValue } from "./evaluator.js";
import { stableStringify } from "./manifest.js";
import type { SpillAdapter, SpillRef } from "./runtime.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { isWindowExprSnapshot } from "./window-snapshot.js";

export interface WindowOptions {
  operatorState?: Uint8Array | WindowOperatorState | { spillRef: string };
  spill?: SpillAdapter;
  spillId?: string;
}

export interface WindowResult {
  rows: Row[];
  operatorState: Uint8Array;
  operatorSpill?: SpillRef;
}

export interface WindowOperatorState {
  version: 1;
  windows: Record<string, WindowExpr>;
  rows?: Record<string, WindowSnapshotValue>[];
  runs?: WindowRunState[];
}

export type WindowSnapshotValue = string | number | boolean | null;

export type WindowRunState =
  | { rows: Record<string, WindowSnapshotValue>[] }
  | { spillRef: string; rowCount: number; byteSize: number };

export function serializeWindowOperatorState(state: WindowOperatorState): Uint8Array {
  return new TextEncoder().encode(stableStringify(state));
}

export function deserializeWindowOperatorState(
  bytes: Uint8Array | WindowOperatorState,
): WindowOperatorState {
  if (!(bytes instanceof Uint8Array)) return validateWindowOperatorState(bytes);
  return validateWindowOperatorState(JSON.parse(new TextDecoder().decode(bytes)));
}

export async function windowRowsFromState(
  windows: Record<string, WindowExpr>,
  options: WindowOptions,
): Promise<Row[]> {
  const state =
    isSpilledOperatorState(options.operatorState) && options.spill !== undefined
      ? await options.spill.read(options.operatorState.spillRef)
      : options.operatorState;
  if (isSpilledOperatorState(state)) {
    throw new LakeqlError(
      "LAKEQL_BOOKMARK_INVALID",
      "Window spill state requires a spill adapter",
      {
        spillRef: state.spillRef,
      },
    );
  }
  if (state === undefined) return [];
  const snapshot = deserializeWindowOperatorState(state);
  if (stableStringify(snapshot.windows) !== stableStringify(windows)) {
    throw new LakeqlError("LAKEQL_BOOKMARK_STALE", "Window operator state does not match request", {
      stateWindows: snapshot.windows,
      windows,
    });
  }
  if (snapshot.rows !== undefined) return snapshot.rows.map((row) => ({ ...row }));
  const rows: Row[] = [];
  for (const run of snapshot.runs ?? []) {
    if ("rows" in run) {
      rows.push(...run.rows.map((row) => ({ ...row })));
      continue;
    }
    if (options.spill === undefined) {
      throw new LakeqlError(
        "LAKEQL_BOOKMARK_INVALID",
        "Window run spill state requires a spill adapter",
        {
          spillRef: run.spillRef,
        },
      );
    }
    rows.push(...deserializeWindowRunRows(await options.spill.read(run.spillRef)));
  }
  return rows;
}

export function windowOperatorState(
  windows: Record<string, WindowExpr>,
  rows: Row[],
): WindowOperatorState {
  return {
    version: 1,
    windows,
    rows: rows.map(snapshotRecord),
  };
}

export async function windowOperatorStateFromRuns(
  windows: Record<string, WindowExpr>,
  runs: readonly Row[][],
  spill: SpillAdapter | undefined,
  spillId: string,
): Promise<WindowOperatorState> {
  const runStates: WindowRunState[] = [];
  for (const [index, run] of runs.entries()) {
    const rows = run.map(snapshotRecord);
    if (spill === undefined) {
      runStates.push({ rows });
      continue;
    }
    const bytes = serializeWindowRunRows(rows);
    const ref = await spill.write(`${spillId}-run-${String(index).padStart(6, "0")}`, bytes);
    runStates.push({ spillRef: ref.id, rowCount: rows.length, byteSize: ref.byteSize });
  }
  return { version: 1, windows, runs: runStates };
}

function validateWindowOperatorState(value: unknown): WindowOperatorState {
  if (!isWindowOperatorState(value)) {
    throw new LakeqlError("LAKEQL_BOOKMARK_INVALID", "Window operator state is invalid");
  }
  return {
    version: 1,
    windows: value.windows,
    ...(value.rows === undefined ? {} : { rows: value.rows.map((row) => ({ ...row })) }),
    ...(value.runs === undefined ? {} : { runs: value.runs.map(cloneWindowRunState) }),
  };
}

function isWindowOperatorState(value: unknown): value is WindowOperatorState {
  return (
    isRecord(value) &&
    value.version === 1 &&
    isRecord(value.windows) &&
    Object.values(value.windows).every(isWindowExprSnapshot) &&
    ((Array.isArray(value.rows) &&
      value.rows.every(isWindowSnapshotRow) &&
      value.runs === undefined) ||
      (value.rows === undefined && Array.isArray(value.runs) && value.runs.every(isWindowRunState)))
  );
}

function serializeWindowRunRows(rows: Record<string, WindowSnapshotValue>[]): Uint8Array {
  return new TextEncoder().encode(stableStringify(rows));
}

function deserializeWindowRunRows(bytes: Uint8Array): Row[] {
  const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
  if (!Array.isArray(parsed) || !parsed.every(isWindowSnapshotRow)) {
    throw new LakeqlError("LAKEQL_BOOKMARK_INVALID", "Window run state is invalid");
  }
  return parsed.map((row) => ({ ...row }));
}

function snapshotRecord(record: Record<string, unknown>): Record<string, WindowSnapshotValue> {
  const out: Record<string, WindowSnapshotValue> = {};
  for (const [key, value] of Object.entries(record)) out[key] = snapshotValue(value);
  return out;
}

function snapshotValue(value: unknown): WindowSnapshotValue {
  const safe = jsonSafeValue(value);
  if (
    safe === null ||
    typeof safe === "string" ||
    typeof safe === "number" ||
    typeof safe === "boolean"
  ) {
    return safe;
  }
  throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window operator state values must be JSON scalars", {
    value: safe,
  });
}

function isWindowSnapshotRow(value: unknown): value is Record<string, WindowSnapshotValue> {
  return isRecord(value) && Object.values(value).every(isWindowSnapshotValue);
}

function isWindowRunState(value: unknown): value is WindowRunState {
  if (!isRecord(value)) return false;
  const rowCount = value.rowCount;
  const byteSize = value.byteSize;
  return (
    (Array.isArray(value.rows) && value.rows.every(isWindowSnapshotRow)) ||
    (typeof value.spillRef === "string" &&
      value.spillRef.length > 0 &&
      typeof rowCount === "number" &&
      Number.isInteger(rowCount) &&
      rowCount >= 0 &&
      typeof byteSize === "number" &&
      Number.isFinite(byteSize) &&
      byteSize >= 0)
  );
}

function cloneWindowRunState(run: WindowRunState): WindowRunState {
  if ("rows" in run) return { rows: run.rows.map((row) => ({ ...row })) };
  return { spillRef: run.spillRef, rowCount: run.rowCount, byteSize: run.byteSize };
}

function isWindowSnapshotValue(value: unknown): value is WindowSnapshotValue {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  );
}

function isSpilledOperatorState(value: unknown): value is { spillRef: string } {
  return isRecord(value) && typeof value.spillRef === "string";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
