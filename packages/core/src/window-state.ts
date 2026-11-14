import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { readCheckpointSpill } from "./checkpoint-spill-read.js";
import { LakeqlError } from "./errors.js";
import { jsonSafeValue } from "./evaluator.js";
import { stableStringify } from "./manifest.js";
import type { MemoryScope } from "./memory-scope.js";
import { encodeOwnedCheckpoint, equalStableData } from "./owned-checkpoint-encoding.js";
import { parseOwnedJson } from "./owned-json-parse.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import { snapshotOwnedRows } from "./owned-snapshot-rows.js";
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

export function serializeWindowOperatorState(
  state: WindowOperatorState,
  scope?: MemoryScope,
): Uint8Array {
  return encodeWindowState(state, scope);
}

function encodeWindowState(value: unknown, scope?: MemoryScope): Uint8Array {
  if (scope === undefined) return new TextEncoder().encode(stableStringify(value));
  const owner = encodeOwnedCheckpoint(value, scope);
  try {
    return owner.value;
  } finally {
    owner.release();
  }
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
  scope?: MemoryScope,
  check?: () => void,
): Promise<Row[]> {
  if (scope !== undefined) return ownedWindowRowsFromState(windows, options, scope, check);
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

async function ownedWindowRowsFromState(
  windows: Record<string, WindowExpr>,
  options: WindowOptions,
  scope: MemoryScope,
  check?: () => void,
): Promise<Row[]> {
  const parsing = scope.childScope();
  try {
    check?.();
    let state: unknown = options.operatorState;
    if (isSpilledOperatorState(state)) {
      if (options.spill === undefined)
        throw new LakeqlError(
          "LAKEQL_BOOKMARK_INVALID",
          "Window spill state requires a spill adapter",
        );
      state = await readCheckpointSpill(options.spill, state.spillRef, parsing);
      check?.();
    }
    const rows = scope.allocate(c.array, "restored window rows", () => [] as Row[]).value;
    if (state === undefined) return rows;
    const decoded =
      state instanceof Uint8Array
        ? parsing.adopt(parseOwnedJson(state, parsing)).value
        : parsing.adopt(detachOwnedRow({ state }, scope.budget)).value.state;
    if (!isWindowOperatorState(decoded))
      throw new LakeqlError("LAKEQL_BOOKMARK_INVALID", "Window operator state is invalid");
    if (!equalStableData(decoded.windows, windows, parsing))
      throw new LakeqlError(
        "LAKEQL_BOOKMARK_STALE",
        "Window operator state does not match request",
      );
    const append = (input: readonly Row[]) => {
      for (const row of input) {
        const owner = detachOwnedRow(row, scope.budget);
        try {
          scope.adopt(owner);
        } catch (error) {
          owner.release();
          throw error;
        }
        scope.allocate(c.arraySlot, "restored window row reference", () => rows.push(owner.value));
      }
    };
    if (decoded.rows !== undefined) append(decoded.rows);
    for (const run of decoded.runs ?? []) {
      check?.();
      if ("rows" in run) {
        append(run.rows);
        continue;
      }
      if (options.spill === undefined)
        throw new LakeqlError(
          "LAKEQL_BOOKMARK_INVALID",
          "Window run spill state requires a spill adapter",
        );
      const runScope = parsing.childScope();
      try {
        const bytes = await readCheckpointSpill(options.spill, run.spillRef, runScope);
        check?.();
        const values = runScope.adopt(parseOwnedJson(bytes, runScope)).value;
        if (!Array.isArray(values) || !values.every(isWindowSnapshotRow))
          throw new LakeqlError("LAKEQL_BOOKMARK_INVALID", "Window run state is invalid");
        append(values);
      } finally {
        runScope.dispose();
      }
    }
    return rows;
  } finally {
    parsing.dispose();
  }
}

export function windowOperatorState(
  windows: Record<string, WindowExpr>,
  rows: Row[],
  scope?: MemoryScope,
): WindowOperatorState {
  const create = (): WindowOperatorState => ({
    version: 1,
    windows,
    rows:
      scope === undefined
        ? rows.map(snapshotRecord)
        : scope.adopt(snapshotOwnedRows(rows, scope)).value,
  });
  return scope === undefined
    ? create()
    : scope.allocate(c.object + 3 * c.property, "window checkpoint state", create).value;
}

export async function windowOperatorStateFromRuns(
  windows: Record<string, WindowExpr>,
  runs: readonly Row[][],
  spill: SpillAdapter | undefined,
  spillId: string,
  scope?: MemoryScope,
  check?: () => void,
): Promise<WindowOperatorState> {
  const runStates: WindowRunState[] =
    scope === undefined
      ? []
      : scope.allocate(
          c.array + runs.length * c.arraySlot,
          "window checkpoint runs",
          () => [] as WindowRunState[],
        ).value;
  for (const [index, run] of runs.entries()) {
    const temporary = spill === undefined ? undefined : scope?.childScope();
    const snapshotScope = temporary ?? scope;
    try {
      const rows =
        snapshotScope === undefined
          ? run.map(snapshotRecord)
          : snapshotScope.adopt(snapshotOwnedRows(run, snapshotScope)).value;
      if (spill === undefined) {
        runStates.push(
          scope === undefined
            ? { rows }
            : scope.allocate(c.object + c.property, "window inline checkpoint run", () => ({
                rows,
              })).value,
        );
        continue;
      }
      const bytes = encodeWindowState(rows, temporary);
      const ref = await spill.write(`${spillId}-run-${String(index).padStart(6, "0")}`, bytes);
      check?.();
      const create = () => ({ spillRef: ref.id, rowCount: rows.length, byteSize: ref.byteSize });
      runStates.push(
        scope === undefined
          ? create()
          : scope.allocate(
              c.object + 3 * c.property + c.string + ref.id.length * c.stringCodeUnit,
              "window checkpoint spill reference",
              create,
            ).value,
      );
    } finally {
      temporary?.dispose();
    }
  }
  const create = (): WindowOperatorState => ({ version: 1, windows, runs: runStates });
  return scope === undefined
    ? create()
    : scope.allocate(c.object + 3 * c.property, "window checkpoint state", create).value;
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
    everyRecordValue(value.windows, isWindowExprSnapshot) &&
    ((Array.isArray(value.rows) &&
      value.rows.every(isWindowSnapshotRow) &&
      value.runs === undefined) ||
      (value.rows === undefined && Array.isArray(value.runs) && value.runs.every(isWindowRunState)))
  );
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
  return isRecord(value) && everyRecordValue(value, isWindowSnapshotValue);
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

function everyRecordValue(
  record: Record<string, unknown>,
  predicate: (value: unknown) => boolean,
): boolean {
  for (const key in record) if (Object.hasOwn(record, key) && !predicate(record[key])) return false;
  return true;
}
