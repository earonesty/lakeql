import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import type { MemoryScope } from "./memory-scope.js";
import { internStableDataKey } from "./owned-checkpoint-encoding.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import { snapshotScalarWithAdmission } from "./owned-snapshot-rows.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import type { WindowExecutionOptions } from "./window-backend.js";
import { withWindowResultScope } from "./window-value-ownership.js";

export interface WindowWorkUnitRow {
  ordinal: WindowWorkUnitOrdinal;
  row: Row;
}

export interface WindowWorkUnitOrdinal {
  task: number;
  row: number;
}

export interface WindowWorkUnitBucket {
  bucket: number;
  rows: WindowWorkUnitRow[];
}

export interface WindowWorkUnitPartial {
  bucketCount: number;
  buckets: WindowWorkUnitBucket[];
}

export interface WindowWorkUnitEvaluationOptions extends WindowExecutionOptions {
  bucketCount?: number;
}

export function partitionWindowWorkUnitRows(
  rows: readonly WindowWorkUnitRow[],
  windows: Record<string, WindowExpr>,
  bucketCount: number,
  options: WindowExecutionOptions = {},
): WindowWorkUnitPartial {
  return withWindowResultScope(options, (scope) => {
    validateBucketCount(bucketCount);
    const scratch = scope.childScope();
    const buckets = scratch.allocate(
      c.object,
      "window partition directory",
      () => new Map<number, WindowWorkUnitBucket>(),
    ).value;
    for (const entry of rows) {
      const bucket = windowPartitionBucket(entry.row, windows, bucketCount, scope);
      let target = buckets.get(bucket);
      if (target === undefined) {
        scratch.allocate(c.property, "window partition directory entry", () => undefined);
        target = scope.allocate(
          c.object + 2 * c.property + c.array,
          "window partition bucket",
          () => ({ bucket, rows: [] as WindowWorkUnitRow[] }),
        ).value;
        buckets.set(bucket, target);
      }
      const detached = detachOwnedRow(entry.row, scope.budget, scope);
      try {
        scope.adopt(detached);
      } catch (error) {
        detached.release();
        throw error;
      }
      target.rows.push(
        scope.allocate(c.arraySlot + 2 * c.object + 4 * c.property, "window partition row", () => ({
          ordinal: { ...entry.ordinal },
          row: detached.value,
        })).value,
      );
    }
    const directory = scope.allocate(
      c.array + buckets.size * c.arraySlot,
      "window partition output directory",
      () => [...buckets.values()],
    ).value;
    const sort = scratch.reserve(directory.length * c.arraySlot, "window partition ordering");
    try {
      directory.sort((left, right) => left.bucket - right.bucket);
    } finally {
      sort.release();
    }
    return scope.allocate(c.object + 2 * c.property, "window partition partial", () => ({
      bucketCount,
      buckets: directory,
    })).value;
  });
}

export async function evaluateWindowWorkUnitPartials(
  partials: readonly WindowWorkUnitPartial[],
  windows: Record<string, WindowExpr>,
  options: WindowWorkUnitEvaluationOptions = {},
): Promise<Row[]> {
  const { applyWindowsToRows } = await import("./window-backend.js");
  return withWindowResultScope(options, (scope) => {
    const scratch = scope.childScope();
    const bucketCount = options.bucketCount ?? partials[0]?.bucketCount ?? 1;
    validateBucketCount(bucketCount);
    const buckets = scratch.allocate(
      c.object,
      "window task merge directory",
      () => new Map<number, WindowWorkUnitRow[]>(),
    ).value;
    for (const partial of partials) {
      if (partial.bucketCount !== bucketCount) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          `Window work-unit bucket count mismatch (${partial.bucketCount} !== ${bucketCount})`,
        );
      }
      for (const bucket of partial.buckets) {
        let rows = buckets.get(bucket.bucket);
        if (rows === undefined) {
          rows = scratch.allocate(
            c.property + c.array,
            "window task merged bucket",
            () => [] as WindowWorkUnitRow[],
          ).value;
          buckets.set(bucket.bucket, rows);
        }
        scratch.allocate(
          bucket.rows.length * c.arraySlot,
          "window task merged row slots",
          () => undefined,
        );
        // Detach payloads once at the borrowed partial boundary. Result records
        // may share these payloads, so their charge follows the result scope.
        for (const entry of bucket.rows) {
          const detached = detachOwnedRow(entry.row, scope.budget, scope);
          try {
            scope.adopt(detached);
          } catch (error) {
            detached.release();
            throw error;
          }
          rows.push(
            scratch.allocate(c.object + 2 * c.property, "window task input ordinal", () => ({
              ordinal: entry.ordinal,
              row: detached.value,
            })).value,
          );
        }
      }
    }
    const out = scratch.allocate(
      c.array,
      "window task ordered results",
      () => [] as WindowWorkUnitRow[],
    ).value;
    const keys = scratch.allocate(
      c.array + buckets.size * c.arraySlot * 2,
      "window task bucket ordering",
      () => [...buckets.keys()].sort((left, right) => left - right),
    ).value;
    for (const bucket of keys) {
      const rows = buckets.get(bucket);
      if (rows === undefined) continue;
      const sort = scope.reserve(rows.length * c.arraySlot, "window task ordinal sort");
      try {
        rows.sort((left, right) => compareWindowWorkUnitOrdinal(left.ordinal, right.ordinal));
      } finally {
        sort.release();
      }
      const input = scratch.allocate(
        c.array + rows.length * c.arraySlot,
        "window task evaluation rows",
        () => rows.map((row) => row.row),
      ).value;
      const evaluated = applyWindowsToRows(input, windows, {
        ...options,
        memoryBudget: scope.budget,
        resultScope: scope,
      });
      scratch.allocate(
        rows.length * (c.arraySlot + c.object + 2 * c.property),
        "window task result ordinals",
        () => undefined,
      );
      for (let index = 0; index < rows.length; index += 1) {
        const row = rows[index];
        const evaluatedRow = evaluated[index];
        if (row !== undefined && evaluatedRow !== undefined)
          out.push({ ordinal: row.ordinal, row: evaluatedRow });
      }
    }
    const sort = scope.reserve(out.length * c.arraySlot, "window task result sort");
    try {
      out.sort((left, right) => compareWindowWorkUnitOrdinal(left.ordinal, right.ordinal));
    } finally {
      sort.release();
    }
    return scope.allocate(c.array + out.length * c.arraySlot, "window task output rows", () =>
      out.map((row) => row.row),
    ).value;
  });
}

export function windowPartitionBucket(
  row: Row,
  windows: Record<string, WindowExpr>,
  bucketCount: number,
  parent?: MemoryScope,
): number {
  validateBucketCount(bucketCount);
  // The key is consumed synchronously; per-row expression and normalization
  // storage must not accumulate with the retained partial.
  const temporary = parent?.childScope();
  try {
    return withWindowResultScope(
      temporary === undefined ? {} : { resultScope: temporary },
      (scope) => {
        const keys = scope.allocate(
          c.array,
          "window partition keys",
          () => [] as unknown[][],
        ).value;
        for (const alias in windows) {
          if (!Object.hasOwn(windows, alias)) continue;
          const expr = windows[alias];
          if (expr === undefined) continue;
          const values = scope.allocate(
            c.arraySlot + c.array + expr.over.partitionBy.length * c.arraySlot,
            "window partition expressions",
            () =>
              expr.over.partitionBy.map((part) =>
                snapshotScalarWithAdmission(evaluate(part, row, scope), scope),
              ),
          ).value;
          keys.push(values);
        }
        return hashString(internStableDataKey(keys, scope)) % bucketCount;
      },
    );
  } finally {
    temporary?.dispose();
  }
}

function validateBucketCount(bucketCount: number): void {
  if (!Number.isSafeInteger(bucketCount) || bucketCount < 1) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Window work-unit bucket count must be a positive integer",
      { bucketCount },
    );
  }
}

function hashString(value: string): number {
  let hash = 2_166_136_261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return hash >>> 0;
}

function compareWindowWorkUnitOrdinal(
  left: WindowWorkUnitOrdinal,
  right: WindowWorkUnitOrdinal,
): number {
  if (left.task !== right.task) return left.task - right.task;
  return left.row - right.row;
}
