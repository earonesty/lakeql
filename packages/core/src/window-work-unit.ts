import { LakeqlError } from "./errors.js";
import { evaluate, jsonSafeValue } from "./evaluator.js";
import { stableStringify } from "./manifest.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import type { WindowExecutionOptions } from "./window-backend.js";

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
): WindowWorkUnitPartial {
  const buckets: WindowWorkUnitBucket[] = Array.from(
    { length: bucketCount },
    (_unused, bucket) => ({
      bucket,
      rows: [],
    }),
  );
  for (const row of rows) {
    const bucket = windowPartitionBucket(row.row, windows, bucketCount);
    buckets[bucket]?.rows.push({ ordinal: row.ordinal, row: { ...row.row } });
  }
  return { bucketCount, buckets: buckets.filter((bucket) => bucket.rows.length > 0) };
}

export async function evaluateWindowWorkUnitPartials(
  partials: readonly WindowWorkUnitPartial[],
  windows: Record<string, WindowExpr>,
  options: WindowWorkUnitEvaluationOptions = {},
): Promise<Row[]> {
  const bucketCount = options.bucketCount ?? partials[0]?.bucketCount ?? 1;
  const buckets = new Map<number, WindowWorkUnitRow[]>();
  for (const partial of partials) {
    if (partial.bucketCount !== bucketCount) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        `Window work-unit bucket count mismatch (${partial.bucketCount} !== ${bucketCount})`,
      );
    }
    for (const bucket of partial.buckets) {
      const rows = buckets.get(bucket.bucket) ?? [];
      rows.push(...bucket.rows.map((row) => ({ ordinal: row.ordinal, row: { ...row.row } })));
      buckets.set(bucket.bucket, rows);
    }
  }

  const { applyWindowsToRows } = await import("./window-backend.js");
  const out: WindowWorkUnitRow[] = [];
  for (const bucket of [...buckets.keys()].sort((left, right) => left - right)) {
    const rows = buckets.get(bucket) ?? [];
    rows.sort((left, right) => compareWindowWorkUnitOrdinal(left.ordinal, right.ordinal));
    const evaluated = applyWindowsToRows(
      rows.map((row) => row.row),
      windows,
      options,
    );
    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const evaluatedRow = evaluated[index];
      if (row !== undefined && evaluatedRow !== undefined) {
        out.push({ ordinal: row.ordinal, row: evaluatedRow });
      }
    }
  }
  out.sort((left, right) => compareWindowWorkUnitOrdinal(left.ordinal, right.ordinal));
  return out.map((row) => row.row);
}

export function windowPartitionBucket(
  row: Row,
  windows: Record<string, WindowExpr>,
  bucketCount: number,
): number {
  if (!Number.isInteger(bucketCount) || bucketCount < 1) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Window work-unit bucket count must be a positive integer",
      { bucketCount },
    );
  }
  const key = windowPartitionKey(row, windows);
  return hashString(key) % bucketCount;
}

function windowPartitionKey(row: Row, windows: Record<string, WindowExpr>): string {
  const keys = Object.values(windows).map((expr) =>
    expr.over.partitionBy.map((part) => jsonSafeValue(evaluate(part, row))),
  );
  return stableStringify(keys);
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
