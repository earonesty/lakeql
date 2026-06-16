import { LakeqlError } from "./errors.js";
import { stableStringify } from "./manifest.js";
import type { Row } from "./types.js";

export type JoinType = "inner" | "left" | "semi" | "anti";
export type JoinKey = string | string[];

export interface BroadcastJoinOptions {
  leftKey: JoinKey;
  rightKey: JoinKey;
  maxRightRows: number;
  type?: JoinType;
  rightPrefix?: string;
}

export interface LookupJoinOptions {
  leftKey: JoinKey;
  rightKey: JoinKey;
  maxRightRows: number;
  type?: JoinType;
  rightPrefix?: string;
}

export type LookupJoinFunction = (
  key: string | number | boolean | bigint | null | (string | number | boolean | bigint | null)[],
  leftRow: Row,
) => AsyncIterable<Row> | Iterable<Row> | Promise<AsyncIterable<Row> | Iterable<Row>>;

export async function broadcastJoin(
  left: AsyncIterable<Row> | Iterable<Row>,
  right: AsyncIterable<Row> | Iterable<Row>,
  options: BroadcastJoinOptions,
): Promise<Row[]> {
  validateJoinOptions(options, "Broadcast");
  const rightRows = await collectRightRows(right, options.maxRightRows);
  const index = new Map<string, Row[]>();
  for (const row of rightRows) {
    const key = joinKey(row, options.rightKey);
    const bucket = index.get(key);
    if (bucket) bucket.push(row);
    else index.set(key, [row]);
  }

  const out: Row[] = [];
  for await (const leftRow of left) {
    const matches = index.get(joinKey(leftRow, options.leftKey));
    if (options.type === "semi") {
      if (matches && matches.length > 0) out.push({ ...leftRow });
      continue;
    }
    if (options.type === "anti") {
      if (!matches || matches.length === 0) out.push({ ...leftRow });
      continue;
    }
    if (!matches || matches.length === 0) {
      if (options.type === "left") out.push({ ...leftRow });
      continue;
    }
    for (const rightRow of matches) out.push(mergeRows(leftRow, rightRow, options));
  }
  return out;
}

export async function lookupJoin(
  left: AsyncIterable<Row> | Iterable<Row>,
  lookup: LookupJoinFunction,
  options: LookupJoinOptions,
): Promise<Row[]> {
  validateJoinOptions(options, "Lookup");
  let rightRowsRead = 0;
  const out: Row[] = [];
  for await (const leftRow of left) {
    const leftValue = joinValue(leftRow, options.leftKey);
    const leftKey = stableStringify(leftValue);
    const matches: Row[] = [];
    const rightRows = await lookup(leftValue, leftRow);
    for await (const rightRow of rightRows) {
      rightRowsRead += 1;
      enforceMaxRightRows("Lookup", rightRowsRead, options.maxRightRows);
      if (joinKey(rightRow, options.rightKey) === leftKey) matches.push(rightRow);
    }
    if (options.type === "semi") {
      if (matches.length > 0) out.push({ ...leftRow });
      continue;
    }
    if (options.type === "anti") {
      if (matches.length === 0) out.push({ ...leftRow });
      continue;
    }
    if (matches.length === 0) {
      if (options.type === "left") out.push({ ...leftRow });
      continue;
    }
    for (const rightRow of matches) out.push(mergeRows(leftRow, rightRow, options));
  }
  return out;
}

async function collectRightRows(
  rows: AsyncIterable<Row> | Iterable<Row>,
  maxRightRows: number,
): Promise<Row[]> {
  const out: Row[] = [];
  for await (const row of rows) {
    if (out.length >= maxRightRows) {
      enforceMaxRightRows("Broadcast", out.length + 1, maxRightRows);
    }
    out.push(row);
  }
  return out;
}

function validateJoinOptions(
  options: BroadcastJoinOptions | LookupJoinOptions,
  strategy: string,
): void {
  const leftKeys = normalizeJoinKeys(options.leftKey, "leftKey", strategy);
  const rightKeys = normalizeJoinKeys(options.rightKey, "rightKey", strategy);
  if (leftKeys.length !== rightKeys.length) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${strategy} join key counts must match`, {
      leftKey: options.leftKey,
      rightKey: options.rightKey,
    });
  }
  if (leftKeys.length === 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${strategy} join requires leftKey and rightKey`);
  }
  if (!Number.isInteger(options.maxRightRows) || options.maxRightRows < 1) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      `${strategy} join maxRightRows must be a positive integer`,
    );
  }
  if (
    options.type !== undefined &&
    options.type !== "inner" &&
    options.type !== "left" &&
    options.type !== "semi" &&
    options.type !== "anti"
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${strategy} join type is not supported`, {
      type: options.type,
    });
  }
}

function normalizeJoinKeys(key: JoinKey, label: string, strategy: string): string[] {
  const keys = Array.isArray(key) ? key : [key];
  if (keys.some((column) => typeof column !== "string" || column.length === 0)) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      `${strategy} join ${label} must contain column names`,
      {
        [label]: key,
      },
    );
  }
  return keys;
}

function joinKey(row: Row, column: JoinKey): string {
  return stableStringify(joinValue(row, column));
}

function joinValue(
  row: Row,
  column: JoinKey,
): string | number | boolean | bigint | null | (string | number | boolean | bigint | null)[] {
  if (Array.isArray(column)) return column.map((key) => scalarJoinValue(row, key));
  return scalarJoinValue(row, column);
}

function scalarJoinValue(row: Row, column: string): string | number | boolean | bigint | null {
  if (!(column in row)) {
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown join key ${column}`, { column });
  }
  const value = row[column];
  if (value === null || value === undefined) return null;
  if (
    typeof value !== "string" &&
    typeof value !== "number" &&
    typeof value !== "boolean" &&
    typeof value !== "bigint"
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `Join key ${column} must be scalar`, { column });
  }
  return value;
}

function mergeRows(left: Row, right: Row, options: BroadcastJoinOptions | LookupJoinOptions): Row {
  const out: Row = { ...left };
  const prefix = options.rightPrefix ?? "right.";
  const leftKeys = normalizeJoinKeys(options.leftKey, "leftKey", "Merge");
  const rightKeys = normalizeJoinKeys(options.rightKey, "rightKey", "Merge");
  for (const [key, value] of Object.entries(right)) {
    if (rightKeys.includes(key) && leftKeys.includes(key)) continue;
    const outKey = key in out ? `${prefix}${key}` : key;
    out[outKey] = value;
  }
  return out;
}

function enforceMaxRightRows(strategy: string, actual: number, limit: number): void {
  if (actual <= limit) return;
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `${strategy} join exceeded maxRightRows (${actual} > ${limit})`,
    { metric: "maxRightRows", limit, actual },
  );
}
