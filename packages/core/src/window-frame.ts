import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import {
  applyIntervalToTimestamp,
  type IntervalValue,
  isIntervalValue,
  isNonNegativeInterval,
} from "./interval.js";
import { isTimestampValue, type TimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import type { WindowExpr, WindowFrame, WindowFrameBound } from "./window.js";
import { samePeer, type WindowRow } from "./window-utils.js";

export interface FrameSpan {
  start: number;
  end: number;
}

export function frameRows(partition: readonly WindowRow[], index: number, expr: WindowExpr): Row[] {
  const span = frameSpan(partition, index, expr);
  const excluded = excludedRows(partition, index, span, expr.over.frame);
  const out: Row[] = [];
  for (let rowIndex = span.start; rowIndex < span.end; rowIndex += 1) {
    if (excluded.has(rowIndex)) continue;
    const row = partition[rowIndex]?.row;
    if (row !== undefined) out.push(row);
  }
  return out;
}

export function frameSpan(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
): FrameSpan {
  const frame = effectiveFrame(expr);
  switch (frame.mode) {
    case "rows":
      return rowsFrameSpan(partition, index, frame);
    case "groups":
      return groupsFrameSpan(partition, index, frame);
    case "range":
      return rangeFrameSpan(partition, index, expr, frame);
  }
}

function effectiveFrame(expr: WindowExpr): WindowFrame {
  if (expr.over.frame !== undefined) return expr.over.frame;
  if (expr.over.orderBy.length === 0) {
    return {
      mode: "rows",
      start: { kind: "unbounded-preceding" },
      end: { kind: "unbounded-following" },
      exclude: "no-others",
    };
  }
  return {
    mode: "range",
    start: { kind: "unbounded-preceding" },
    end: { kind: "current-row" },
    exclude: "no-others",
  };
}

function rowsFrameSpan(
  partition: readonly WindowRow[],
  index: number,
  frame: WindowFrame,
): FrameSpan {
  return normalizeSpan(partition.length, {
    start: rowBoundIndex(partition, index, frame.start),
    end: rowBoundIndex(partition, index, frame.end) + 1,
  });
}

function rowBoundIndex(
  partition: readonly WindowRow[],
  index: number,
  bound: WindowFrameBound,
): number {
  switch (bound.kind) {
    case "unbounded-preceding":
      return 0;
    case "unbounded-following":
      return partition.length - 1;
    case "current-row":
      return index;
    case "preceding":
      return index - offset(bound, partition[index]?.row);
    case "following":
      return index + offset(bound, partition[index]?.row);
  }
}

function groupsFrameSpan(
  partition: readonly WindowRow[],
  index: number,
  frame: WindowFrame,
): FrameSpan {
  const groups = peerGroups(partition);
  const groupIndex = groups.findIndex((group) => index >= group.start && index < group.end);
  if (groupIndex === -1) return { start: 0, end: 0 };
  const startGroup = groupBoundIndex(groups.length, groupIndex, frame.start, partition[index]?.row);
  const endGroup = groupBoundIndex(groups.length, groupIndex, frame.end, partition[index]?.row);
  if (endGroup < 0 || startGroup >= groups.length) return { start: 0, end: 0 };
  const clampedStartGroup = clamp(startGroup, 0, groups.length - 1);
  const clampedEndGroup = clamp(endGroup, 0, groups.length - 1);
  return normalizeSpan(partition.length, {
    start: groups[clampedStartGroup]?.start ?? 0,
    end: groups[clampedEndGroup]?.end ?? partition.length,
  });
}

function groupBoundIndex(
  groupCount: number,
  groupIndex: number,
  bound: WindowFrameBound,
  row: Row | undefined,
): number {
  switch (bound.kind) {
    case "unbounded-preceding":
      return 0;
    case "unbounded-following":
      return groupCount - 1;
    case "current-row":
      return groupIndex;
    case "preceding":
      return groupIndex - offset(bound, row);
    case "following":
      return groupIndex + offset(bound, row);
  }
}

function rangeFrameSpan(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
  frame: WindowFrame,
): FrameSpan {
  if (frame.start.offset !== undefined || frame.end.offset !== undefined) {
    return rangeOffsetFrameSpan(partition, index, expr, frame);
  }
  const peer = peerSpan(partition, index);
  return normalizeSpan(partition.length, {
    start: rangeEndpoint(partition.length, peer, frame.start, "start"),
    end: rangeEndpoint(partition.length, peer, frame.end, "end") + 1,
  });
}

function rangeEndpoint(
  rowCount: number,
  peer: FrameSpan,
  bound: WindowFrameBound,
  side: "start" | "end",
): number {
  switch (bound.kind) {
    case "unbounded-preceding":
      return 0;
    case "unbounded-following":
      return rowCount - 1;
    case "current-row":
      return side === "start" ? peer.start : peer.end - 1;
    case "preceding":
    case "following":
      throw new LakeqlError(
        "LAKEQL_SQL_UNSUPPORTED",
        "RANGE offset frames require the range-offset backend",
      );
  }
}

function rangeOffsetFrameSpan(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
  frame: WindowFrame,
): FrameSpan {
  if (expr.over.orderBy.length !== 1) {
    throw new LakeqlError(
      "LAKEQL_SQL_UNSUPPORTED",
      "RANGE offset frames require exactly one ORDER BY expression",
    );
  }
  const direction = expr.over.orderBy[0]?.direction ?? "asc";
  const current = rangePoint(partition[index]?.orderKey[0], direction);
  const start = rangeOffsetBound(current, frame.start, partition[index]?.row, direction);
  const end = rangeOffsetBound(current, frame.end, partition[index]?.row, direction);
  let first = 0;
  while (
    first < partition.length &&
    compareRangeValues(rangePoint(partition[first]?.orderKey[0], direction).normalized, start) < 0
  ) {
    first += 1;
  }
  let afterLast = first;
  while (
    afterLast < partition.length &&
    compareRangeValues(rangePoint(partition[afterLast]?.orderKey[0], direction).normalized, end) <=
      0
  ) {
    afterLast += 1;
  }
  return normalizeSpan(partition.length, { start: first, end: afterLast });
}

type RangePoint =
  | { kind: "number"; value: number; normalized: number }
  | { kind: "timestamp"; value: TimestampValue; normalized: bigint };

function rangeOffsetBound(
  current: RangePoint,
  bound: WindowFrameBound,
  row: Row | undefined,
  direction: "asc" | "desc",
): number | bigint {
  switch (bound.kind) {
    case "unbounded-preceding":
      return current.kind === "number" ? -Infinity : -rangeInfinity();
    case "unbounded-following":
      return current.kind === "number" ? Infinity : rangeInfinity();
    case "current-row":
      return current.normalized;
    case "preceding":
      if (current.kind === "number") return current.normalized - numericRangeOffset(bound, row);
      return shiftedTimestampRangeBound(
        current,
        bound,
        row,
        direction === "asc" ? -1 : 1,
        direction,
      );
    case "following":
      if (current.kind === "number") return current.normalized + numericRangeOffset(bound, row);
      return shiftedTimestampRangeBound(
        current,
        bound,
        row,
        direction === "asc" ? 1 : -1,
        direction,
      );
  }
}

function shiftedTimestampRangeBound(
  current: Extract<RangePoint, { kind: "timestamp" }>,
  bound: WindowFrameBound,
  row: Row | undefined,
  shiftDirection: -1 | 1,
  direction: "asc" | "desc",
): bigint {
  const interval = intervalRangeOffset(bound, row);
  const shifted = applyIntervalToTimestamp(current.value, interval, shiftDirection);
  return direction === "desc" ? -shifted.epochNanoseconds : shifted.epochNanoseconds;
}

function rangePoint(value: unknown, direction: "asc" | "desc"): RangePoint {
  if (typeof value === "number" && Number.isFinite(value)) {
    return { kind: "number", value, normalized: direction === "asc" ? value : -value };
  }
  if (isTimestampValue(value)) {
    return {
      kind: "timestamp",
      value,
      normalized: direction === "asc" ? value.epochNanoseconds : -value.epochNanoseconds,
    };
  }
  throw new LakeqlError(
    "LAKEQL_TYPE_ERROR",
    "RANGE offset frames require a non-null numeric or timestamp ORDER BY value",
  );
}

function compareRangeValues(left: number | bigint, right: number | bigint): number {
  if (typeof left !== typeof right) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "RANGE frame bounds must have matching types");
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function rangeInfinity(): bigint {
  return 1n << 255n;
}

function excludedRows(
  partition: readonly WindowRow[],
  index: number,
  span: FrameSpan,
  frame: WindowFrame | undefined,
): Set<number> {
  const exclude = frame?.exclude ?? "no-others";
  const out = new Set<number>();
  if (exclude === "no-others") return out;
  const peer = peerSpan(partition, index);
  if (exclude === "current-row") {
    out.add(index);
    return out;
  }
  for (
    let rowIndex = Math.max(span.start, peer.start);
    rowIndex < Math.min(span.end, peer.end);
    rowIndex += 1
  ) {
    if (exclude === "ties" && rowIndex === index) continue;
    out.add(rowIndex);
  }
  return out;
}

function peerSpan(partition: readonly WindowRow[], index: number): FrameSpan {
  let start = index;
  while (start > 0 && samePeer(partition[start - 1] as WindowRow, partition[index] as WindowRow)) {
    start -= 1;
  }
  let end = index + 1;
  while (
    end < partition.length &&
    samePeer(partition[index] as WindowRow, partition[end] as WindowRow)
  ) {
    end += 1;
  }
  return { start, end };
}

function peerGroups(partition: readonly WindowRow[]): FrameSpan[] {
  const groups: FrameSpan[] = [];
  let start = 0;
  while (start < partition.length) {
    const peer = peerSpan(partition, start);
    groups.push(peer);
    start = peer.end;
  }
  return groups;
}

function offset(bound: WindowFrameBound, row: Row | undefined): number {
  if (bound.offset === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window frame offset is required");
  }
  const value = evaluate(bound.offset, row ?? {});
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Window frame offset must be a non-negative integer",
    );
  }
  return value;
}

function numericRangeOffset(bound: WindowFrameBound, row: Row | undefined): number {
  if (bound.offset === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window frame offset is required");
  }
  const value = evaluate(bound.offset, row ?? {});
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "RANGE frame offset must be a non-negative finite number",
    );
  }
  return value;
}

function intervalRangeOffset(bound: WindowFrameBound, row: Row | undefined): IntervalValue {
  if (bound.offset === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window frame offset is required");
  }
  const value = evaluate(bound.offset, row ?? {});
  if (!isIntervalValue(value) || !isNonNegativeInterval(value)) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Timestamp RANGE frame offset must be a non-negative interval",
    );
  }
  return value;
}

function normalizeSpan(rowCount: number, span: FrameSpan): FrameSpan {
  const start = clamp(span.start, 0, rowCount);
  const end = clamp(span.end, 0, rowCount);
  return start <= end ? { start, end } : { start: end, end };
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}
