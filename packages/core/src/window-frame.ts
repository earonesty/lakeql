import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import {
  applyIntervalToTimestamp,
  type IntervalValue,
  isIntervalValue,
  isNonNegativeInterval,
} from "./interval.js";
import type { MemoryScope } from "./memory-scope.js";
import { isTimestampValue, type TimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import type { WindowExpr, WindowFrame, WindowFrameBound } from "./window.js";
import type { WindowPeers } from "./window-peers.js";
import { samePeer, type WindowRow } from "./window-utils.js";

export interface FrameSpan {
  start: number;
  end: number;
}

export function frameRows(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
  peers?: WindowPeers,
  scope?: MemoryScope,
): Row[] {
  return [...frameRowIterator(partition, index, expr, peers, false, scope)];
}

/** Borrow frame rows with constant iterator state. Excluded intervals are jumped
 * over, not enumerated; reverse traversal supports bounded last-value reads. */
export function* frameRowIterator(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
  peers?: WindowPeers,
  reverse = false,
  scope?: MemoryScope,
): IterableIterator<Row> {
  const span = frameSpan(partition, index, expr, peers, scope);
  const exclude = expr.over.frame?.exclude ?? "no-others";
  let excludedStart = -1;
  let excludedEnd = -1;
  if (exclude === "current-row") {
    excludedStart = index;
    excludedEnd = index + 1;
  } else if (exclude === "group" || exclude === "ties") {
    const peer = peers?.peer(index) ?? peerSpan(partition, index);
    // Peer cursors are mutable: snapshot boundaries before yielding to a consumer.
    excludedStart = peer.start;
    excludedEnd = peer.end;
  }
  const step = reverse ? -1 : 1;
  for (
    let rowIndex = reverse ? span.end - 1 : span.start;
    rowIndex >= span.start && rowIndex < span.end;
  ) {
    if (
      rowIndex >= excludedStart &&
      rowIndex < excludedEnd &&
      !(exclude === "ties" && rowIndex === index)
    ) {
      rowIndex = reverse
        ? exclude === "ties" && rowIndex > index
          ? index
          : excludedStart - 1
        : exclude === "ties" && rowIndex < index
          ? index
          : excludedEnd;
      continue;
    }
    const row = partition[rowIndex]?.row;
    if (row !== undefined) yield row;
    rowIndex += step;
  }
}

export function frameSpan(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
  peers?: WindowPeers,
  scope?: MemoryScope,
): FrameSpan {
  const frame = effectiveFrame(expr);
  const temporary =
    frame.start.offset === undefined && frame.end.offset === undefined
      ? undefined
      : scope?.childScope();
  try {
    switch (frame.mode) {
      case "rows":
        return rowsFrameSpan(partition, index, frame, temporary);
      case "groups":
        return groupsFrameSpan(partition, index, frame, peers, temporary);
      case "range":
        return rangeFrameSpan(partition, index, expr, frame, peers, temporary);
      default:
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid window frame mode");
    }
  } finally {
    temporary?.dispose();
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
  scope?: MemoryScope,
): FrameSpan {
  return normalizeSpan(partition.length, {
    start: rowBoundIndex(partition, index, frame.start, scope),
    end: rowBoundIndex(partition, index, frame.end, scope) + 1,
  });
}

function rowBoundIndex(
  partition: readonly WindowRow[],
  index: number,
  bound: WindowFrameBound,
  scope?: MemoryScope,
): number {
  switch (bound.kind) {
    case "unbounded-preceding":
      return 0;
    case "unbounded-following":
      return partition.length - 1;
    case "current-row":
      return index;
    case "preceding":
      return index - offset(bound, partition[index]?.row, scope);
    case "following":
      return index + offset(bound, partition[index]?.row, scope);
  }
}

function groupsFrameSpan(
  partition: readonly WindowRow[],
  index: number,
  frame: WindowFrame,
  peers?: WindowPeers,
  scope?: MemoryScope,
): FrameSpan {
  const groups = peers === undefined ? peerGroups(partition) : undefined;
  const groupCount = peers?.groupCount ?? groups?.length ?? 0;
  if (groupCount === 0) return { start: 0, end: 0 };
  const groupIndex =
    peers?.peer(index).group ??
    groups?.findIndex((group) => index >= group.start && index < group.end) ??
    -1;
  if (groupIndex === -1) return { start: 0, end: 0 };
  const startGroup = groupBoundIndex(
    groupCount,
    groupIndex,
    frame.start,
    partition[index]?.row,
    scope,
  );
  const endGroup = groupBoundIndex(groupCount, groupIndex, frame.end, partition[index]?.row, scope);
  if (endGroup < 0 || startGroup >= groupCount) return { start: 0, end: 0 };
  const clampedStartGroup = clamp(startGroup, 0, groupCount - 1);
  const clampedEndGroup = clamp(endGroup, 0, groupCount - 1);
  return normalizeSpan(partition.length, {
    start: (peers?.group(clampedStartGroup, "start") ?? groups?.[clampedStartGroup])?.start ?? 0,
    end:
      (peers?.group(clampedEndGroup, "end") ?? groups?.[clampedEndGroup])?.end ?? partition.length,
  });
}

function groupBoundIndex(
  groupCount: number,
  groupIndex: number,
  bound: WindowFrameBound,
  row: Row | undefined,
  scope?: MemoryScope,
): number {
  switch (bound.kind) {
    case "unbounded-preceding":
      return 0;
    case "unbounded-following":
      return groupCount - 1;
    case "current-row":
      return groupIndex;
    case "preceding":
      return groupIndex - offset(bound, row, scope);
    case "following":
      return groupIndex + offset(bound, row, scope);
  }
}

function rangeFrameSpan(
  partition: readonly WindowRow[],
  index: number,
  expr: WindowExpr,
  frame: WindowFrame,
  peers?: WindowPeers,
  scope?: MemoryScope,
): FrameSpan {
  if (frame.start.offset !== undefined || frame.end.offset !== undefined) {
    return rangeOffsetFrameSpan(partition, index, expr, frame, scope);
  }
  const peer = peers?.peer(index) ?? peerSpan(partition, index);
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
  scope?: MemoryScope,
): FrameSpan {
  if (expr.over.orderBy.length !== 1) {
    throw new LakeqlError(
      "LAKEQL_SQL_UNSUPPORTED",
      "RANGE offset frames require exactly one ORDER BY expression",
    );
  }
  const direction = expr.over.orderBy[0]?.direction ?? "asc";
  const current = rangePoint(partition[index]?.orderKey[0], direction);
  const start = rangeOffsetBound(current, frame.start, partition[index]?.row, direction, scope);
  const end = rangeOffsetBound(current, frame.end, partition[index]?.row, direction, scope);
  const first = rangeBoundary(partition, direction, start, 0, false);
  const afterLast = rangeBoundary(partition, direction, end, first, true);
  return normalizeSpan(partition.length, { start: first, end: afterLast });
}

/** Sorted RANGE endpoints need no partition-sized key cache or monotone offsets. */
function rangeBoundary(
  partition: readonly WindowRow[],
  direction: "asc" | "desc",
  bound: number | bigint,
  start: number,
  inclusive: boolean,
): number {
  let low = start;
  let high = partition.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    const value = rangePoint(partition[middle]?.orderKey[0], direction).normalized;
    const comparison = compareRangeValues(value, bound);
    if (comparison < 0 || (inclusive && comparison === 0)) low = middle + 1;
    else high = middle;
  }
  return low;
}

type RangePoint =
  | { kind: "number"; value: number; normalized: number }
  | { kind: "timestamp"; value: TimestampValue; normalized: bigint };

function rangeOffsetBound(
  current: RangePoint,
  bound: WindowFrameBound,
  row: Row | undefined,
  direction: "asc" | "desc",
  scope?: MemoryScope,
): number | bigint {
  switch (bound.kind) {
    case "unbounded-preceding":
      return current.kind === "number" ? -Infinity : -rangeInfinity();
    case "unbounded-following":
      return current.kind === "number" ? Infinity : rangeInfinity();
    case "current-row":
      return current.normalized;
    case "preceding":
      if (current.kind === "number")
        return current.normalized - numericRangeOffset(bound, row, scope);
      return shiftedTimestampRangeBound(
        current,
        bound,
        row,
        direction === "asc" ? -1 : 1,
        direction,
        scope,
      );
    case "following":
      if (current.kind === "number")
        return current.normalized + numericRangeOffset(bound, row, scope);
      return shiftedTimestampRangeBound(
        current,
        bound,
        row,
        direction === "asc" ? 1 : -1,
        direction,
        scope,
      );
  }
}

function shiftedTimestampRangeBound(
  current: Extract<RangePoint, { kind: "timestamp" }>,
  bound: WindowFrameBound,
  row: Row | undefined,
  shiftDirection: -1 | 1,
  direction: "asc" | "desc",
  scope?: MemoryScope,
): bigint {
  const interval = intervalRangeOffset(bound, row, scope);
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

function offset(bound: WindowFrameBound, row: Row | undefined, scope?: MemoryScope): number {
  if (bound.offset === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window frame offset is required");
  }
  const value = evaluate(bound.offset, row ?? {}, scope);
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Window frame offset must be a non-negative integer",
    );
  }
  return value;
}

function numericRangeOffset(
  bound: WindowFrameBound,
  row: Row | undefined,
  scope?: MemoryScope,
): number {
  if (bound.offset === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window frame offset is required");
  }
  const value = evaluate(bound.offset, row ?? {}, scope);
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "RANGE frame offset must be a non-negative finite number",
    );
  }
  return value;
}

function intervalRangeOffset(
  bound: WindowFrameBound,
  row: Row | undefined,
  scope?: MemoryScope,
): IntervalValue {
  if (bound.offset === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window frame offset is required");
  }
  const value = evaluate(bound.offset, row ?? {}, scope);
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
