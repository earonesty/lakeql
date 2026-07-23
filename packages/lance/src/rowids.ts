import { LakeqlError } from "lakeql";
import type { LanceRowIdSegment } from "./proto.js";

export interface ResolvedRowAddress {
  fragmentIndex: number;
  rowOffset: number;
}

export function resolveRequestedRowIds(
  requested: readonly bigint[],
  fragmentSequences: readonly {
    physicalRows: number;
    segments: LanceRowIdSegment[];
  }[],
): Map<string, ResolvedRowAddress> {
  const unresolved = new Set(requested.map(String));
  const resolved = new Map<string, ResolvedRowAddress>();
  for (const [fragmentIndex, fragment] of fragmentSequences.entries()) {
    let baseOffset = 0;
    for (const segment of fragment.segments) {
      validateSegment(segment);
      for (const rowIdText of unresolved) {
        const localOffset = findInSegment(segment, BigInt(rowIdText));
        if (localOffset === undefined) continue;
        resolved.set(rowIdText, { fragmentIndex, rowOffset: baseOffset + localOffset });
      }
      for (const rowIdText of resolved.keys()) unresolved.delete(rowIdText);
      baseOffset += segmentLength(segment);
    }
    if (baseOffset !== fragment.physicalRows) {
      corrupt("Lance stable row-id sequence length does not match fragment rows", {
        fragmentIndex,
        sequenceRows: baseOffset,
        physicalRows: fragment.physicalRows,
      });
    }
    if (unresolved.size === 0) break;
  }
  return resolved;
}

export function stableRowIdAtOffset(
  segments: readonly LanceRowIdSegment[],
  physicalOffset: number,
): bigint {
  if (!Number.isSafeInteger(physicalOffset) || physicalOffset < 0) {
    corrupt("Invalid Lance physical row offset", { physicalOffset });
  }
  let remaining = physicalOffset;
  for (const segment of segments) {
    validateSegment(segment);
    const length = segmentLength(segment);
    if (remaining >= length) {
      remaining -= length;
      continue;
    }
    switch (segment.kind) {
      case "range":
        return requiredRange(segment).start + BigInt(remaining);
      case "range_with_holes": {
        let value = requiredRange(segment).start + BigInt(remaining);
        for (const hole of segment.holes ?? []) {
          if (hole > value) break;
          value += 1n;
        }
        return value;
      }
      case "range_with_bitmap": {
        const { start, end } = requiredRange(segment);
        const width = safeOffset(end - start);
        let rank = 0;
        for (let index = 0; index < width; index += 1) {
          if (!bitmapValue(segment.bitmap ?? new Uint8Array(), index)) continue;
          if (rank === remaining) return start + BigInt(index);
          rank += 1;
        }
        break;
      }
      case "sorted_array":
      case "array": {
        const value = segment.values?.[remaining];
        if (value !== undefined) return value;
        break;
      }
    }
    corrupt("Lance stable row-ID segment does not cover its physical offset", {
      physicalOffset,
    });
  }
  corrupt("Lance stable row-ID sequence does not cover its physical offset", {
    physicalOffset,
  });
}

function findInSegment(segment: LanceRowIdSegment, rowId: bigint): number | undefined {
  switch (segment.kind) {
    case "range": {
      const { start, end } = requiredRange(segment);
      if (rowId < start || rowId >= end) return undefined;
      return safeOffset(rowId - start);
    }
    case "range_with_holes": {
      const { start, end } = requiredRange(segment);
      const holes = segment.holes ?? [];
      if (rowId < start || rowId >= end) return undefined;
      const position = lowerBound(holes, rowId);
      if (holes[position] === rowId) return undefined;
      return safeOffset(rowId - start) - position;
    }
    case "range_with_bitmap": {
      const { start, end } = requiredRange(segment);
      if (rowId < start || rowId >= end) return undefined;
      const index = safeOffset(rowId - start);
      const bitmap = segment.bitmap ?? new Uint8Array();
      if (!bitmapValue(bitmap, index)) return undefined;
      return bitmapRank(bitmap, index);
    }
    case "sorted_array": {
      const values = segment.values ?? [];
      const position = lowerBound(values, rowId);
      return values[position] === rowId ? position : undefined;
    }
    case "array": {
      const position = segment.values?.indexOf(rowId) ?? -1;
      return position < 0 ? undefined : position;
    }
  }
}

function segmentLength(segment: LanceRowIdSegment): number {
  switch (segment.kind) {
    case "range": {
      const { start, end } = requiredRange(segment);
      return safeOffset(end - start);
    }
    case "range_with_holes": {
      const { start, end } = requiredRange(segment);
      return safeOffset(end - start) - (segment.holes?.length ?? 0);
    }
    case "range_with_bitmap":
      return bitmapRank(
        segment.bitmap ?? new Uint8Array(),
        safeOffset(requiredRange(segment).end - requiredRange(segment).start),
      );
    case "sorted_array":
    case "array":
      return segment.values?.length ?? 0;
  }
}

function validateSegment(segment: LanceRowIdSegment): void {
  switch (segment.kind) {
    case "range":
      requiredRange(segment);
      return;
    case "range_with_holes": {
      const { start, end } = requiredRange(segment);
      const holes = segment.holes ?? [];
      let previous: bigint | undefined;
      for (const hole of holes) {
        if (hole < start || hole >= end || (previous !== undefined && hole <= previous)) {
          corrupt("Invalid Lance row-id hole sequence");
        }
        previous = hole;
      }
      return;
    }
    case "range_with_bitmap": {
      const { start, end } = requiredRange(segment);
      const width = safeOffset(end - start);
      if ((segment.bitmap?.byteLength ?? 0) * 8 < width) {
        corrupt("Truncated Lance row-id bitmap", {
          rangeLength: width,
          bitmapBytes: segment.bitmap?.byteLength ?? 0,
        });
      }
      return;
    }
    case "sorted_array": {
      let previous: bigint | undefined;
      for (const value of segment.values ?? []) {
        if (previous !== undefined && value <= previous) {
          corrupt("Lance sorted row-id array is not strictly increasing");
        }
        previous = value;
      }
      return;
    }
    case "array":
      return;
  }
}

function requiredRange(segment: LanceRowIdSegment): { start: bigint; end: bigint } {
  const start = segment.start;
  const end = segment.end;
  if (start === undefined || end === undefined || end < start) {
    corrupt("Invalid Lance row-id range");
  }
  return { start, end };
}

function lowerBound(values: readonly bigint[], target: bigint): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2);
    const value = values[middle];
    if (value !== undefined && value < target) low = middle + 1;
    else high = middle;
  }
  return low;
}

function bitmapValue(bitmap: Uint8Array, index: number): boolean {
  const byte = bitmap[Math.floor(index / 8)] ?? 0;
  return (byte & (1 << (7 - (index % 8)))) !== 0;
}

function bitmapRank(bitmap: Uint8Array, endExclusive: number): number {
  let count = 0;
  for (let index = 0; index < endExclusive; index += 1) {
    if (bitmapValue(bitmap, index)) count += 1;
  }
  return count;
}

function safeOffset(value: bigint): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    corrupt("Lance row offset exceeds JavaScript's safe integer range", {
      value: value.toString(),
    });
  }
  return Number(value);
}

function corrupt(message: string, details: Record<string, unknown> = {}): never {
  throw new LakeqlError("LAKEQL_LANCE_READ_ERROR", message, details);
}
