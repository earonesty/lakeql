import type { Batch } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { gatherBatch } from "./vector-sort.js";

export interface PositionedBatch {
  rowOffset: number;
  batch: Batch;
}

/** Join ordered selected positions to deferred columns in bounded read windows.
 * The caller owns indices for the iterator lifetime. Each yielded owner transfers
 * to the receiver; source and fetched backing remain live until it is released.
 * Without a deferred reader, gather already-present columns under the same contract.
 */
export async function* gatherDeferredColumns(
  input: OwnedMemory<PositionedBatch>,
  indices: Uint32Array,
  maxWindowRows: number,
  read?: (rowStart: number, rowEnd: number) => AsyncIterable<OwnedMemory<PositionedBatch>>,
): AsyncGenerator<OwnedMemory<Batch>, void, unknown> {
  const { rowOffset, batch } = input.value;
  if (
    !Number.isSafeInteger(maxWindowRows) ||
    maxWindowRows < 1 ||
    !Number.isSafeInteger(rowOffset) ||
    rowOffset < 0 ||
    !Number.isSafeInteger(batch.rowCount) ||
    batch.rowCount < 0 ||
    batch.rowCount > 0x100000000 ||
    !Number.isSafeInteger(rowOffset + batch.rowCount)
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid deferred gather extent");
  let previous = -1;
  for (const index of indices) {
    if (index <= previous || index >= batch.rowCount)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Deferred positions must be ordered and unique",
      );
    previous = index;
  }
  if (read === undefined) {
    if (indices.length === 0) return;
    const scope = new MemoryScope(input.budget);
    try {
      scope.adopt(input.retain());
      yield scope.retainAllocations(gatherBatch(batch, indices, scope));
    } finally {
      scope.dispose();
    }
    return;
  }
  let cursor = 0;
  while (cursor < indices.length) {
    const first = indices[cursor] ?? 0;
    let end = cursor + 1;
    while (end < indices.length && (indices[end] ?? 0) - first < maxWindowRows) end++;
    const rowStart = rowOffset + first;
    const rowEnd = rowOffset + (indices[end - 1] ?? 0) + 1;
    let fetchedEnd = rowStart;
    for await (const fetched of read(rowStart, rowEnd)) {
      const scope = new MemoryScope(input.budget);
      try {
        scope.adopt(fetched);
        scope.adopt(input.retain());
        const late = fetched.value;
        const lateEnd = late.rowOffset + late.batch.rowCount;
        if (
          !Number.isSafeInteger(late.rowOffset) ||
          !Number.isSafeInteger(lateEnd) ||
          !Number.isSafeInteger(late.batch.rowCount) ||
          late.batch.rowCount < 0 ||
          late.rowOffset < fetchedEnd ||
          lateEnd > rowEnd
        )
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Deferred scan returned invalid row positions",
          );
        fetchedEnd = lateEnd;
        const start = cursor;
        while (cursor < end && rowOffset + (indices[cursor] ?? 0) < lateEnd) {
          if (rowOffset + (indices[cursor] ?? 0) < late.rowOffset)
            throw new LakeqlError(
              "LAKEQL_VALIDATION_ERROR",
              "Deferred scan omitted a selected row",
            );
          cursor++;
        }
        if (cursor === start) continue;
        const sourceIndices = indices.subarray(start, cursor);
        const local = scope.allocate((cursor - start) * 4, "deferred gather positions", () => {
          const positions = new Uint32Array(cursor - start);
          for (let i = 0; i < positions.length; i++)
            positions[i] = rowOffset + (sourceIndices[i] ?? 0) - late.rowOffset;
          return positions;
        });
        const earlyBatch = gatherBatch(batch, sourceIndices, scope);
        const lateBatch = gatherBatch(late.batch, local.value, scope);
        local.release();
        for (const name of Object.keys(lateBatch.columns)) {
          if (Object.hasOwn(earlyBatch.columns, name))
            throw new LakeqlError(
              "LAKEQL_VALIDATION_ERROR",
              "Deferred columns overlap retained columns",
              { name },
            );
        }
        yield scope.retainAllocations({
          rowCount: cursor - start,
          columns: { ...earlyBatch.columns, ...lateBatch.columns },
        });
        if (cursor === end) break;
      } finally {
        scope.dispose();
        fetched.release();
      }
    }
    if (cursor !== end)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Deferred scan omitted selected rows");
  }
}
