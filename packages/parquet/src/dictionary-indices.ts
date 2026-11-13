import type { DataReader } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope } from "lakeql-core";
import { visitLevelRuns } from "./level-runs.js";

/** Decode indices only; dictionary payload remains borrowed and is never read. */
export function readDictionaryIndices(
  reader: DataReader,
  count: number,
  dictionaryLength: number,
  scope: MemoryScope,
): Uint32Array {
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > 0xffffffff ||
    !Number.isSafeInteger(dictionaryLength) ||
    dictionaryLength < 0 ||
    dictionaryLength > 0xffffffff
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid dictionary index extent");
  const values = scope.allocate(
    count * 4,
    "Parquet dictionary indices",
    () => new Uint32Array(count),
  ).value;
  if (count === 0) {
    const remaining = reader.view.byteLength - reader.offset;
    if (remaining === 0) return values;
    if (remaining !== 1 || reader.view.getUint8(reader.offset) > 32)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid empty dictionary index stream");
    reader.offset++;
    return values;
  }
  if (reader.offset >= reader.view.byteLength)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing dictionary bit width");
  const width = reader.view.getUint8(reader.offset++);
  if (width > 32) throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid dictionary bit width");
  if (width === 0) {
    if (dictionaryLength === 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Dictionary index out of bounds");
    // Some writers omit zero-width runs; others emit ordinary hybrid headers.
    if (reader.offset === reader.view.byteLength) return values;
  }
  visitLevelRuns(
    reader,
    width,
    count,
    (value, start, run) => {
      if (value >= dictionaryLength)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Dictionary index out of bounds");
      values.fill(value, start, start + run);
    },
    reader.view.byteLength - reader.offset,
  );
  return values;
}
