import { LakeqlError, type OwnedMemory, type Vector, vectorLength } from "lakeql-core";

/** Select a half-open row interval before batching. Borrows each source lease;
 * closing the window closes the source without reading beyond the requested end.
 */
export async function* readNestedRowWindow(
  rows: AsyncIterable<OwnedMemory<Vector>>,
  start: number,
  end: number,
): AsyncGenerator<OwnedMemory<Vector>> {
  validateNestedRowWindow(start, end);
  if (start === end) return;
  let position = 0;
  for await (const row of rows) {
    if (vectorLength(row.value) !== 1)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested row window requires single rows");
    if (position++ >= start) yield row;
    if (position === end) return;
  }
  if (position < end)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested row window exceeds column rows");
}

export function validateNestedRowWindow(start: number, end: number): void {
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end < start)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid nested row window");
}
