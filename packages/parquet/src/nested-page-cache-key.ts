import { MATERIALIZATION_CHARGES as c, type MemoryScope } from "lakeql-core";

export interface NestedPageKeyParts {
  shape: "dictionary" | "compact" | "leaf-slots";
  interpretation: string;
  parentKey: string;
  byteLength: number;
  etag?: string | undefined;
  path: readonly string[];
  start: number;
  end: number;
  previousDefinition?: number | undefined;
}

/** Optional lookup workspace. JSON escaping needs at most six UTF-16 units per
 * input unit. Keep the existing key format; decline without evicting cached data
 * when workspace cannot fit. The caller owns this scratch through cache insertion. */
export function nestedPageCacheKey(
  scope: MemoryScope,
  parts: NestedPageKeyParts,
): string | undefined {
  // Optional lookup work cannot spend a nonrefundable stage extent reserved for
  // later mandatory decode/output. Use direct reading in that ownership context.
  if (scope.hasAdmission) return undefined;
  let units =
    parts.shape.length +
    parts.interpretation.length +
    parts.parentKey.length +
    (parts.etag?.length ?? 0);
  for (const field of parts.path) units += field.length;
  // Fixed marker, quotes/separators, four numeric/nullable scalars and nested
  // path punctuation. Every JS finite number needs fewer than 32 JSON units.
  const characters = 6 * units + 384 + 3 * parts.path.length;
  const bytes = c.array + 10 * c.arraySlot + c.string + characters * c.stringCodeUnit;
  if (!Number.isSafeInteger(bytes) || bytes > scope.availableAllocationBytes) return undefined;
  return scope.allocate(bytes, "nested page cache key workspace", () =>
    JSON.stringify([
      "nested-page-v3",
      parts.shape,
      parts.interpretation,
      parts.parentKey,
      parts.byteLength,
      parts.etag,
      parts.path,
      parts.start,
      parts.end,
      parts.previousDefinition,
    ]),
  ).value;
}
