/** Preserve file identity field boundaries, including absent versus empty tags.
 * Versioned namespaces keep prior delimiter-joined entries cold until eviction. */
export function fileCacheKey(
  namespace: "scan-range-v2" | "owned-scan-range-v2" | "parquet-metadata-v2",
  path: string,
  byteLength: number,
  etag: string | undefined,
  start?: number,
  end?: number,
): string {
  return JSON.stringify([namespace, path, byteLength, etag ?? null, start ?? null, end ?? null]);
}
