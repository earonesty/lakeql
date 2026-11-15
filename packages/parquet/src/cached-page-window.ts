import {
  MATERIALIZATION_CHARGES as c,
  isLakeqlError,
  jsonStringUtf8Bytes,
  type MemoryScope,
} from "lakeql-core";
import {
  buildDecodedCacheKey,
  type DecodedCacheKey,
  type DecodedColumnCache,
  decodedCacheKeyJsonCharacters,
  decodedCacheKeyPlan,
  decodedCacheKeyWorkspaceBytes,
  decodedCacheNumberCharacters,
} from "./decoded-column-cache.js";
import { type PreparedPageWindow, preparePageWindow } from "./page-window.js";
import type { StoreAsyncBuffer } from "./types.js";

/** Cache only projected scalar header metadata, never probe backing or a decoder.
 * The caller pins the descriptor independently of eviction and source lifetime. */
export async function cachedPageWindow(
  file: StoreAsyncBuffer,
  extent: { start: number; end: number },
  offset: number,
  scope: MemoryScope,
  cache: { cache: DecodedColumnCache; path: DecodedCacheKey },
): Promise<PreparedPageWindow | undefined> {
  const scratch = scope.childScope();
  try {
    const path = cache.path;
    const { byteLength, etag } = file;
    const { start, end } = extent;
    const key = decodedCacheKeyPlan(
      21 +
        decodedCacheKeyJsonCharacters(path) +
        decodedCacheNumberCharacters(byteLength) +
        (etag === undefined ? 4 : jsonStringUtf8Bytes(etag)) +
        decodedCacheNumberCharacters(start) +
        decodedCacheNumberCharacters(end) +
        decodedCacheNumberCharacters(offset),
      () =>
        JSON.stringify([
          "page-header",
          buildDecodedCacheKey(path),
          byteLength,
          etag ?? null,
          start,
          end,
          offset,
        ]),
      decodedCacheKeyWorkspaceBytes(path),
    );
    try {
      const hit = cache.cache.acquireValueOwned<PreparedPageWindow>(key, scope.budget);
      if (hit) {
        try {
          return scope.adopt(hit).value;
        } catch (error) {
          hit.release();
          throw error;
        }
      }
    } catch (error) {
      if (!isLakeqlError(error) || error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
    }
    // preparePageWindow projects at most four scalar header records. Reserve
    // their descriptors before parsing; enum strings are shared constants.
    scratch.allocate(
      6 * c.object + 32 * c.property + 24 * c.number,
      "cached page header metadata",
      () => null,
    );
    const page = await preparePageWindow(file, extent.start, extent.end, offset, scratch);
    if (!page) return undefined;
    const owner = scratch.retainAllocations(page);
    try {
      cache.cache.setValueOwned(key, owner);
      const retained = owner.retain();
      try {
        return scope.adopt(retained).value;
      } catch (error) {
        retained.release();
        throw error;
      }
    } finally {
      owner.release();
    }
  } finally {
    scratch.dispose();
  }
}
