import { MATERIALIZATION_CHARGES as c, isLakeqlError, type MemoryScope } from "lakeql-core";
import type { DecodedColumnCache } from "./decoded-column-cache.js";
import { type PreparedPageWindow, preparePageWindow } from "./page-window.js";
import type { StoreAsyncBuffer } from "./types.js";

/** Cache only projected scalar header metadata, never probe backing or a decoder.
 * The caller pins the descriptor independently of eviction and source lifetime. */
export async function cachedPageWindow(
  file: StoreAsyncBuffer,
  extent: { start: number; end: number },
  offset: number,
  scope: MemoryScope,
  cache: { cache: DecodedColumnCache; path: string },
): Promise<PreparedPageWindow | undefined> {
  const scratch = scope.childScope();
  try {
    const key = scratch.allocate(
      c.string + c.stringCodeUnit * (6 * (cache.path.length + (file.etag?.length ?? 0)) + 256),
      "page header cache key",
      () =>
        JSON.stringify([
          "page-header",
          cache.path,
          file.byteLength,
          file.etag ?? null,
          extent.start,
          extent.end,
          offset,
        ]),
    ).value;
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
