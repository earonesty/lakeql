import type { MemoryScope } from "./memory-scope.js";
import type { ObjectHead, ObjectStore } from "./store.js";

/** Metadata remains live through the receiving scope; legacy adapter allocations
 * remain external. Owned callbacks must admit their probe and metadata backing.
 */
export async function readObjectHead(
  store: ObjectStore,
  path: string,
  scope?: MemoryScope,
): Promise<ObjectHead | null> {
  if (scope === undefined || store.headOwned === undefined) return store.head(path);
  const owner = await store.headOwned(path, scope);
  if (owner === null) return null;
  try {
    return scope.adopt(owner).value;
  } catch (error) {
    owner.release();
    throw error;
  }
}
