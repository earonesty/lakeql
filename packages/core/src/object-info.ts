import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";
import type { ObjectInfo } from "./store.js";

/** Independent metadata records preserve transport hints across cache boundaries. */
export function cloneObjectInfos(
  objects: readonly ObjectInfo[],
  parent?: MemoryScope,
): ObjectInfo[] {
  const output = parent?.childScope();
  try {
    const createList = () => [] as ObjectInfo[];
    const result =
      output === undefined
        ? createList()
        : output.allocate(
            c.array + objects.length * c.arraySlot,
            "object metadata list",
            createList,
          ).value;
    for (const object of objects) {
      result.push(cloneObjectInfo(object, output));
    }
    if (output !== undefined && parent !== undefined) {
      const owner = output.retainAllocations(result);
      try {
        return parent.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return result;
  } finally {
    output?.dispose();
  }
}

export function cloneObjectInfo(object: ObjectInfo, scope?: MemoryScope): ObjectInfo {
  const create = (): ObjectInfo => {
    const cloned: ObjectInfo = { path: object.path, size: object.size };
    if (object.etag !== undefined) cloned.etag = object.etag;
    if (object.lastModified !== undefined) cloned.lastModified = new Date(object.lastModified);
    if (object.rangeReadMode !== undefined) cloned.rangeReadMode = object.rangeReadMode;
    return cloned;
  };
  const bytes =
    c.object +
    5 * c.property +
    c.string +
    object.path.length * c.stringCodeUnit +
    (object.etag === undefined ? 0 : c.string + object.etag.length * c.stringCodeUnit) +
    (object.lastModified === undefined ? 0 : c.timestamp);
  return scope === undefined ? create() : scope.allocate(bytes, "object metadata", create).value;
}
