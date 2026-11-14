import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Vector } from "./batch.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

/** Identity reachability over immutable vector descriptors, not arbitrary payload
 * objects. Set entries and the iterative queue are admitted before construction.
 */
export function reachableVectors(roots: Iterable<Vector>, scope: MemoryScope): ReadonlySet<Vector> {
  const construction = scope.childScope();
  interface Pending {
    vector: Vector;
    next: OwnedMemory<Pending> | undefined;
  }
  let head: OwnedMemory<Pending> | undefined;
  let tail: OwnedMemory<Pending> | undefined;
  try {
    const seen = construction.allocate(
      c.object,
      "vector reachability set",
      () => new Set<Vector>(),
    ).value;
    const add = (vector: Vector): void => {
      if (seen.has(vector)) return;
      construction.allocate(c.property, "vector reachability entry", () => {
        seen.add(vector);
      });
      // Terminal descriptors are already fully discovered. Queue only nodes
      // with vector children; payload storage is indexed separately.
      if (
        vector.type !== "dict" &&
        vector.type !== "union" &&
        vector.type !== "list" &&
        vector.type !== "map" &&
        vector.type !== "struct"
      )
        return;
      const entry: OwnedMemory<Pending> = construction.allocate(
        c.object + 2 * c.property,
        "vector reachability queue",
        () => ({ vector, next: undefined }),
      );
      if (tail === undefined) head = entry;
      else tail.value.next = entry;
      tail = entry;
    };
    for (const vector of roots) add(vector);
    while (head !== undefined) {
      const current: OwnedMemory<Pending> = head;
      const vector = current.value.vector;
      head = current.value.next;
      if (head === undefined) tail = undefined;
      current.release();
      switch (vector.type) {
        case "dict":
          add(vector.dictionary);
          break;
        case "union":
          for (const child of vector.children) add(child);
          break;
        case "list":
          add(vector.child);
          break;
        case "map":
          add(vector.keys);
          add(vector.values);
          break;
        case "struct":
          for (const name in vector.fields)
            if (Object.hasOwn(vector.fields, name)) {
              const field = vector.fields[name];
              if (field !== undefined) add(field);
            }
          break;
      }
    }
    scope.adopt(construction.retainAllocations(undefined));
    return seen;
  } finally {
    construction.dispose();
  }
}

export interface VectorStorageIndex {
  readonly vectors: ReadonlySet<Vector>;
  readonly buffers: ReadonlySet<ArrayBufferLike>;
  readonly childDirectories: ReadonlySet<readonly Vector[]>;
}

/** Index aliases once; later allocation checks require only identity lookups.
 * The caller must keep the supplied descriptor set alive with this index.
 */
export function indexVectorStorage(
  vectors: ReadonlySet<Vector>,
  scope: MemoryScope,
): VectorStorageIndex {
  const construction = scope.childScope();
  try {
    const buffers = construction.allocate(
      c.object,
      "vector backing index",
      () => new Set<ArrayBufferLike>(),
    ).value;
    const childDirectories = construction.allocate(
      c.object,
      "vector directory index",
      () => new Set<readonly Vector[]>(),
    ).value;
    const addView = (value: unknown): void => {
      if (!ArrayBuffer.isView(value) || buffers.has(value.buffer)) return;
      construction.allocate(c.property, "vector backing entry", () => {
        buffers.add(value.buffer);
      });
    };
    for (const vector of vectors) {
      if ("valid" in vector) addView(vector.valid);
      if ("offsets" in vector) addView(vector.offsets);
      if ("values" in vector) addView(vector.values);
      if (vector.type === "union") {
        addView(vector.tags);
        if (!childDirectories.has(vector.children))
          construction.allocate(c.property, "vector directory entry", () => {
            childDirectories.add(vector.children);
          });
      } else if (vector.type === "dict") addView(vector.indices);
      else if (vector.type === "binary-view" || vector.type === "utf8-view") {
        addView(vector.data);
        addView(vector.ranges);
      } else if (vector.type === "timestamp96") {
        addView(vector.low);
        addView(vector.high);
      } else if (vector.type === "binary") for (const value of vector.values) addView(value);
    }
    const result = construction.allocate(c.object + 3 * c.property, "vector storage index", () => ({
      vectors,
      buffers,
      childDirectories,
    })).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
