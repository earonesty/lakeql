import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";

/** Parse segments without allocating an array proportional to the entire path. */
export function parseHivePartitions(path: string, parent?: MemoryScope): Record<string, string> {
  const output = parent?.childScope();
  try {
    const values: Record<string, string> = output
      ? output.allocate(c.object, "hive partitions", () => ({})).value
      : {};
    let start = 0;
    while (start < path.length) {
      const slash = path.indexOf("/", start);
      const end = slash < 0 ? path.length : slash;
      let equals = start;
      while (equals < end && path.charCodeAt(equals) !== 61) equals++;
      if (equals > start && equals < end - 1) {
        // Decoded UTF-16 length cannot exceed the encoded substring length.
        // Keep conservative charges for overwritten duplicate keys as well.
        output?.allocate(
          c.property + 2 * c.string + (end - start - 1) * c.stringCodeUnit,
          "hive partition entry",
          () => undefined,
        );
        const encoded = output?.reserve(
          c.string + (end - equals - 1) * c.stringCodeUnit,
          "encoded hive partition value",
        );
        try {
          const key = path.slice(start, equals);
          const value = decodeURIComponent(path.slice(equals + 1, end));
          Object.defineProperty(values, key, {
            value,
            enumerable: true,
            configurable: true,
            writable: true,
          });
        } finally {
          encoded?.release();
        }
      }
      start = end + 1;
    }
    if (output && parent) {
      const owner = output.retainAllocations(values);
      try {
        return parent.adopt(owner).value;
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return values;
  } finally {
    output?.dispose();
  }
}
