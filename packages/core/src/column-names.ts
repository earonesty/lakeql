import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";

/** Deduplicate read columns with admitted collection and output storage. */
export function admittedColumnNames(
  parent: MemoryScope,
  collect: (columns: { add(name: string): void }) => void,
  sorted = false,
): string[] {
  const output = parent.childScope();
  const scratch = output.childScope();
  try {
    const names = scratch.allocate(
      c.object,
      "read column directory",
      () => new Set<string>(),
    ).value;
    collect({
      add(name) {
        if (names.has(name)) return;
        scratch.allocate(c.property, "read column directory entry", () => undefined);
        output.allocate(
          c.string + name.length * c.stringCodeUnit,
          "read column name",
          () => undefined,
        );
        names.add(name);
      },
    });
    const list = output.allocate(c.array + names.size * c.arraySlot, "read column list", () => [
      ...names,
    ]);
    if (sorted) {
      const ordering = scratch.reserve(
        c.array + names.size * 2 * c.arraySlot,
        "read column ordering",
      );
      try {
        list.value.sort();
      } finally {
        ordering.release();
      }
    }
    const owned = output.retainAllocations(list.value);
    try {
      return parent.adopt(owned).value;
    } catch (error) {
      owned.release();
      throw error;
    }
  } finally {
    scratch.dispose();
    output.dispose();
  }
}
