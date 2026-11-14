import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Vector, vectorFromSource } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import type { MemoryScope } from "./memory-scope.js";

/** A constant uses ordinary dictionary capabilities, with one admitted payload
 * and a bounded index vector rather than repeated scalar materialization. */
export function constantVector(value: Scalar, rowCount: number, parent: MemoryScope): Vector {
  if (!Number.isSafeInteger(rowCount) || rowCount < 0 || rowCount > 0x100000000)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid constant vector row count");
  const scope = parent.childScope();
  try {
    // vectorFromSource admits string slots but borrows their immutable backing.
    // Constants originate outside the scanner, so retain that backing explicitly.
    if (rowCount > 0 && typeof value === "string") {
      scope.allocate(
        c.string + value.length * c.stringCodeUnit,
        "constant text backing",
        () => value,
      );
    }
    const dictionary = vectorFromSource(
      { length: rowCount === 0 ? 0 : 1, get: () => value },
      scope,
    );
    const indices = scope.allocate(
      c.view + rowCount * Uint32Array.BYTES_PER_ELEMENT,
      "constant vector indices",
      () => new Uint32Array(rowCount),
    ).value;
    const vector = scope.allocate(
      c.object + 3 * c.property,
      "constant vector",
      (): Vector => ({ type: "dict", dictionary, indices }),
    ).value;
    parent.adopt(scope.retainAllocations(vector));
    return vector;
  } finally {
    scope.dispose();
  }
}
