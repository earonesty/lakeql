import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { admitVectorBuffer, type VectorAllocator } from "./vector-buffer.js";

/** One owned SQL truth mask: admit backing and its typed-array view together. */
export function allocatePredicateMask(length: number, scope?: VectorAllocator): Uint8Array {
  if (!Number.isSafeInteger(length) || length < 0 || !Number.isSafeInteger(length + c.view))
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid predicate mask length", { length });
  return admitVectorBuffer(scope, c.view + length, () => new Uint8Array(length));
}
