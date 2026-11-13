import { type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import type { MemoryScope } from "./memory-scope.js";
import { planUnionConcat } from "./union-concat.js";

type Struct = Extract<Vector, { type: "struct" }>;
/** Concatenate matching struct layouts while borrowing field payloads.
 * New per-field routing and parent validity are admitted atomically.
 */
export function concatStructRouting(vectors: readonly Struct[], scope: MemoryScope): Struct {
  const first = vectors[0];
  const invalid = () => new LakeqlError("LAKEQL_TYPE_ERROR", "Incompatible struct routing inputs");
  if (!first) throw invalid();
  let fieldsCount = 0;
  for (const name in first.fields) if (Object.hasOwn(first.fields, name)) fieldsCount++;
  let length = 0,
    nullable = false;
  for (const vector of vectors) {
    if (!Number.isSafeInteger(vector.length) || vector.length < 0) throw invalid();
    let count = 0;
    for (const name in vector.fields) {
      if (!Object.hasOwn(vector.fields, name)) continue;
      const field = vector.fields[name];
      if (!Object.hasOwn(first.fields, name) || !field || vectorLength(field) !== vector.length)
        throw invalid();
      count++;
    }
    if (count !== fieldsCount || (vector.valid && vector.valid.length !== vector.length))
      throw invalid();
    length += vector.length;
    nullable ||= vector.valid !== undefined;
  }
  if (!Number.isSafeInteger(length) || length > 0xffffffff) throw invalid();
  if (vectors.length === 1) return first;
  const construction = scope.childScope();
  const scratch = construction.childScope();
  try {
    const inputs = scratch.allocate(
      c.array + vectors.length * c.arraySlot,
      "struct field concat inputs",
      () => new Array<Vector>(vectors.length),
    ).value;
    const fields = construction.allocate(
      c.object + fieldsCount * c.property,
      "routed struct fields",
      (): Record<string, Vector> => Object.create(null),
    ).value;
    for (const name in first.fields) {
      if (!Object.hasOwn(first.fields, name)) continue;
      for (let i = 0; i < vectors.length; i++) inputs[i] = vectors[i]?.fields[name] as Vector;
      fields[name] = planUnionConcat(inputs).execute(construction);
    }
    const valid = nullable
      ? construction.allocate(
          length + c.view,
          "routed struct validity",
          () => new Uint8Array(length),
        ).value
      : undefined;
    if (valid) {
      let offset = 0;
      for (const vector of vectors) {
        if (vector.valid) valid.set(vector.valid, offset);
        else valid.fill(1, offset, offset + vector.length);
        offset += vector.length;
      }
    }
    const result = construction.allocate(
      c.object + 4 * c.property,
      "routed struct",
      (): Struct => ({ type: "struct", fields, length, ...(valid ? { valid } : {}) }),
    ).value;
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    scratch.dispose();
    construction.dispose();
  }
}
