import { type Batch, type Vector, vectorLength } from "./batch.js";
import { type CollectionVector, collectionConcatInputs } from "./collection-concat.js";
import { LakeqlError } from "./errors.js";
import {
  adaptivePackedDictionaryPlan,
  packedVectorConcatPlan,
  scalarDictionaryConcatPlan,
} from "./packed-gather.js";
import { planSelectedConcat, type SelectedConcatPlan } from "./selected-concat.js";
import { structConcatInputs } from "./struct-concat.js";
import { hasUnionLeaf, planUnionConcat } from "./union-concat.js";
import { selectVector } from "./vector-selection.js";

/** Exact new typed allocation extent for supported fixed-width, packed and dictionary concat. Inputs must remain
 * charged separately through construction. Undefined means the physical shapes
 * require a different planner; it is not permission to await individual buffers.
 */
export interface ConcatColumnPlan {
  union?: ReturnType<typeof planUnionConcat>;
  selected?: SelectedConcatPlan;
  sharedNullDictionary?: boolean;
  scalar?: NonNullable<ReturnType<typeof scalarDictionaryConcatPlan>>;
  packed?: NonNullable<ReturnType<typeof adaptivePackedDictionaryPlan>>;
  directPacked?: NonNullable<ReturnType<typeof packedVectorConcatPlan>>;
  children?: ConcatPlan;
}

/** Operation-local plan; inputs must remain immutable through synchronous execution. */
export interface ConcatPlan {
  bytes: number;
  columns: ReadonlyMap<string, ConcatColumnPlan>;
}

export function concatAllocationBytes(batches: readonly Batch[]): number | undefined {
  return planConcat(batches)?.bytes;
}

export function planConcat(batches: readonly Batch[]): ConcatPlan | undefined {
  const columns = new Map<string, ConcatColumnPlan>();
  const first = batches[0];
  if (first === undefined) return { bytes: 0, columns };
  const names = Object.keys(first.columns);
  if (batches.some((batch) => Object.keys(batch.columns).length !== names.length)) return undefined;
  let bytes = 0;
  // A column identifies its entire index sequence in the immutable input list.
  // Keep one candidate name instead of another batch-sized array per column.
  const indexCopies = new Map<Uint32Array, string>();
  for (const name of names) {
    const initial = first.columns[name];
    if (initial === undefined) return undefined;
    let hasUnion = false;
    let hasNull = false;
    let hasConcrete = false;
    let differentDictionary = false;
    for (const batch of batches) {
      const vector = batch.columns[name];
      if (vector === undefined) return undefined;
      hasUnion ||= hasUnionLeaf(vector);
      hasNull ||= vector.type === "null";
      hasConcrete ||= vector.type !== "null";
      if (initial.type === "dict")
        differentDictionary ||= vector.type !== "dict" || vector.dictionary !== initial.dictionary;
    }
    if (hasUnion) {
      const union = planUnionConcat(batches.map((batch) => batch.columns[name] as Vector));
      bytes = checkedAdd(bytes, union.bytes);
      columns.set(name, { union });
      continue;
    }
    if (hasNull && hasConcrete) {
      const vectors = batches.map((batch) => batch.columns[name]);
      if (vectors.some((vector) => vector === undefined)) return undefined;
      const typed = vectors as Vector[];
      const concrete = typed.find((vector) => vector.type !== "null");
      const shared =
        concrete?.type === "dict" &&
        typed.every(
          (vector) =>
            vector.type === "null" ||
            (vector.type === "dict" && vector.dictionary === concrete.dictionary),
        );
      // Null normalization allocates indices+mask for each null source, followed
      // by one concatenated index+mask output. Existing payload stays pinned.
      const sharedBytes = shared
        ? typed.reduce(
            (sum, vector) => sum + vectorLength(vector) * (vector.type === "null" ? 10 : 5),
            0,
          )
        : Infinity;
      const selected = planSelectedConcat(typed.map(selectVector));
      if (shared && (selected === undefined || sharedBytes <= selected.bytes)) {
        bytes = checkedAdd(bytes, sharedBytes);
        columns.set(name, { sharedNullDictionary: true });
      } else {
        if (selected === undefined) return undefined;
        bytes = checkedAdd(bytes, selected.bytes);
        columns.set(name, { selected });
      }
      continue;
    }
    if (initial.type === "dict" && differentDictionary) {
      const vectors = batches.map((batch) => batch.columns[name]);
      if (vectors.some((vector) => vector?.type !== "dict")) return undefined;
      const dictionaries = vectors as Extract<Vector, { type: "dict" }>[];
      const plan = scalarDictionaryConcatPlan(
        dictionaries,
        dictionaries.some((vector) => vector.valid !== undefined),
      );
      if (plan === undefined) {
        const packed = adaptivePackedDictionaryPlan(
          dictionaries,
          dictionaries.some((vector) => vector.valid !== undefined),
        );
        if (packed === undefined) {
          const selected = planSelectedConcat(dictionaries.map(selectVector));
          if (selected === undefined) return undefined;
          bytes = checkedAdd(bytes, selected.bytes);
          columns.set(name, { selected });
        } else {
          bytes = checkedAdd(bytes, packed.totalBytes);
          columns.set(name, { packed });
        }
      } else {
        bytes = checkedAdd(bytes, plan.bytes);
        columns.set(name, { scalar: plan });
      }
      continue;
    }
    if (initial.type === "utf8" || initial.type === "binary") {
      const vectors = batches.map((batch) => batch.columns[name]);
      if (vectors.some((vector) => vector === undefined)) return undefined;
      const selected = planSelectedConcat((vectors as Vector[]).map(selectVector));
      if (selected === undefined) return undefined;
      bytes = checkedAdd(bytes, selected.bytes);
      columns.set(name, { selected });
      continue;
    }
    if (initial.type === "utf8-view" || initial.type === "binary-view") {
      const vectors = batches.map((batch) => batch.columns[name]);
      if (vectors.some((vector) => vector?.type !== initial.type)) return undefined;
      const packed = packedVectorConcatPlan(
        vectors as Vector[],
        vectors.some(
          (vector) => vector !== undefined && "valid" in vector && vector.valid !== undefined,
        ),
      );
      if (packed === undefined) return undefined;
      if (packed.bytes > 0xffffffff)
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Packed concat exceeds Uint32 byte offsets",
        );
      bytes = checkedAdd(bytes, packed.totalBytes);
      columns.set(name, { directPacked: packed });
      continue;
    }
    if (initial.type === "list" || initial.type === "map") {
      const vectors = batches.map((batch) => batch.columns[name]);
      if (vectors.some((vector) => vector?.type !== initial.type)) return undefined;
      const plan = collectionConcatInputs(vectors as CollectionVector[]);
      const children = planConcat(plan.batches);
      if (children === undefined) return undefined;
      bytes = checkedAdd(bytes, children.bytes);
      columns.set(name, { children });
      bytes = checkedAdd(bytes, (plan.rows + 1) * 4);
      if (
        vectors.some(
          (vector) => vector !== undefined && "valid" in vector && vector.valid !== undefined,
        )
      )
        bytes = checkedAdd(bytes, plan.rows);
      continue;
    }
    if (initial.type === "struct") {
      const vectors = batches.map((batch) => batch.columns[name]);
      if (vectors.some((vector) => vector === undefined)) return undefined;
      const inputs = structConcatInputs(vectors as Vector[]);
      if (inputs === undefined) return undefined;
      const fields = planConcat(inputs);
      if (fields === undefined) return undefined;
      bytes = checkedAdd(bytes, fields.bytes);
      columns.set(name, { children: fields });
      if (
        vectors.some(
          (vector) => vector !== undefined && "valid" in vector && vector.valid !== undefined,
        )
      )
        for (const input of inputs) bytes = checkedAdd(bytes, input.rowCount);
      continue;
    }
    if (initial.type === "timestamp96") {
      const inputs = batches.map((batch) => batch.columns[name]);
      if (inputs.some((vector) => vector === undefined)) return undefined;
      const selected = planSelectedConcat((inputs as Vector[]).map(selectVector));
      if (selected === undefined) return undefined;
      columns.set(name, { selected });
      bytes = checkedAdd(bytes, selected.bytes);
      continue;
    }
    if (
      initial.type === "timestamp" &&
      batches.some((batch) => {
        const vector = batch.columns[name];
        return (
          vector?.type === "timestamp96" ||
          (vector?.type === "timestamp" &&
            (vector.unit !== initial.unit || vector.isAdjustedToUTC !== initial.isAdjustedToUTC))
        );
      })
    ) {
      const inputs = batches.map((batch) => batch.columns[name]);
      if (inputs.some((vector) => vector === undefined)) return undefined;
      const selected = planSelectedConcat((inputs as Vector[]).map(selectVector));
      if (selected === undefined) return undefined;
      columns.set(name, { selected });
      bytes = checkedAdd(bytes, selected.bytes);
      continue;
    }
    let length = 0;
    let nullable = false;
    const previous = initial.type === "dict" ? indexCopies.get(initial.indices) : undefined;
    let sharesIndices = previous !== undefined;
    let indexBytes = 0;
    for (const batch of batches) {
      const vector = batch.columns[name];
      if (vector === undefined || vector.type !== initial.type) return undefined;
      if (vector.type === "dict" && initial.type === "dict") {
        if (vector.dictionary !== initial.dictionary) return undefined;
        indexBytes = checkedAdd(indexBytes, vector.indices.byteLength);
        const candidate = previous === undefined ? undefined : batch.columns[previous];
        if (candidate?.type !== "dict" || candidate.indices !== vector.indices)
          sharesIndices = false;
      }
      const size = vector.type === "dict" ? 0 : fixedBackingBytes(vector);
      if (size === undefined) return undefined;
      bytes = checkedAdd(bytes, size);
      length = checkedAdd(length, vectorLength(vector));
      nullable ||= "valid" in vector && vector.valid !== undefined;
    }
    if (nullable) bytes = checkedAdd(bytes, length);
    if (initial.type === "dict") {
      if (!sharesIndices) {
        bytes = checkedAdd(bytes, indexBytes);
        indexCopies.set(initial.indices, name);
      }
    }
  }
  return { bytes, columns };
}

function fixedBackingBytes(vector: Vector): number | undefined {
  switch (vector.type) {
    case "null":
      return 0;
    case "bool":
    case "u8":
    case "i32":
    case "u32":
    case "f32":
    case "f64":
    case "i64":
    case "u64":
    case "timestamp":
      return vector.values.byteLength;
    default:
      return undefined;
  }
}

function checkedAdd(left: number, right: number): number {
  const sum = left + right;
  if (!Number.isSafeInteger(sum) || sum < 0)
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Concat allocation extent exceeds safe integer capacity",
    );
  return sum;
}
