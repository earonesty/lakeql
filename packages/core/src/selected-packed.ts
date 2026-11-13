import type { Vector } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import type { SelectedConcatPlan } from "./selected-concat.js";
import { losslessUtf8Bytes } from "./utf8-size.js";
import { admitVectorBuffer } from "./vector-buffer.js";
import { selectVector, type VectorSelection } from "./vector-selection.js";

const encoder = new TextEncoder();
export function packedSelectionKind(vector: Vector): "text" | "binary" | undefined {
  if (vector.type === "utf8" || vector.type === "utf8-view") return "text";
  if (vector.type === "binary" || vector.type === "binary-view") return "binary";
  return undefined;
}

function range(vector: Vector, index: number): readonly [number, number] | undefined {
  if (vector.type === "utf8") {
    const bytes = losslessUtf8Bytes(vector.values[index] ?? "");
    return bytes === undefined ? undefined : [0, bytes];
  }
  if (vector.type === "binary") return [0, vector.values[index]?.byteLength ?? 0];
  if (vector.type !== "utf8-view" && vector.type !== "binary-view") return undefined;
  const row = (vector.rowOffset ?? 0) + index;
  const start =
    vector.ranges === undefined ? row * (vector.fixedLength ?? 0) : (vector.ranges[row * 2] ?? 0);
  const length =
    vector.ranges === undefined ? (vector.fixedLength ?? 0) : (vector.ranges[row * 2 + 1] ?? 0);
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(length) ||
    start < 0 ||
    length < 0 ||
    start + length > vector.data.byteLength
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Selected packed range exceeds payload");
  return [start, length];
}

/** Select expansion or a detached union by complete output cost. Neither path
 * builds row-index scratch; immutable selections are replayed into final backing.
 */
export function planSelectedPacked(
  inputs: readonly VectorSelection[],
  allowUnion = true,
): SelectedConcatPlan | undefined {
  const first = inputs.find((input) => input.leaf.type !== "null");
  if (first === undefined) return undefined;
  const kind = packedSelectionKind(first.leaf);
  if (
    kind === undefined ||
    inputs.some(({ leaf }) => leaf.type !== "null" && packedSelectionKind(leaf) !== kind)
  )
    return undefined;
  const rows = inputs.reduce((sum, input) => sum + input.length, 0);
  let payload = 0;
  let nullable = false;
  let lossless = true;
  for (const input of inputs)
    input.forEach((index) => {
      if (index < 0) {
        nullable = true;
        return;
      }
      const span = range(input.leaf, index);
      if (span === undefined) lossless = false;
      else payload += span[1];
    });
  if (!lossless) return undefined;
  const maskBytes = nullable ? rows : 0;
  const expandedBytes = rows * 8 + payload + maskBytes;
  if (!Number.isSafeInteger(expandedBytes))
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Selected packed extent exceeds safe capacity",
    );
  const valid = (scope?: MemoryScope) =>
    nullable ? admitVectorBuffer(scope, rows, () => new Uint8Array(rows)) : undefined;
  if (allowUnion) {
    const unique = [
      ...new Set(inputs.map((input) => input.leaf).filter((leaf) => leaf.type !== "null")),
    ];
    const offsets = new Map<Vector, number>();
    let entries = 0;
    const dictionaries = unique.map((leaf) => {
      offsets.set(leaf, entries);
      const selected = selectVector(leaf);
      entries += selected.length;
      return selected;
    });
    if (entries <= 0xffffffff && entries * 8 + rows * 4 + maskBytes < expandedBytes) {
      const dictionary = planSelectedPacked(dictionaries, false);
      const bytes = (dictionary?.bytes ?? Infinity) + rows * 4 + maskBytes;
      if (dictionary !== undefined && bytes < expandedBytes)
        return {
          bytes,
          execute(scope) {
            const values = dictionary.execute(scope);
            const indices = admitVectorBuffer(scope, rows * 4, () => new Uint32Array(rows));
            const validity = valid(scope);
            let row = 0;
            for (const input of inputs)
              input.forEach((index) => {
                indices[row] = index < 0 ? 0 : (offsets.get(input.leaf) ?? 0) + index;
                if (validity !== undefined) validity[row] = index < 0 ? 0 : 1;
                row++;
              });
            return {
              type: "dict",
              indices,
              dictionary: values,
              ...(validity === undefined ? {} : { valid: validity }),
            };
          },
        };
    }
  }
  if (payload > 0xffffffff) {
    if (!allowUnion) return undefined;
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Selected packed payload exceeds Uint32 offsets",
    );
  }
  return {
    bytes: expandedBytes,
    execute(scope) {
      const ranges = admitVectorBuffer(scope, rows * 8, () => new Uint32Array(rows * 2));
      const data = admitVectorBuffer(scope, payload, () => new Uint8Array(payload));
      const validity = valid(scope);
      let row = 0;
      let offset = 0;
      for (const input of inputs)
        input.forEach((index) => {
          const leaf = input.leaf;
          const span = index < 0 ? [0, 0] : range(leaf, index);
          if (span === undefined) throw new Error("Selected text changed after planning");
          const [start = 0, length = 0] = span;
          ranges[row * 2] = offset;
          ranges[row * 2 + 1] = length;
          if (index >= 0) {
            if (leaf.type === "utf8")
              encoder.encodeInto(leaf.values[index] ?? "", data.subarray(offset, offset + length));
            else if (leaf.type === "binary")
              data.set(leaf.values[index] ?? new Uint8Array(), offset);
            else if (leaf.type === "utf8-view" || leaf.type === "binary-view")
              data.set(leaf.data.subarray(start, start + length), offset);
          }
          if (validity !== undefined) validity[row] = index < 0 ? 0 : 1;
          offset += length;
          row++;
        });
      return {
        type: kind === "text" ? "utf8-view" : "binary-view",
        data,
        ranges,
        length: rows,
        ...(validity === undefined ? {} : { valid: validity }),
      };
    },
  };
}
