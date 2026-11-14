import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Vector } from "./batch.js";
import { DecimalValue, decimalValueAllocationBytes, requireDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import { allocateInt96Storage, readInt96, writeInt96 } from "./int96-storage.js";
import type { MemoryScope } from "./memory-scope.js";
import { setRecordValue } from "./record-value.js";
import { packedSelectionKind, planSelectedPacked } from "./selected-packed.js";
import { admitVectorBuffer } from "./vector-buffer.js";
import {
  forEachSelectedRange,
  selectCollectionChild,
  selectStructField,
  selectVector,
  type VectorSelection,
} from "./vector-selection.js";

const decimalNull = new DecimalValue(0n, 0);

const MIN_TIMESTAMP_EPOCH = -(1n << 63n);
const MAX_TIMESTAMP_EPOCH = (1n << 63n) - 1n;

/** Complete detached allocation plan over replayable selections. Unsupported
 * leaf representations return undefined before allocating output backing.
 */
export interface SelectedConcatPlan {
  readonly bytes: number;
  execute(scope?: MemoryScope): Vector;
}

export function planSelectedConcat(
  inputs: readonly VectorSelection[],
): SelectedConcatPlan | undefined {
  const first = inputs.find((input) => input.leaf.type !== "null")?.leaf;
  const rows = inputs.reduce((sum, input) => sum + input.length, 0);
  if (!Number.isSafeInteger(rows))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Selected concat exceeds safe row capacity");
  if (first === undefined) {
    for (const input of inputs) input.forEach(() => {});
    return { bytes: 0, execute: () => ({ type: "null", length: rows }) };
  }
  if (packedSelectionKind(first) !== undefined) return planSelectedPacked(inputs);
  const timestampShape = (
    vector: Vector,
  ): vector is Extract<Vector, { type: "timestamp" | "timestamp96" }> =>
    vector.type === "timestamp" || vector.type === "timestamp96";
  if (
    inputs.some(
      (input) =>
        input.leaf.type !== "null" &&
        input.leaf.type !== first.type &&
        !(timestampShape(first) && timestampShape(input.leaf)),
    )
  )
    return undefined;
  let nullable = false;
  for (const input of inputs) {
    if (!input.mayHaveNull) continue;
    input.forEach((index) => {
      if (index < 0) nullable = true;
    });
  }
  const validityBytes = nullable ? rows : 0;
  const validity = (scope?: MemoryScope) => {
    if (!nullable) return undefined;
    const valid = admitVectorBuffer(scope, rows, () => new Uint8Array(rows));
    let offset = 0;
    for (const input of inputs) {
      input.forEach((index, row) => {
        valid[offset + row] = index < 0 ? 0 : 1;
      });
      offset += input.length;
    }
    return valid;
  };
  const withValid = (vector: Vector, valid: Uint8Array | undefined): Vector =>
    valid === undefined ? vector : ({ ...vector, valid } as Vector);
  const checked = (bytes: number, execute: SelectedConcatPlan["execute"]): SelectedConcatPlan => {
    if (!Number.isSafeInteger(bytes))
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Selected concat exceeds safe byte capacity",
      );
    return { bytes, execute };
  };
  if (first.type === "decimal") {
    let storageBytes = c.array + rows * c.arraySlot;
    for (const input of inputs) {
      input.forEach((index) => {
        if (index >= 0 && input.leaf.type === "decimal")
          storageBytes += decimalValueAllocationBytes(
            requireDecimalValue(input.leaf.values[index]).coefficient,
          );
      });
    }
    return checked(storageBytes + validityBytes, (scope) => {
      const valid = validity(scope);
      const values = admitVectorBuffer(scope, storageBytes, () => {
        const values = new Array<DecimalValue>(rows);
        let offset = 0;
        for (const input of inputs) {
          input.forEach((index, row) => {
            const value =
              index >= 0 && input.leaf.type === "decimal" ? input.leaf.values[index] : undefined;
            values[offset + row] =
              value === undefined ? decimalNull : new DecimalValue(value.coefficient, value.scale);
          });
          offset += input.length;
        }
        return values;
      });
      return withValid({ type: "decimal", values }, valid);
    });
  }
  if (first.type === "struct") {
    const names = Object.keys(first.fields);
    if (
      inputs.some(
        ({ leaf }) =>
          leaf.type === "struct" &&
          (Object.keys(leaf.fields).length !== names.length ||
            names.some((name) => !Object.hasOwn(leaf.fields, name))),
      )
    )
      return undefined;
    const fields: [string, SelectedConcatPlan][] = [];
    let bytes = validityBytes;
    for (const name of names) {
      const children = inputs.map((input) =>
        input.leaf.type === "null" ? input : selectStructField(input, name),
      );
      const plan = planSelectedConcat(children);
      if (plan === undefined) return undefined;
      fields.push([name, plan]);
      bytes += plan.bytes;
    }
    return checked(bytes, (scope) => {
      const valid = validity(scope);
      const output: Record<string, Vector> = {};
      for (const [name, plan] of fields) setRecordValue(output, name, plan.execute(scope));
      return withValid({ type: "struct", length: rows, fields: output }, valid);
    });
  }
  if (first.type === "list" || first.type === "map") {
    const names = first.type === "list" ? (["child"] as const) : (["keys", "values"] as const);
    const children: SelectedConcatPlan[] = [];
    let bytes = validityBytes + (rows + 1) * 4;
    let entries = 0;
    for (const input of inputs) {
      if (input.leaf.type === "null") continue;
      entries += input.collectionLength();
    }
    if (entries > 0x7fffffff)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Selected concat exceeds Int32 offsets");
    for (const name of names) {
      const selections = inputs.map((input) =>
        input.leaf.type === "null"
          ? selectVector({ type: "null", length: 0 })
          : selectCollectionChild(input, name),
      );
      const plan = planSelectedConcat(selections);
      if (plan === undefined) return undefined;
      children.push(plan);
      bytes += plan.bytes;
    }
    return checked(bytes, (scope) => {
      const valid = validity(scope);
      const offsets = admitVectorBuffer(scope, (rows + 1) * 4, () => new Int32Array(rows + 1));
      let row = 0;
      let position = 0;
      for (const input of inputs) {
        if (input.leaf.type === "null") {
          for (let i = 0; i < input.length; i++) offsets[++row] = position;
        } else
          forEachSelectedRange(input, (start, end) => {
            position += end - start;
            offsets[++row] = position;
          });
      }
      const child = children[0]?.execute(scope);
      if (child === undefined) throw new Error("Missing selected child plan");
      if (first.type === "list") return withValid({ type: "list", offsets, child }, valid);
      const values = children[1]?.execute(scope);
      if (values === undefined) throw new Error("Missing selected values plan");
      return withValid({ type: "map", offsets, keys: child, values }, valid);
    });
  }
  if (timestampShape(first)) {
    let unit = first.unit;
    let wide = first.type === "timestamp96";
    for (const { leaf } of inputs) {
      if (!timestampShape(leaf)) continue;
      wide ||= leaf.type === "timestamp96";
      if (leaf.isAdjustedToUTC !== first.isAdjustedToUTC)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Incompatible timestamp UTC semantics");
      if (leaf.unit === "nanos" || (leaf.unit === "micros" && unit === "millis")) unit = leaf.unit;
    }
    const scale = (source: typeof unit): bigint =>
      source === unit ? 1n : source === "millis" && unit === "nanos" ? 1_000_000n : 1_000n;
    const epochAt = (
      leaf: Extract<Vector, { type: "timestamp" | "timestamp96" }>,
      index: number,
    ) => (leaf.type === "timestamp96" ? readInt96(leaf, index) : (leaf.values[index] ?? 0n));
    const min = -(1n << 95n),
      max = (1n << 95n) - 1n;
    for (const input of inputs) {
      const leaf = input.leaf;
      if (!timestampShape(leaf)) continue;
      const factor = scale(leaf.unit);
      if (factor === 1n) continue;
      input.forEach((index) => {
        if (index < 0) return;
        const epoch = epochAt(leaf, index) * factor;
        if (epoch < min || epoch > max)
          throw new LakeqlError(
            "LAKEQL_TYPE_ERROR",
            "Timestamp exceeds signed 96-bit vector capacity",
          );
        wide ||= epoch < MIN_TIMESTAMP_EPOCH || epoch > MAX_TIMESTAMP_EPOCH;
      });
    }
    return checked(validityBytes + rows * (wide ? 12 : 8), (scope) => {
      const valid = validity(scope);
      const output: Extract<Vector, { type: "timestamp" | "timestamp96" }> = wide
        ? {
            type: "timestamp96",
            ...allocateInt96Storage(rows, scope),
            unit,
            isAdjustedToUTC: first.isAdjustedToUTC,
          }
        : {
            type: "timestamp",
            values: admitVectorBuffer(scope, rows * 8, () => new BigInt64Array(rows)),
            unit,
            isAdjustedToUTC: first.isAdjustedToUTC,
          };
      let offset = 0;
      for (const input of inputs) {
        const leaf = input.leaf;
        if (timestampShape(leaf)) {
          const factor = scale(leaf.unit);
          input.forEach((index, row) => {
            if (index < 0) return;
            if (output.type === "timestamp96" && leaf.type === "timestamp96" && factor === 1n) {
              output.low[offset + row] = leaf.low[index] ?? 0n;
              output.high[offset + row] = leaf.high[index] ?? 0;
            } else {
              const epoch = epochAt(leaf, index) * factor;
              if (output.type === "timestamp96") writeInt96(output, offset + row, epoch);
              else output.values[offset + row] = epoch;
            }
          });
        }
        offset += input.length;
      }
      return withValid(output, valid);
    });
  }
  if (!("values" in first) || !ArrayBuffer.isView(first.values)) return undefined;
  const sourceValues = first.values;
  const Constructor = sourceValues.constructor as { new (length: number): typeof sourceValues };
  return checked(validityBytes + rows * sourceValues.BYTES_PER_ELEMENT, (scope) => {
    const valid = validity(scope);
    const values = admitVectorBuffer(
      scope,
      rows * sourceValues.BYTES_PER_ELEMENT,
      () => new Constructor(rows),
    );
    let offset = 0;
    for (const input of inputs) {
      const leaf = input.leaf;
      input.forEach((index, row) => {
        if (index >= 0 && "values" in leaf && ArrayBuffer.isView(leaf.values))
          values[offset + row] = leaf.values[index] ?? 0;
      });
      offset += input.length;
    }
    const { valid: _valid, ...shape } = first;
    return withValid({ ...shape, values } as Vector, valid);
  });
}
