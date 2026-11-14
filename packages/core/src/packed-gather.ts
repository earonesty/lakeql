import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, type PackedByteVector, type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { planSelectedConcat } from "./selected-concat.js";
import { losslessUtf8Bytes } from "./utf8-size.js";
import { admitVectorBuffer } from "./vector-buffer.js";
import { selectVectorIndices } from "./vector-selection.js";

const textEncoder = new TextEncoder();

/** Measure compatible fixed-width dictionary output without materializing rows.
 * Includes one complete validity mask if any selected dictionary level is null.
 */
export function scalarDictionaryConcatPlan(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  outerValidity: boolean,
) {
  const leaves = vectors.map(dictionaryLeaf);
  const first = leaves[0];
  if (first === undefined || !("values" in first) || !ArrayBuffer.isView(first.values))
    return undefined;
  if (
    leaves.some(
      (leaf) =>
        leaf.type !== first.type ||
        (first.type === "timestamp" &&
          (leaf.type !== "timestamp" ||
            leaf.unit !== first.unit ||
            leaf.isAdjustedToUTC !== first.isAdjustedToUTC)),
    )
  )
    return undefined;
  const length = vectors.reduce((sum, vector) => sum + vector.indices.length, 0);
  let hasNull = outerValidity;
  if (!hasNull) {
    for (const vector of vectors) {
      // Non-nullable dictionary chains need only shape inspection. Avoid a
      // selected-row pass when no validity mask can affect the result.
      let level: Vector = vector;
      let nullable = false;
      for (;;) {
        nullable ||= "valid" in level && level.valid !== undefined;
        if (level.type !== "dict") break;
        level = level.dictionary;
      }
      if (!nullable) continue;
      for (let row = 0; row < vector.indices.length; row++) {
        if (resolvedIndex(vector, row) < 0) {
          hasNull = true;
          break;
        }
      }
      if (hasNull) break;
    }
  }
  const bytes = length * (first.values.BYTES_PER_ELEMENT + (hasNull ? 1 : 0));
  if (!Number.isSafeInteger(length) || !Number.isSafeInteger(bytes))
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Scalar dictionary concat exceeds safe integer capacity",
    );
  return { leaves, first, sourceValues: first.values, length, hasNull, bytes };
}

/** Concatenate compatible fixed-width dictionaries without JS row materialization. */
export function concatScalarDictionaries(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  validity: Uint8Array | undefined,
  scope?: MemoryScope,
  prepared?: NonNullable<ReturnType<typeof scalarDictionaryConcatPlan>>,
): Vector | undefined {
  const plan = prepared ?? scalarDictionaryConcatPlan(vectors, validity !== undefined);
  if (plan === undefined) return undefined;
  const { leaves, first, sourceValues, length, hasNull } = plan;
  const Constructor = sourceValues.constructor as { new (length: number): typeof sourceValues };
  const values = admitVectorBuffer(
    scope,
    length * sourceValues.BYTES_PER_ELEMENT,
    () => new Constructor(length),
  );
  const valid =
    validity ??
    (hasNull ? admitVectorBuffer(scope, length, () => new Uint8Array(length)) : undefined);
  let output = 0;
  for (let column = 0; column < vectors.length; column++) {
    const vector = vectors[column];
    const leaf = leaves[column];
    if (
      vector === undefined ||
      leaf === undefined ||
      !("values" in leaf) ||
      !ArrayBuffer.isView(leaf.values)
    )
      throw new Error("Invalid fixed-width dictionary shape");
    for (let row = 0; row < vector.indices.length; row++, output++) {
      const index = resolvedIndex(vector, row);
      if (index >= 0) values[output] = leaf.values[index] ?? 0;
      if (valid !== undefined) valid[output] = index < 0 ? 0 : 1;
    }
  }
  const { valid: _sourceValidity, ...shape } = first;
  return { ...shape, values, ...(valid === undefined ? {} : { valid }) } as Vector;
}

type PackedGatherLeaf = PackedByteVector | Extract<Vector, { type: "binary" }>;

/** Choose the smaller complete packed working set, including union construction scratch. */
export function adaptivePackedDictionaryPlan(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  outerValidity: boolean,
  availableBytes = Infinity,
) {
  const expansion = packedVectorConcatPlan(vectors, outerValidity);
  if (expansion === undefined) return undefined;
  const expanded = () => {
    requirePackedOffsetCapacity(expansion.bytes);
    return { ...expansion, kind: "expanded" as const };
  };
  const leaves = vectors.map(dictionaryLeaf);
  const unique = [...new Set(leaves)];
  let entries = 0;
  let payload = 0;
  let dictionaryNull = false;
  let text: boolean | undefined;
  const offsets = new Map<Vector, number>();
  for (const leaf of unique) {
    const packed = packedGatherLeaf(leaf);
    if (leaf.type !== "utf8" && packed === undefined) return expanded();
    const isText = leaf.type === "utf8" || leaf.type === "utf8-view";
    if (text !== undefined && text !== isText) return undefined;
    text = isText;
    offsets.set(leaf, entries);
    entries += vectorLength(leaf);
  }
  const { rows, hasNull } = expansion;
  const newValidity = hasNull && !outerValidity ? rows : 0;
  const expandedBytes = expansion.allocationBytes;
  const baseBytes = entries * 12 + rows * 4 + newValidity;
  if (
    entries > 0xffffffff ||
    !Number.isSafeInteger(baseBytes) ||
    baseBytes >= expandedBytes ||
    baseBytes > availableBytes
  )
    return expanded();
  for (const leaf of unique) {
    const packed = packedGatherLeaf(leaf);
    for (let index = 0; index < vectorLength(leaf); index++) {
      if (resolvedIndex(leaf, index) < 0) {
        dictionaryNull = true;
        if (baseBytes + payload + entries >= expandedBytes) return expanded();
        continue;
      }
      const length =
        leaf.type === "utf8"
          ? losslessUtf8Bytes(leaf.values[index] ?? "")
          : packed === undefined
            ? undefined
            : rangeLength(packed, index);
      if (length === undefined) return expanded();
      payload += length;
      if (baseBytes + payload + (dictionaryNull ? entries : 0) >= expandedBytes) return expanded();
    }
  }
  const unionBytes =
    entries * 8 + payload + (dictionaryNull ? entries : 0) + rows * 4 + newValidity;
  const scratchBytes = entries * 4;
  if (
    entries > 0xffffffff ||
    !Number.isSafeInteger(unionBytes + scratchBytes) ||
    payload > 0xffffffff ||
    unionBytes + scratchBytes >= expandedBytes ||
    unionBytes + scratchBytes > availableBytes
  )
    return expanded();
  return {
    kind: "union" as const,
    leaves,
    unique,
    offsets,
    rows,
    hasNull,
    unionBytes,
    scratchBytes,
    allocationBytes: unionBytes + scratchBytes,
    totalBytes: unionBytes + scratchBytes + (outerValidity ? rows : 0),
  };
}

/** Choose a dictionary union only when its output and construction scratch are smaller. */
export function concatAdaptivePackedDictionaries(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  validity: Uint8Array | undefined,
  scope?: MemoryScope,
  prepared?: NonNullable<ReturnType<typeof adaptivePackedDictionaryPlan>>,
): Vector | undefined {
  const plan =
    prepared ??
    adaptivePackedDictionaryPlan(vectors, validity !== undefined, scope?.availableAllocationBytes);
  if (plan === undefined) return undefined;
  if (plan.kind === "expanded") return concatPackedVectors(vectors, validity, scope, plan);
  const { leaves, unique, offsets, rows, hasNull, unionBytes, scratchBytes } = plan;
  const stage = scope?.childStage(unionBytes + scratchBytes, "dictionary union working set");
  try {
    const dictionaries = unique.map((leaf) => ({
      type: "dict" as const,
      indices: admitVectorBuffer(stage, vectorLength(leaf) * 4, () =>
        Uint32Array.from({ length: vectorLength(leaf) }, (_, i) => i),
      ),
      dictionary: leaf,
    }));
    const dictionary = concatPackedDictionaries(dictionaries, undefined, stage);
    if (dictionary === undefined) throw new Error("Dictionary union changed shape");
    const indices = admitVectorBuffer(stage, rows * 4, () => new Uint32Array(rows));
    const valid =
      validity ??
      (hasNull ? admitVectorBuffer(stage, rows, () => new Uint8Array(rows)) : undefined);
    let output = 0;
    for (let i = 0; i < vectors.length; i++) {
      const vector = vectors[i];
      const leaf = leaves[i];
      if (vector === undefined || leaf === undefined) throw new Error("Missing dictionary");
      for (let row = 0; row < vector.indices.length; row++, output++) {
        const index = resolvedIndex(vector, row);
        indices[output] = index < 0 ? 0 : (offsets.get(leaf) ?? 0) + index;
        if (valid !== undefined) valid[output] = index < 0 ? 0 : 1;
      }
    }
    const result: Vector = {
      type: "dict",
      indices,
      dictionary,
      ...(valid === undefined ? {} : { valid }),
    };
    if (stage !== undefined && scope !== undefined) {
      for (const temporary of dictionaries) stage.releaseAllocation(temporary.indices);
      const owner = stage.retainAllocations(result);
      try {
        scope.adopt(owner);
      } catch (error) {
        owner.release();
        throw error;
      }
    }
    return result;
  } finally {
    stage?.dispose();
  }
}

/** Copy selected encoded byte ranges across distinct dictionaries without row objects. */
export function concatPackedDictionaries(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  validity: Uint8Array | undefined,
  scope?: MemoryScope,
): PackedByteVector | undefined {
  return concatPackedVectors(vectors, validity, scope);
}

/** Exact selected packed expansion backing. Existing outer validity is borrowed;
 * totalBytes includes it, while allocationBytes counts only newly allocated backing.
 * Costs may exceed Uint32 payload capacity so callers can compare dictionary unions.
 * Execution must check the selected representation's offset capacity.
 * Undefined means the selected values cannot be represented losslessly as packed bytes.
 */
export function packedVectorConcatPlan(vectors: readonly Vector[], outerValidity: boolean) {
  const leaves = vectors.map((vector) => {
    const leaf = dictionaryLeaf(vector);
    return leaf.type === "utf8" ? leaf : packedGatherLeaf(leaf);
  });
  const first = leaves[0];
  if (first === undefined) return undefined;
  const text = first.type === "utf8-view" || first.type === "utf8";
  if (
    leaves.some(
      (leaf) => leaf === undefined || (leaf.type === "utf8-view" || leaf.type === "utf8") !== text,
    )
  )
    return undefined;
  let rows = 0;
  let bytes = 0;
  let hasNull = outerValidity;
  for (let column = 0; column < vectors.length; column++) {
    const vector = vectors[column];
    const leaf = leaves[column];
    if (vector === undefined || leaf === undefined) throw new Error("Missing packed dictionary");
    rows += vectorLength(vector);
    for (let row = 0; row < vectorLength(vector); row++) {
      const index = resolvedIndex(vector, row);
      if (index < 0) hasNull = true;
      else {
        const length =
          leaf.type === "utf8"
            ? losslessUtf8Bytes(leaf.values[index] ?? "")
            : rangeLength(leaf, index);
        if (length === undefined) return undefined;
        bytes += length;
      }
    }
  }
  const totalBytes = rows * 8 + bytes + (hasNull ? rows : 0);
  if (!Number.isSafeInteger(rows) || !Number.isSafeInteger(totalBytes))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Packed concat exceeds safe integer capacity");
  return {
    leaves,
    text,
    rows,
    bytes,
    hasNull,
    totalBytes,
    allocationBytes: totalBytes - (outerValidity ? rows : 0),
  };
}

export function concatPackedVectors(
  vectors: readonly Vector[],
  validity: Uint8Array | undefined,
  scope?: MemoryScope,
  prepared?: NonNullable<ReturnType<typeof packedVectorConcatPlan>>,
): PackedByteVector | undefined {
  const plan = prepared ?? packedVectorConcatPlan(vectors, validity !== undefined);
  if (plan === undefined) return undefined;
  const { leaves, text, rows, bytes, hasNull } = plan;
  requirePackedOffsetCapacity(bytes);
  const { data, ranges, valid } = allocatePackedOutput(scope, rows, bytes, hasNull, validity);
  let output = 0;
  let offset = 0;
  for (let column = 0; column < vectors.length; column++) {
    const vector = vectors[column];
    const leaf = leaves[column];
    if (vector === undefined || leaf === undefined) throw new Error("Missing packed dictionary");
    for (let row = 0; row < vectorLength(vector); row++, output++) {
      const index = resolvedIndex(vector, row);
      const start = index < 0 || leaf.type === "utf8" ? 0 : rangeStart(leaf, index);
      const length =
        index < 0
          ? 0
          : leaf.type === "utf8"
            ? losslessUtf8Bytes(leaf.values[index] ?? "")
            : rangeLength(leaf, index);
      if (length === undefined) throw new Error("Text changed after UTF-8 admission");
      ranges[output * 2] = offset;
      ranges[output * 2 + 1] = length;
      if (index >= 0) {
        if (leaf.type === "utf8") {
          textEncoder.encodeInto(leaf.values[index] ?? "", data.subarray(offset, offset + length));
        } else {
          const source =
            leaf.type === "binary" ? leaf.values[index] : leaf.data.subarray(start, start + length);
          if (source !== undefined) data.set(source, offset);
        }
      }
      offset += length;
      if (valid !== undefined) valid[output] = index < 0 ? 0 : 1;
    }
  }
  return {
    type: text ? "utf8-view" : "binary-view",
    data,
    ranges,
    length: rows,
    ...(valid === undefined ? {} : { valid }),
  };
}

export function packedGatherLeaf(vector: Vector): PackedGatherLeaf | undefined {
  while (vector.type === "dict") vector = vector.dictionary;
  return vector.type === "binary-view" || vector.type === "utf8-view" || vector.type === "binary"
    ? vector
    : undefined;
}

/** Payload bytes copied by direct packed concat; null payloads are omitted. */
export function packedConcatPayloadBytes(vector: PackedByteVector): number {
  let bytes = 0;
  for (let row = 0; row < vector.length; row++) {
    if (vector.valid === undefined || vector.valid[row] === 1) bytes += rangeLength(vector, row);
  }
  if (!Number.isSafeInteger(bytes) || bytes > 0xffffffff)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Packed concat exceeds Uint32 byte offsets");
  return bytes;
}

function dictionaryLeaf(vector: Vector): Vector {
  while (vector.type === "dict") vector = vector.dictionary;
  return vector;
}

function resolvedIndex(vector: Vector, index: number): number {
  while (true) {
    if ("valid" in vector && vector.valid !== undefined && vector.valid[index] !== 1) return -1;
    if (vector.type !== "dict") return index;
    index = vector.indices[index] ?? 0;
    vector = vector.dictionary;
  }
}

function rangeStart(vector: PackedGatherLeaf, index: number): number {
  if (vector.type === "binary") return 0;
  const row = (vector.rowOffset ?? 0) + index;
  return vector.ranges === undefined
    ? row * (vector.fixedLength ?? 0)
    : (vector.ranges[row * 2] ?? 0);
}

function rangeLength(vector: PackedGatherLeaf, index: number): number {
  if (vector.type === "binary")
    return vector.valid?.[index] === 0 ? 0 : (vector.values[index]?.byteLength ?? 0);
  if (vector.valid?.[index] === 0) return 0;
  const row = (vector.rowOffset ?? 0) + index;
  return vector.ranges === undefined
    ? (vector.fixedLength ?? 0)
    : (vector.ranges[row * 2 + 1] ?? 0);
}

function payloadBytes(
  vector: Vector,
  leaf: PackedGatherLeaf,
  indices: readonly number[] | Uint32Array,
): number {
  let bytes = 0;
  for (const index of indices) {
    const resolved = resolvedIndex(vector, index);
    if (resolved >= 0) bytes += rangeLength(leaf, resolved);
  }
  return bytes;
}

/** Exact new typed backing for a fully detachable gather, or undefined for shared shapes. */
export function compactGatherPlan(
  batch: Batch,
  indices: readonly number[] | Uint32Array,
): { typedBytes: number; viewBytes: number } | undefined {
  let bytes = 0;
  let viewBytes = 0;
  for (const name in batch.columns) {
    if (!Object.hasOwn(batch.columns, name)) continue;
    const vector = batch.columns[name];
    if (vector === undefined) continue;
    const validity = indices.some((index) => resolvedIndex(vector, index) < 0) ? indices.length : 0;
    const scalarLeaf = dictionaryLeaf(vector);
    const leaf = packedGatherLeaf(vector);
    if (leaf !== undefined) {
      const payload = payloadBytes(vector, leaf, indices);
      if (!Number.isSafeInteger(payload) || payload > 0xffffffff) return undefined;
      bytes += payload + indices.length * 8 + validity;
      viewBytes += (2 + (validity > 0 ? 1 : 0)) * c.view;
    } else if (scalarLeaf.type === "decimal") {
      const plan = planSelectedConcat([selectVectorIndices(vector, indices)]);
      if (plan === undefined) return undefined;
      bytes += plan.bytes;
      if (validity > 0) viewBytes += c.view;
    } else if (scalarLeaf.type === "null") {
    } else if ("values" in scalarLeaf && ArrayBuffer.isView(scalarLeaf.values)) {
      bytes += indices.length * scalarLeaf.values.BYTES_PER_ELEMENT + validity;
      viewBytes += (1 + (validity > 0 ? 1 : 0)) * c.view;
    } else {
      return undefined;
    }
  }
  return Number.isSafeInteger(bytes + viewBytes) ? { typedBytes: bytes, viewBytes } : undefined;
}

export function compactPackedGather(
  vector: Vector,
  indices: readonly number[] | Uint32Array,
  scope: MemoryScope,
): PackedByteVector {
  const leaf = packedGatherLeaf(vector);
  if (leaf === undefined) throw new Error("Packed gather requires a packed leaf");
  const bytes = payloadBytes(vector, leaf, indices);
  const hasNull = indices.some((index) => resolvedIndex(vector, index) < 0);
  const { ranges, data, valid } = allocatePackedOutput(scope, indices.length, bytes, hasNull);
  let offset = 0;
  for (let row = 0; row < indices.length; row++) {
    const index = resolvedIndex(vector, indices[row] ?? 0);
    const start = index < 0 ? 0 : rangeStart(leaf, index);
    const length = index < 0 ? 0 : rangeLength(leaf, index);
    ranges[row * 2] = offset;
    ranges[row * 2 + 1] = length;
    if (index >= 0) {
      const source =
        leaf.type === "binary" ? leaf.values[index] : leaf.data.subarray(start, start + length);
      if (source !== undefined) data.set(source, offset);
    }
    offset += length;
    if (valid !== undefined) valid[row] = index < 0 ? 0 : 1;
  }
  return {
    type: leaf.type === "binary" ? "binary-view" : leaf.type,
    data,
    ranges,
    length: indices.length,
    ...(valid === undefined ? {} : { valid }),
  };
}

/** Admit the complete packed output before constructing any of its backing. */
function allocatePackedOutput(
  scope: MemoryScope | undefined,
  rows: number,
  bytes: number,
  hasNull: boolean,
  validity?: Uint8Array,
) {
  if (scope === undefined)
    return {
      ranges: new Uint32Array(rows * 2),
      data: new Uint8Array(bytes),
      valid: validity ?? (hasNull ? new Uint8Array(rows) : undefined),
    };
  const sizes = [rows * 8, bytes, hasNull && validity === undefined ? rows : 0];
  const admission = scope.reserve(
    sizes.reduce((sum, size) => sum + size, 0),
    "packed gather output",
  );
  let parts: MemoryReservation[];
  try {
    parts = admission.partition(sizes);
  } finally {
    admission.release();
  }
  const owners: OwnedMemory<unknown>[] = [];
  const allocationScope = scope;
  function allocate<T>(index: number, create: () => T): T {
    const part = parts[index];
    if (part === undefined) throw new Error("Missing packed output admission");
    const owner = allocationScope.allocateReserved(part, create);
    owners.push(owner);
    return owner.value;
  }
  try {
    return {
      ranges: allocate(0, () => new Uint32Array(rows * 2)),
      data: allocate(1, () => new Uint8Array(bytes)),
      valid: validity ?? (hasNull ? allocate(2, () => new Uint8Array(rows)) : undefined),
    };
  } catch (error) {
    for (const owner of owners) owner.release();
    throw error;
  } finally {
    for (const part of parts) part.release();
  }
}

/** Detach dictionary-backed fixed-width values without a materialized index array. */
export function compactScalarDictionary(
  vector: Vector,
  indices: readonly number[] | Uint32Array,
  scope: MemoryScope,
): Vector {
  const leaf = dictionaryLeaf(vector);
  if (leaf.type === "null") return { type: "null", length: indices.length };
  if (!("values" in leaf) || !ArrayBuffer.isView(leaf.values)) {
    throw new Error("Scalar dictionary compaction requires typed values");
  }
  const sourceValues = leaf.values;
  const Constructor = sourceValues.constructor as { new (length: number): typeof sourceValues };
  const values = admitVectorBuffer(
    scope,
    indices.length * leaf.values.BYTES_PER_ELEMENT,
    () => new Constructor(indices.length),
  );
  const valid = indices.some((index) => resolvedIndex(vector, index) < 0)
    ? admitVectorBuffer(scope, indices.length, () => new Uint8Array(indices.length))
    : undefined;
  for (let row = 0; row < indices.length; row++) {
    const index = resolvedIndex(vector, indices[row] ?? 0);
    if (index >= 0) values[row] = leaf.values[index] ?? 0;
    if (valid !== undefined) valid[row] = index < 0 ? 0 : 1;
  }
  const { valid: _sourceValid, ...shape } = leaf;
  return { ...shape, values, ...(valid === undefined ? {} : { valid }) } as Vector;
}

function requirePackedOffsetCapacity(bytes: number): void {
  if (bytes > 0xffffffff)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Packed concat exceeds Uint32 byte offsets");
}
