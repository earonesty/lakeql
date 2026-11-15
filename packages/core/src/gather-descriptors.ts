import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch, Vector } from "./batch.js";
import { registerCompactionMetadata } from "./compaction-metadata.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";

export const GATHER_BATCH_DESCRIPTOR_BYTES = c.object + 2 * c.property;
export const GATHER_DESCRIPTOR_BASE_BYTES = GATHER_BATCH_DESCRIPTOR_BYTES + c.object;

/** Directory entries retain their key storage independently of the source batch. */
export function gatherColumnEntryBytes(name: string): number {
  return c.property + c.string + name.length * c.stringCodeUnit;
}

/** Forecast the descriptor actually produced by the gather's shape capability. */
export function gatherVectorDescriptorBytes(
  vector: Vector,
  hasNull: boolean,
  compact: boolean,
): number {
  if (compact) {
    let leaf = vector;
    while (leaf.type === "dict") leaf = leaf.dictionary;
    if (leaf.type === "utf8-view" || leaf.type === "binary-view" || leaf.type === "binary")
      return c.object + (4 + Number(hasNull)) * c.property;
    if (leaf.type === "decimal") return c.object + (2 + Number(hasNull)) * c.property;
    if (leaf.type === "null") return c.object + 2 * c.property;
    if (vector.type === "dict") {
      // Scalar dictionary compaction preserves the leaf's enumerable shape,
      // replacing values and selected validity rather than its metadata fields.
      let properties = Number(hasNull);
      for (const key in leaf) if (Object.hasOwn(leaf, key) && key !== "valid") properties++;
      return c.object + properties * c.property;
    }
  }
  switch (vector.type) {
    case "decimal":
    case "union":
    case "list":
    case "map":
    case "struct":
    case "utf8":
    case "binary":
    case "utf8-view":
    case "binary-view":
      return c.object + 3 * c.property;
    case "null":
      return c.object + 2 * c.property;
    case "dict":
      return c.object + (3 + Number(hasNull)) * c.property;
    case "timestamp96":
      return c.object + (5 + Number(hasNull)) * c.property;
    case "timestamp":
      return c.object + (4 + Number(hasNull)) * c.property;
    default:
      return c.object + (2 + Number(hasNull)) * c.property;
  }
}

/** The enclosing gather stage admits all containers before construction. Track
 * each container independently so payload-only handoff can drop obsolete shells. */
export function ownGatherDescriptors(
  scope: MemoryScope,
  batch: Batch,
  admission: MemoryReservation,
): void {
  let remaining = admission;
  const own = (bytes: number, value: object, kind: "vector" | "columns" | "batch") => {
    const [part, rest] = remaining.partition([bytes, remaining.bytes - bytes]);
    if (part === undefined || rest === undefined) throw new Error("Missing descriptor admission");
    remaining = rest;
    try {
      scope.allocateContainerReserved(part, () => value);
      registerCompactionMetadata(value, kind);
    } finally {
      part.release();
    }
  };
  try {
    let directoryBytes = c.object;
    for (const name in batch.columns) {
      if (!Object.hasOwn(batch.columns, name)) continue;
      const vector = batch.columns[name];
      if (vector === undefined) continue;
      directoryBytes += gatherColumnEntryBytes(name);
      let bytes = c.object;
      for (const key in vector) if (Object.hasOwn(vector, key)) bytes += c.property;
      own(bytes, vector, "vector");
    }
    own(directoryBytes, batch.columns, "columns");
    own(GATHER_BATCH_DESCRIPTOR_BYTES, batch, "batch");
  } finally {
    remaining.release();
  }
}
