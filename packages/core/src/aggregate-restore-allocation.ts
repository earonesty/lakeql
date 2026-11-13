import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type {
  VectorAggregateSnapshotValue,
  VectorAggregateStateSnapshots,
} from "./vector-aggregate.js";

/** Mutable checkpoint reconstruction, including collection entries and scalar
 * payloads. Duplicate entries are conservatively charged before deduplication.
 * This admits restore only; subsequent mutation needs its own growth admission.
 */
export function aggregateRestoreAllocationBytes(snapshots: VectorAggregateStateSnapshots): number {
  let bytes = c.object;
  for (const alias in snapshots) {
    if (!Object.hasOwn(snapshots, alias)) continue;
    const snapshot = snapshots[alias];
    if (!snapshot) continue;
    bytes += c.property + stringBytes(alias) + c.object;
    switch (snapshot.op) {
      case "count":
      case "sum":
        bytes += 2 * c.property;
        break;
      case "avg":
        bytes += 3 * c.property;
        break;
      case "var_samp":
      case "var_pop":
      case "stddev_samp":
      case "stddev_pop":
        bytes += 4 * c.property;
        break;
      case "min":
      case "max":
        bytes += 2 * c.property + scalarBytes(snapshot.value);
        break;
      case "first":
      case "last":
      case "any":
        bytes += 3 * c.property + scalarBytes(snapshot.value);
        break;
      case "median":
      case "quantile":
        bytes += (snapshot.op === "median" ? 3 : 4) * c.property + c.array;
        for (const value of snapshot.values) bytes += c.arraySlot + scalarBytes(value);
        break;
      case "count_distinct":
      case "approx_count_distinct":
        bytes += 7 * c.property + c.object;
        for (const value of snapshot.values) bytes += c.property + stringBytes(value);
        break;
      case "mode":
        bytes += 7 * c.property + c.object;
        for (const entry of snapshot.values)
          bytes +=
            c.property +
            c.object +
            2 * c.property +
            stringBytes(entry.key) +
            scalarBytes(entry.value);
        break;
    }
    if (!Number.isSafeInteger(bytes))
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Aggregate restore extent overflow");
  }
  return bytes;
}

function stringBytes(value: string): number {
  return c.string + c.stringCodeUnit * value.length;
}

function scalarBytes(value: VectorAggregateSnapshotValue): number {
  if (typeof value === "string") return stringBytes(value);
  if (value !== null && typeof value === "object") {
    // Decimal input length bounds binary bigint payload without parsing before
    // admission. Include fixed scalar metadata and timestamp object where needed.
    return value.type === "bigint"
      ? c.bigint + c.stringCodeUnit * value.value.length
      : c.timestamp + c.bigint + c.stringCodeUnit * value.epochNanoseconds.length;
  }
  return c.number;
}
