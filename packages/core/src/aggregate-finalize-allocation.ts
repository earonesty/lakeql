import { finalizeMode } from "./aggregate-mode.js";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { isTimestampValue } from "./timestamp.js";
import type { VectorAggregateStates, VectorAggregateValue } from "./vector-aggregate.js";

/** Finalizers run serially: only the largest sort/merge workspace overlaps the
 * result row. Payload charges keep results independent of accumulator lifetime.
 */
export function aggregateFinalizeAllocation(states: VectorAggregateStates): {
  outputBytes: number;
  scratchBytes: number;
} {
  let outputBytes = c.object;
  let scratchBytes = 0;
  for (const alias in states) {
    if (!Object.hasOwn(states, alias)) continue;
    const state = states[alias];
    if (!state) continue;
    outputBytes += c.property + stringBytes(alias);
    let payload: number = c.number;
    let scratch = 0;
    switch (state.op) {
      case "min":
      case "max":
      case "first":
      case "last":
      case "any":
        payload = scalarBytes(state.value);
        break;
      case "mode":
        payload = scalarBytes(finalizeMode(state));
        break;
      case "median":
        for (const value of state.values) payload = Math.max(payload, scalarBytes(value));
        scratch = state.values.length === 0 ? 0 : c.array + 2 * c.arraySlot * state.values.length;
        break;
      case "quantile":
        scratch = state.values.length === 0 ? 0 : c.array + 2 * c.arraySlot * state.values.length;
        break;
      case "count_distinct":
      case "approx_count_distinct":
        if (state.sortedRuns !== undefined && state.sortedRuns.length > 1) {
          scratch = c.array;
          for (const run of state.sortedRuns)
            if (run.length > 0) scratch += c.arraySlot + c.object + 3 * c.property;
        }
        break;
    }
    outputBytes += payload;
    scratchBytes = Math.max(scratchBytes, scratch);
    if (!Number.isSafeInteger(outputBytes + scratchBytes))
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Aggregate finalization extent overflow");
  }
  return { outputBytes, scratchBytes };
}

function stringBytes(value: string): number {
  return c.string + c.stringCodeUnit * value.length;
}

function scalarBytes(value: VectorAggregateValue): number {
  if (typeof value === "string") return stringBytes(value);
  if (typeof value === "bigint") return c.bigint + decimalStringAllocationBytes(value);
  if (isTimestampValue(value))
    return c.timestamp + c.bigint + decimalStringAllocationBytes(value.epochNanoseconds);
  return c.number;
}
