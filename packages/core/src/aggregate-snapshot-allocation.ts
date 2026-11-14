import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { isTimestampValue } from "./timestamp.js";
import type { VectorAggregateStates, VectorAggregateValue } from "./vector-aggregate.js";

/** Conservative checkpoint extent, including sort/merge work and borrowed string
 * payloads. No snapshot containers or serialized scalar strings are created here.
 * State must remain unchanged between this synchronous forecast and copying.
 */
export function aggregateSnapshotAllocationBytes(states: VectorAggregateStates): number {
  let bytes = c.object;
  const add = (amount: number) => {
    bytes += amount;
    if (!Number.isSafeInteger(bytes))
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Aggregate checkpoint extent overflow");
  };
  for (const alias in states) {
    if (!Object.hasOwn(states, alias)) continue;
    const state = states[alias];
    if (!state) continue;
    add(c.property + stringBytes(alias) + c.object);
    switch (state.op) {
      case "count":
      case "sum":
        add(2 * c.property);
        break;
      case "avg":
        add(3 * c.property);
        break;
      case "var_samp":
      case "var_pop":
      case "stddev_samp":
      case "stddev_pop":
        add(4 * c.property);
        break;
      case "min":
      case "max":
        add(2 * c.property + scalarBytes(state.value));
        break;
      case "first":
      case "last":
      case "any":
        add(3 * c.property + scalarBytes(state.value));
        break;
      case "median":
      case "quantile":
        add((state.op === "median" ? 2 : 3) * c.property + c.array);
        for (const value of state.values) add(c.arraySlot + scalarBytes(value));
        break;
      case "mode":
        add(2 * c.property + c.array);
        for (const [key, entry] of state.values)
          add(
            c.arraySlot + c.object + 3 * c.property + stringBytes(key) + scalarBytes(entry.value),
          );
        break;
      case "count_distinct":
      case "approx_count_distinct": {
        add(2 * c.property + c.array);
        if (state.sortedRuns !== undefined) {
          // Include duplicate output candidates without allocating to deduplicate.
          // The merge reuses one heap entry per nonempty run.
          add(c.array);
          for (const run of state.sortedRuns) {
            if (run.length > 0) add(c.arraySlot + c.object + 3 * c.property);
            for (const value of run) add(c.arraySlot + stringBytes(value));
          }
        } else {
          for (const value of state.sortedValues ?? state.values)
            add(2 * c.arraySlot + stringBytes(value));
        }
        break;
      }
    }
  }
  return bytes;
}

function stringBytes(value: string): number {
  return c.string + c.stringCodeUnit * value.length;
}

function scalarBytes(value: VectorAggregateValue): number {
  if (typeof value === "string") return stringBytes(value);
  if (typeof value === "bigint")
    return c.object + 2 * c.property + decimalStringAllocationBytes(value);
  if (isTimestampValue(value))
    return c.object + 4 * c.property + decimalStringAllocationBytes(value.epochNanoseconds);
  return c.number;
}
