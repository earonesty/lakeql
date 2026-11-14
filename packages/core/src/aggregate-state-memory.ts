import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import { MutableMemoryLedger } from "./mutable-memory-ledger.js";
import type { AggregateSpec } from "./query.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { isTimestampValue } from "./timestamp.js";
import {
  addModeEntry,
  aggregateStateCreationBytes,
  createVectorAggregateStates,
  mergeVectorAggregateState,
  minMaxShouldReplace,
  updateVectorAggregateStateValue,
  type VectorAggregateOptions,
  type VectorAggregateState,
  type VectorAggregateStates,
  type VectorAggregateValue,
} from "./vector-aggregate.js";
import { addDistinctValue, distinctKey } from "./vector-aggregate-distinct.js";

/** One exclusive arena can own many accumulator directories. State remains private
 * to its enclosing reducer; checkpoints/results must be separately admitted.
 */
export class AggregateStateMemory {
  private readonly states = new WeakSet<VectorAggregateState>();
  private readonly replacements = new WeakMap<VectorAggregateState, number>();
  private readonly growth: MutableMemoryLedger;
  static allocate(scope: MemoryScope): AggregateStateMemory {
    return scope.allocate(
      c.object * 4 + 16 * c.property + c.array,
      "aggregate arena metadata",
      () => new AggregateStateMemory(scope),
    ).value;
  }

  get usedBytes(): number {
    return this.growth.usedBytes;
  }

  private constructor(private readonly scope: MemoryScope) {
    this.growth = new MutableMemoryLedger(scope);
  }

  create(spec: AggregateSpec, options: VectorAggregateOptions = {}): VectorAggregateStates {
    this.requireOpen();
    let bytes = c.object;
    for (const alias in spec) {
      if (!Object.hasOwn(spec, alias)) continue;
      const aggregate = spec[alias];
      if (!aggregate)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Missing aggregate specification", { alias });
      // Directory entry, alias payload, accumulator and replacement-ledger slot.
      bytes += 3 * c.property + stringBytes(alias) + aggregateStateCreationBytes(aggregate.op);
    }
    let states: VectorAggregateStates | undefined;
    this.growth.append(bytes, "aggregate state construction", () => {
      states = createVectorAggregateStates(spec, options);
      for (const alias in states) {
        if (!Object.hasOwn(states, alias)) continue;
        const state = states[alias];
        if (state) this.states.add(state);
      }
    });
    if (!states) throw new Error("Missing admitted aggregate states");
    return states;
  }

  updateValue(
    state: VectorAggregateState,
    value: VectorAggregateValue,
    options: VectorAggregateOptions = {},
  ): void {
    this.requireOpen();
    this.requireState(state);
    const update = () => updateVectorAggregateStateValue(state, value, options);
    switch (state.op) {
      case "median":
      case "quantile":
        if (value === null) return;
        this.growth.append(c.arraySlot + scalarBytes(value), "aggregate list growth", update);
        return;
      case "mode":
      case "count_distinct":
      case "approx_count_distinct": {
        if (value === null) return;
        const scratch = this.scope.childScope();
        try {
          // Keep the conservative formatting/key workspace allowance, but normalize
          // once and share the key between admission lookup and mutation.
          const keyBytes = keyAllocationBytes(value);
          const key = scratch.allocate(2 * keyBytes, "aggregate key lookup", () =>
            distinctKey(value),
          ).value;
          const updateKey = () =>
            state.op === "mode"
              ? addModeEntry(state, key, value, 1, options.budget)
              : addDistinctValue(state, key, options.budget);
          if (state.values.has(key)) {
            updateKey();
          } else {
            const entryBytes =
              c.property +
              keyBytes +
              (state.op === "mode" ? c.object + 2 * c.property + scalarBytes(value) : 0);
            this.growth.append(entryBytes, "aggregate collection growth", updateKey);
          }
        } finally {
          scratch.dispose();
        }
        return;
      }
      case "min":
      case "max":
      case "first":
      case "last":
      case "any": {
        if ((state.op === "min" || state.op === "max") && !minMaxShouldReplace(state, value))
          return;
        if (
          Object.is(state.value, value) ||
          ((state.op === "first" || state.op === "any") && state.seen)
        ) {
          update();
          return;
        }
        const bytes = scalarBytes(value);
        this.growth.append(bytes, "aggregate scalar replacement", update);
        if (Object.is(state.value, value)) {
          this.growth.release(this.replacements.get(state) ?? 0);
          this.replacements.set(state, bytes);
        } else this.growth.release(bytes);
        return;
      }
      default:
        update();
    }
  }

  mergeState(
    target: VectorAggregateState,
    source: VectorAggregateState,
    options: VectorAggregateOptions = {},
  ): void {
    this.requireOpen();
    this.requireState(target);
    if (target.op !== source.op)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Aggregate state operation mismatch");
    switch (source.op) {
      case "median":
      case "quantile":
        for (const value of source.values) this.updateValue(target, value, options);
        break;
      case "min":
      case "max":
        this.updateValue(target, source.value, options);
        break;
      case "first":
      case "last":
      case "any":
        if (source.seen) this.updateValue(target, source.value, options);
        break;
      case "mode":
        if (target.op !== "mode") throw new Error("Validated mode operation changed");
        for (const [key, entry] of source.values) {
          const update = () => addModeEntry(target, key, entry.value, entry.count, options.budget);
          if (target.values.has(key)) update();
          else
            this.growth.append(
              c.property + stringBytes(key) + c.object + 2 * c.property + scalarBytes(entry.value),
              "aggregate mode merge growth",
              update,
            );
        }
        break;
      case "count_distinct":
      case "approx_count_distinct":
        if (target.op !== "count_distinct" && target.op !== "approx_count_distinct")
          throw new Error("Validated distinct operation changed");
        for (const key of source.values) {
          const update = () => addDistinctValue(target, key, options.budget);
          if (target.values.has(key)) update();
          else
            this.growth.append(
              c.property + stringBytes(key),
              "aggregate distinct merge growth",
              update,
            );
        }
        break;
      default:
        mergeVectorAggregateState(target, source, options.budget);
    }
  }

  private requireState(state: VectorAggregateState): void {
    if (!this.states.has(state))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Accumulator belongs to another arena");
  }

  private requireOpen(): void {
    if (this.scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Aggregate arena is disposed");
  }
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

function keyAllocationBytes(value: Exclude<VectorAggregateValue, null>): number {
  if (typeof value === "string") return stringBytes(value) + 14;
  if (typeof value === "bigint") return decimalStringAllocationBytes(value) + 14;
  if (isTimestampValue(value)) return decimalStringAllocationBytes(value.epochNanoseconds) + 128;
  return c.string + 80;
}
