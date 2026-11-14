import { AggregateStateMemory } from "./aggregate-state-memory.js";
import {
  type Batch,
  batchExprValues,
  type Selection,
  scalarVectorValue,
  selectedRowCount,
  type Vector,
} from "./batch.js";
import { isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import { isIntervalValue } from "./interval.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import {
  ownedAggregateExpressionBytes,
  requireOwnedAggregateVector,
} from "./owned-aggregate-input.js";
import type { AggregateSpec } from "./query.js";
import {
  finalizeVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  type VectorAggregateOptions,
  type VectorAggregateStateSnapshots,
  type VectorAggregateStates,
  type VectorAggregateValue,
} from "./vector-aggregate.js";

/** Exclusive mutable accumulators. No mutable state escapes; checkpoint and finish
 * return independently admitted output. Input values are borrowed during update;
 * every newly retained payload is charged before mutation.
 */
export class OwnedAggregateState {
  private states: VectorAggregateStates | undefined;
  private readonly scope: MemoryScope;
  private readonly memory: AggregateStateMemory;

  constructor(
    parent: MemoryScope,
    private readonly spec: AggregateSpec,
    private readonly options: VectorAggregateOptions = {},
  ) {
    this.scope = parent.childScope();
    try {
      this.memory = AggregateStateMemory.allocate(this.scope);
      this.scope.signal.addEventListener(
        "abort",
        () => {
          this.states = undefined;
        },
        { once: true },
      );
      this.states = this.memory.create(spec, options);
    } catch (error) {
      this.scope.dispose();
      throw error;
    }
  }

  /** Reuses mutable state across borrowed batches; no checkpoint round trip.
   * Admission capabilities are checked for the whole batch before any updates.
   */
  updateBatch(batch: Batch, selection?: Selection): void {
    const states = this.requireOpen();
    if (
      !Number.isSafeInteger(batch.rowCount) ||
      batch.rowCount < 0 ||
      (selection !== undefined && selection.length !== batch.rowCount)
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid aggregate batch shape");
    for (const alias in this.spec) {
      if (!Object.hasOwn(this.spec, alias)) continue;
      const aggregate = this.spec[alias];
      if (!aggregate) continue;
      if (aggregate.expr) ownedAggregateExpressionBytes(batch, aggregate.expr);
      else if (aggregate.column !== undefined) requireOwnedAggregateVector(batch, aggregate.column);
    }
    for (const alias in this.spec) {
      if (!Object.hasOwn(this.spec, alias)) continue;
      const aggregate = this.spec[alias];
      const state = states[alias];
      if (!aggregate || !state) continue;
      if (state.op === "count" && aggregate.column === undefined && aggregate.expr === undefined) {
        state.count += selectedRowCount(batch.rowCount, selection);
        continue;
      }
      const compiled = this.scope.childScope();
      try {
        const values =
          aggregate.expr === undefined
            ? undefined
            : compiled.allocate(
                ownedAggregateExpressionBytes(batch, aggregate.expr),
                "aggregate expression evaluation plan",
                () =>
                  batchExprValues(
                    batch,
                    aggregate.expr as NonNullable<typeof aggregate.expr>,
                    compiled,
                  ),
              ).value;
        const vector =
          aggregate.expr !== undefined || aggregate.column === undefined
            ? undefined
            : requireOwnedAggregateVector(batch, aggregate.column);
        for (let index = 0; index < batch.rowCount; index++) {
          if (selection !== undefined && selection[index] !== 1) continue;
          if (values !== undefined) {
            const scratch = compiled.childScope();
            try {
              const value = values.valueAt(index, scratch);
              if (value instanceof Uint8Array || isIntervalValue(value) || isDecimalValue(value))
                throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expression is not an aggregate scalar");
              this.updateValue(alias, value);
            } finally {
              scratch.dispose();
            }
          } else if (vector !== undefined) this.updateVectorValue(alias, vector, index);
          else this.updateValue(alias, true);
        }
      } finally {
        compiled.dispose();
      }
    }
  }

  /** Decode one borrowed vector element into admitted scratch, then transfer its
   * logical value into independently charged accumulator storage if retained.
   */
  updateVectorValue(alias: string, vector: Vector, index: number): void {
    this.requireOpen();
    const scratch = this.scope.childScope();
    try {
      const value = scalarVectorValue(vector, index, scratch);
      if (value instanceof Uint8Array || isIntervalValue(value) || isDecimalValue(value))
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector value is not an aggregate scalar");
      this.updateValue(alias, value);
    } finally {
      scratch.dispose();
    }
  }

  updateValue(alias: string, value: VectorAggregateValue): void {
    const states = this.requireOpen();
    const state = states[alias];
    if (!state || !Object.hasOwn(states, alias))
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Unknown aggregate alias", { alias });
    this.memory.updateValue(state, value, this.options);
  }

  /** Merge is prefix-applied, like the unowned reducer. Source reconstruction is
   * temporary; every new target entry receives independent retained admission.
   */
  merge(checkpoint: VectorAggregateStateSnapshots): void {
    const states = this.requireOpen();
    for (const alias in checkpoint) {
      if (!Object.hasOwn(checkpoint, alias)) continue;
      const target = Object.hasOwn(states, alias) ? states[alias] : undefined;
      const source = checkpoint[alias];
      if (!target || !source || target.op !== source.op)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Aggregate checkpoint specification mismatch", {
          alias,
        });
    }
    const scratch = this.scope.childScope();
    try {
      const restored = restoreVectorAggregateStates(checkpoint, this.options, scratch);
      for (const alias in restored) {
        if (!Object.hasOwn(restored, alias)) continue;
        const source = restored[alias];
        const target = states[alias];
        if (!source || !target) continue;
        this.memory.mergeState(target, source, this.options);
      }
    } finally {
      scratch.dispose();
    }
  }

  checkpoint(): OwnedMemory<VectorAggregateStateSnapshots> {
    const states = this.requireOpen();
    const output = this.scope.childScope();
    try {
      return output.retainAllocations(snapshotVectorAggregateStates(states, output));
    } finally {
      output.dispose();
    }
  }

  /** Ends mutation on success or failure. Returned results outlive this owner. */
  finish(): OwnedMemory<Record<string, unknown>> {
    const states = this.requireOpen();
    const output = this.scope.childScope();
    try {
      return output.retainAllocations(finalizeVectorAggregateStates(states, output));
    } finally {
      output.dispose();
      this.dispose();
    }
  }

  dispose(): void {
    this.scope.dispose();
  }

  private requireOpen(): VectorAggregateStates {
    if (this.scope.disposed || this.states === undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Aggregate owner is disposed");
    return this.states;
  }
}
