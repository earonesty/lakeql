import { AggregateStateMemory } from "./aggregate-state-memory.js";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import {
  type Batch,
  type BatchExprValues,
  batchExprValues,
  type Selection,
  scalarVectorValue,
  type Vector,
} from "./batch.js";
import { isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import { groupKey } from "./group-key.js";
import { isIntervalValue } from "./interval.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { MutableMemoryLedger } from "./mutable-memory-ledger.js";
import {
  ownedAggregateExpressionBytes,
  requireOwnedAggregateVector,
} from "./owned-aggregate-input.js";
import type { AggregateSpec } from "./query.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { isTimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import {
  finalizeVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  type VectorAggregateValue,
} from "./vector-aggregate.js";
import {
  estimateAggregateStateBytes,
  restoreGroupValue,
  snapshotGroupValue,
  type VectorGroup,
  type VectorGroupByOptions,
  type VectorGroupByStateSnapshot,
} from "./vector-group-by.js";

/** Private mutable directory for an enclosing grouped reducer. Raw group objects
 * must not escape its lifetime; output/checkpoints require independent admission.
 */
export class OwnedGroupDirectory {
  private readonly scope: MemoryScope;
  private readonly arena: AggregateStateMemory;
  private readonly ledger: MutableMemoryLedger;
  private readonly groups = new Map<string, VectorGroup>();
  private readonly members = new WeakSet<VectorGroup>();
  private bytes = 0;

  static allocate(
    parent: MemoryScope,
    readonlyKeys: readonly string[],
    spec: AggregateSpec,
    options: VectorGroupByOptions = {},
  ): OwnedGroupDirectory {
    return parent.allocate(
      c.object * 4 + 16 * c.property + c.array,
      "group directory metadata",
      () => new OwnedGroupDirectory(parent, readonlyKeys, spec, options),
    ).value;
  }

  private constructor(
    parent: MemoryScope,
    readonly keys: readonly string[],
    private readonly spec: AggregateSpec,
    private readonly options: VectorGroupByOptions,
  ) {
    this.scope = parent.childScope();
    try {
      this.arena = AggregateStateMemory.allocate(this.scope);
      this.ledger = new MutableMemoryLedger(this.scope);
      this.scope.signal.addEventListener(
        "abort",
        () => {
          this.groups.clear();
          this.bytes = 0;
        },
        { once: true },
      );
    } catch (error) {
      this.scope.dispose();
      throw error;
    }
  }

  get size(): number {
    return this.groups.size;
  }
  /** Existing operator-size metric, separate from shared allocation reservations. */
  get memoryBytes(): number {
    return this.bytes;
  }

  /** Compile once per batch and keep group state across batches. Unsupported
   * input capabilities are rejected before the first group is mutated. */
  updateBatch(batch: Batch, selection?: Selection): void {
    this.requireOpen();
    if (
      !Number.isSafeInteger(batch.rowCount) ||
      batch.rowCount < 0 ||
      (selection !== undefined && selection.length !== batch.rowCount)
    )
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid grouped aggregate batch shape");
    for (const key of this.keys) requireOwnedAggregateVector(batch, key);
    let count = 0;
    for (const alias in this.spec) {
      if (!Object.hasOwn(this.spec, alias)) continue;
      const aggregate = this.spec[alias];
      if (!aggregate) continue;
      count++;
      if (aggregate.expr) ownedAggregateExpressionBytes(batch, aggregate.expr);
      else if (aggregate.column !== undefined) requireOwnedAggregateVector(batch, aggregate.column);
    }
    const compiled = this.scope.childScope();
    try {
      const keys = compiled.allocate(
        c.array + c.arraySlot * this.keys.length,
        "group key readers",
        () => this.keys.map((key) => requireOwnedAggregateVector(batch, key)),
      ).value;
      const inputs = compiled.allocate(
        c.array + count * (c.arraySlot + c.object + 3 * c.property),
        "group aggregate readers",
        (): { alias: string; vector?: Vector; values?: BatchExprValues }[] => [],
      ).value;
      for (const alias in this.spec) {
        if (!Object.hasOwn(this.spec, alias)) continue;
        const aggregate = this.spec[alias];
        if (!aggregate) continue;
        if (aggregate.expr) {
          const expr = aggregate.expr;
          const values = compiled.allocate(
            ownedAggregateExpressionBytes(batch, expr),
            "group aggregate expression plan",
            () => batchExprValues(batch, expr, compiled),
          ).value;
          inputs.push({ alias, values });
        } else if (aggregate.column !== undefined) {
          inputs.push({ alias, vector: requireOwnedAggregateVector(batch, aggregate.column) });
        } else inputs.push({ alias });
      }
      for (let index = 0; index < batch.rowCount; index++) {
        if (selection !== undefined && selection[index] !== 1) continue;
        const row = compiled.childScope();
        try {
          const values = row.allocate(
            c.array + c.arraySlot * keys.length,
            "decoded group key tuple",
            (): Scalar[] => [],
          ).value;
          for (const key of keys) values.push(scalarVectorValue(key, index, row));
          const group = this.getOrCreate(values);
          for (const input of inputs) {
            const scratch = row.childScope();
            try {
              const value = input.values
                ? input.values.valueAt(index, scratch)
                : input.vector
                  ? scalarVectorValue(input.vector, index, scratch)
                  : true;
              if (value instanceof Uint8Array || isIntervalValue(value) || isDecimalValue(value))
                throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expression is not an aggregate scalar");
              this.update(group, input.alias, value);
            } finally {
              scratch.dispose();
            }
          }
        } finally {
          row.dispose();
        }
      }
    } finally {
      compiled.dispose();
    }
  }

  getOrCreate(values: readonly Scalar[]): VectorGroup {
    this.requireOpen();
    if (values.length !== this.keys.length)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Group key arity mismatch");
    const scratch = this.scope.childScope();
    try {
      const key = groupKey(values, scratch);
      const existing = this.groups.get(key);
      if (existing) return existing;
      const limit = this.options.maxGroups;
      if (limit !== undefined && this.groups.size >= limit)
        throw new LakeqlError("LAKEQL_GROUP_LIMIT_EXCEEDED", "Query exceeded group budget", {
          limit,
          actual: this.groups.size + 1,
        });
      const buffered = this.options.budget?.maxBufferedRows;
      if (buffered !== undefined && this.groups.size >= buffered)
        throw new LakeqlError(
          "LAKEQL_BUDGET_EXCEEDED",
          "Group directory exceeds buffered rows budget",
        );
      let bytes =
        c.object +
        4 * c.property +
        c.string +
        2 * key.length +
        c.array +
        c.arraySlot * values.length;
      for (const value of values) bytes += scalarBytes(value ?? null);
      let result: VectorGroup | undefined;
      this.ledger.append(bytes, "group directory entry", () => {
        const states = this.arena.create(this.spec, this.options);
        const group = { keyValues: [...values], states };
        this.groups.set(key, group);
        this.members.add(group);
        result = group;
        this.bytes += 64 + 2 * key.length + 16 * values.length;
        for (const alias in states) {
          if (!Object.hasOwn(states, alias)) continue;
          const state = states[alias];
          if (state) this.bytes += estimateAggregateStateBytes(state);
        }
      });
      this.enforceBudget();
      if (!result) throw new Error("Missing admitted group");
      return result;
    } finally {
      scratch.dispose();
    }
  }

  update(group: VectorGroup, alias: string, value: VectorAggregateValue): void {
    this.requireOpen();
    if (!this.members.has(group))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Group belongs to another directory");
    const state = Object.hasOwn(group.states, alias) ? group.states[alias] : undefined;
    if (!state) throw new LakeqlError("LAKEQL_TYPE_ERROR", "Missing group accumulator", { alias });
    const before = estimateAggregateStateBytes(state);
    try {
      this.arena.updateValue(state, value, this.options);
    } finally {
      this.bytes += estimateAggregateStateBytes(state) - before;
    }
    this.enforceBudget();
  }

  values(): IterableIterator<VectorGroup> {
    this.requireOpen();
    return this.groups.values();
  }

  /** Prefix-applied merge. Reconstructed source state is temporary; retained
   * target keys and accumulators receive independent arena admission. */
  merge(snapshot: VectorGroupByStateSnapshot): void {
    this.requireOpen();
    for (const entry of snapshot.groups) {
      if (entry.keyValues.length !== this.keys.length)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Group checkpoint key arity mismatch");
      for (const alias in entry.states) {
        if (!Object.hasOwn(entry.states, alias)) continue;
        const target = Object.hasOwn(this.spec, alias) ? this.spec[alias] : undefined;
        if (!target || target.op !== entry.states[alias]?.op)
          throw new LakeqlError("LAKEQL_TYPE_ERROR", "Group checkpoint specification mismatch", {
            alias,
          });
      }
    }
    for (const entry of snapshot.groups) {
      const scratch = this.scope.childScope();
      try {
        const keys = scratch.allocate(
          c.array + c.arraySlot * entry.keyValues.length,
          "restored group key tuple",
          () => entry.keyValues.map((value) => restoreGroupValue(value, scratch)),
        ).value;
        const states = restoreVectorAggregateStates(entry.states, this.options, scratch);
        const group = this.getOrCreate(keys);
        for (const alias in states) {
          if (!Object.hasOwn(states, alias)) continue;
          const source = states[alias];
          const target = group.states[alias];
          if (!source || !target) continue;
          const before = estimateAggregateStateBytes(target);
          try {
            this.arena.mergeState(target, source, this.options);
          } finally {
            this.bytes += estimateAggregateStateBytes(target) - before;
          }
          this.enforceBudget();
        }
      } finally {
        scratch.dispose();
      }
    }
  }

  checkpoint(): OwnedMemory<VectorGroupByStateSnapshot> {
    this.requireOpen();
    const output = this.scope.childScope();
    try {
      const snapshot = output.allocate(
        c.object + c.property + c.array + c.arraySlot * this.size,
        "group checkpoint directory",
        (): VectorGroupByStateSnapshot => ({ groups: [] }),
      ).value;
      for (const group of this.groups.values()) {
        const entry = output.allocate(
          c.object + 2 * c.property + c.array + c.arraySlot * group.keyValues.length,
          "group checkpoint entry",
          () => ({
            keyValues: [] as VectorGroupByStateSnapshot["groups"][number]["keyValues"],
            states: snapshotVectorAggregateStates(group.states, output),
          }),
        ).value;
        for (const key of group.keyValues)
          entry.keyValues.push(snapshotGroupValue(key ?? null, output));
        snapshot.groups.push(entry);
      }
      return output.retainAllocations(snapshot);
    } finally {
      output.dispose();
    }
  }

  /** Closes mutation even when result admission fails. */
  finish(enforceOutputRows = true): OwnedMemory<Row[]> {
    this.requireOpen();
    const output = this.scope.childScope();
    try {
      const limit = this.options.budget?.maxOutputRows;
      if (enforceOutputRows && limit !== undefined && this.size > limit)
        throw new LakeqlError(
          "LAKEQL_BUDGET_EXCEEDED",
          "Grouped result exceeds output rows budget",
        );
      const rows = output.allocate(
        c.array + c.arraySlot * this.size,
        "group result rows",
        (): Row[] => [],
      ).value;
      for (const group of this.groups.values()) {
        let bytes = c.object;
        for (const alias in this.spec) {
          if (Object.hasOwn(this.spec, alias)) bytes += c.property;
        }
        for (let i = 0; i < this.keys.length; i++) {
          const name = this.keys[i];
          if (name !== undefined)
            bytes +=
              c.property +
              c.string +
              c.stringCodeUnit * name.length +
              scalarBytes(group.keyValues[i] ?? null);
        }
        const row = output.allocate(bytes, "group result keys", (): Row => ({})).value;
        for (let i = 0; i < this.keys.length; i++) {
          const name = this.keys[i];
          if (name !== undefined) row[name] = group.keyValues[i] ?? null;
        }
        // The finalizer admits aggregate properties/payload and its temporary row.
        Object.assign(row, finalizeVectorAggregateStates(group.states, output));
        rows.push(row);
      }
      return output.retainAllocations(rows);
    } finally {
      output.dispose();
      this.dispose();
    }
  }

  dispose(): void {
    this.scope.dispose();
  }

  private enforceBudget(): void {
    const limit = this.options.budget?.maxMemoryBytes;
    if (limit !== undefined && this.bytes > limit)
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        "Group directory exceeds operator memory budget",
        { metric: "operator memory bytes", limit, actual: this.bytes },
      );
  }
  private requireOpen(): void {
    if (this.scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Group directory is disposed");
  }
}

function scalarBytes(value: Scalar): number {
  if (typeof value === "string") return c.string + c.stringCodeUnit * value.length;
  if (typeof value === "bigint") return c.bigint + decimalStringAllocationBytes(value);
  if (isTimestampValue(value))
    return c.timestamp + c.bigint + decimalStringAllocationBytes(value.epochNanoseconds);
  return c.number;
}
