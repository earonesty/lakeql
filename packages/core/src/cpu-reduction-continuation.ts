import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, type Selection, selectedRowCount } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { OwnedAggregateState } from "./owned-aggregate-state.js";
import { OwnedGroupDirectory } from "./owned-group-directory.js";
import type {
  BackendExecutionContext,
  PhysicalGroupedReduce,
  PhysicalOutputValue,
  PhysicalReduce,
  PhysicalReductionContinuation,
} from "./physical.js";
import { throwIfAborted } from "./store.js";
import type { Row } from "./types.js";

/** CPU terminal reduction session. Failed appends close the session because a
 * prefix may have committed; replay must start from an explicit checkpoint.
 */
export class CpuReductionContinuation implements PhysicalReductionContinuation {
  private readonly scope: MemoryScope;
  private readonly owner: OwnedAggregateState | OwnedGroupDirectory;
  private readonly now: () => number;
  private readonly startedAt: number;

  constructor(
    readonly backendId: string,
    operator: PhysicalReduce | PhysicalGroupedReduce,
    private readonly context: BackendExecutionContext & { allocationScope: MemoryScope },
  ) {
    this.now = context.now ?? Date.now;
    this.startedAt = context.queryStartedAt ?? this.now();
    this.scope = context.allocationScope.childScope(
      context.budget?.signal ? [context.budget.signal] : [],
    );
    try {
      this.check();
      const options = context.budget === undefined ? {} : { budget: context.budget };
      if (operator.kind === "grouped-reduce") {
        const owner = OwnedGroupDirectory.allocate(
          this.scope,
          operator.keys,
          operator.aggregates,
          operator.maxGroups === undefined
            ? options
            : { ...options, maxGroups: operator.maxGroups },
        );
        this.owner = owner;
      } else {
        const owner = new OwnedAggregateState(this.scope, operator.aggregates, options);
        this.owner = owner;
      }
      if (context.priorOutput !== undefined) this.mergeSnapshot(context.priorOutput);
      this.check();
    } catch (error) {
      this.scope.dispose();
      throw error;
    }
  }

  async append(
    batch: Batch,
    selection?: Selection,
  ): Promise<{ inputRows: number; selectedRows: number; elapsedMs: number }> {
    const started = this.now();
    try {
      this.check();
      this.owner.updateBatch(batch, selection);
      this.check();
      return {
        inputRows: batch.rowCount,
        selectedRows: selectedRowCount(batch.rowCount, selection),
        elapsedMs: this.now() - started,
      };
    } catch (error) {
      this.dispose();
      throw error;
    }
  }

  async mergeCheckpoint(checkpoint: PhysicalOutputValue): Promise<void> {
    try {
      this.check();
      this.mergeSnapshot(checkpoint);
      this.check();
    } catch (error) {
      this.dispose();
      throw error;
    }
  }

  private mergeSnapshot(checkpoint: PhysicalOutputValue): void {
    if (this.owner instanceof OwnedGroupDirectory) {
      if (checkpoint.kind !== "grouped-aggregate-snapshot")
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Grouped continuation requires a grouped checkpoint",
        );
      this.owner.merge(checkpoint.snapshot);
    } else {
      if (checkpoint.kind !== "aggregate-snapshot")
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Reduction continuation requires an aggregate checkpoint",
        );
      this.owner.merge(checkpoint.snapshot);
    }
  }

  async checkpoint(): Promise<OwnedMemory<PhysicalOutputValue>> {
    this.check();
    const output = this.scope.childScope();
    try {
      const value = output.allocate(
        c.object + 2 * c.property,
        "physical reduction checkpoint",
        (): PhysicalOutputValue =>
          this.owner instanceof OwnedGroupDirectory
            ? {
                kind: "grouped-aggregate-snapshot",
                snapshot: output.adopt(this.owner.checkpoint()).value,
              }
            : { kind: "aggregate-snapshot", snapshot: output.adopt(this.owner.checkpoint()).value },
      ).value;
      this.check();
      return output.retainAllocations(value);
    } finally {
      output.dispose();
    }
  }

  async finish(): Promise<OwnedMemory<Row[]>> {
    try {
      this.check();
      if (
        this.context.enforceOutputRows !== false &&
        this.context.budget?.maxOutputRows !== undefined &&
        this.context.budget.maxOutputRows <
          (this.owner instanceof OwnedGroupDirectory ? this.owner.size : 1)
      )
        throw new LakeqlError(
          "LAKEQL_BUDGET_EXCEEDED",
          "Reduction result exceeds output rows budget",
        );
      const output = this.scope.childScope();
      try {
        const owner = this.owner;
        const rows =
          owner instanceof OwnedGroupDirectory
            ? output.adopt(owner.finish(this.context.enforceOutputRows !== false)).value
            : output.allocate(c.array + c.arraySlot, "physical reduction rows", () => [
                output.adopt(owner.finish()).value,
              ]).value;
        this.check();
        return output.retainAllocations(rows);
      } finally {
        output.dispose();
      }
    } finally {
      this.dispose();
    }
  }

  dispose(): void {
    this.scope.dispose();
  }

  private check(): void {
    throwIfAborted(this.context.budget?.signal);
    if (this.scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Reduction continuation is disposed");
    const limit = this.context.budget?.maxElapsedMs;
    if (limit !== undefined && this.now() - this.startedAt > limit)
      throw new LakeqlError(
        "LAKEQL_BUDGET_EXCEEDED",
        "Reduction continuation exceeded elapsed time budget",
      );
  }
}
