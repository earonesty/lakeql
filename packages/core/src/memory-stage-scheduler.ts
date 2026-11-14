import { isLakeqlError, LakeqlError } from "./errors.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";

export interface MemoryStageRequest<T> {
  budget: MemoryBudget;
  /** Total new allocations, including scratch and output. Inputs stay charged separately. */
  bytes: number;
  purpose: string;
  inputs?: readonly OwnedMemory<unknown>[];
  signal?: AbortSignal;
  /** Perform a complete stage without awaiting another scheduled stage. Nested
   * operators consume this scope's admission. Return a value, not an ownership handle.
   * Acquire transport/semaphore slots inside this callback, after admission.
   * Input handles are included in output ownership unless explicitly released
   * here after proving the output does not alias them. Separate execution guards
   * keep their backing charged until the callback settles, including on abort.
   */
  execute(
    scope: MemoryScope,
    signal: AbortSignal,
    inputs: readonly OwnedMemory<unknown>[],
  ): T | Promise<T>;
}

interface Stage {
  budget: MemoryBudget;
  bytes: number;
  purpose: string;
  inputs: OwnedMemory<unknown>[];
  controller: AbortController;
  detach(): void;
  run(scope: MemoryScope): Promise<void>;
  reject(error: unknown): void;
}

/** FIFO whole-stage admission for independent jobs in one allocation pool.
 * Only running stages are progress sources. Once they settle, retained outputs
 * and unrelated allocations cannot keep an impossible request waiting forever.
 * One outstanding stage per job budget prevents same-job recursive admission.
 * This is an execution primitive; it does not infer missing decoder working sets.
 */
export class MemoryStageScheduler {
  private readonly queue: Stage[] = [];
  private readonly jobs = new Set<MemoryBudget>();
  private active = 0;
  private draining = false;
  private settling = 0;

  constructor(
    private readonly pool: MemoryBudget,
    private readonly maxStages = 64,
  ) {
    if (!Number.isSafeInteger(maxStages) || maxStages < 1)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Stage capacity must be a positive integer");
    if (pool !== pool.allocationPool)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Scheduler requires the root allocation pool",
      );
  }

  get queuedStages(): number {
    return this.queue.length;
  }

  get activeStages(): number {
    return this.active;
  }

  async run<T>(request: MemoryStageRequest<T>): Promise<OwnedMemory<T>> {
    if (!Number.isSafeInteger(request.bytes) || request.bytes < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid stage allocation extent");
    if (request.budget.allocationPool !== this.pool)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Stage requires the scheduler's allocation pool",
      );
    if (request.signal?.aborted) throw aborted();
    if (this.jobs.has(request.budget))
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "A job budget may have only one scheduled stage; nested work must use existing admission",
      );
    if (this.jobs.size >= this.maxStages)
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Memory stage queue capacity exceeded", {
        limit: this.maxStages,
      });
    this.jobs.add(request.budget);
    const inputs: OwnedMemory<unknown>[] = [];
    try {
      for (const input of request.inputs ?? []) inputs.push(input.retainIn(request.budget));
      if (request.signal?.aborted) throw aborted();
    } catch (error) {
      this.settling++;
      try {
        for (const input of inputs) {
          try {
            input.release();
          } catch {
            // Preserve admission failure while attempting every input cleanup.
          }
        }
      } finally {
        this.jobs.delete(request.budget);
        this.settling--;
        this.drain();
      }
      throw error;
    }

    return new Promise<OwnedMemory<T>>((resolve, reject) => {
      const controller = new AbortController();
      const cancel = () => {
        controller.abort();
        const index = this.queue.indexOf(stage);
        if (index >= 0) {
          this.queue.splice(index, 1);
          this.finish(stage);
          reject(aborted());
          this.drain();
        }
      };
      const stage: Stage = {
        budget: request.budget,
        bytes: request.bytes,
        purpose: request.purpose,
        inputs,
        controller,
        detach: () => request.signal?.removeEventListener("abort", cancel),
        reject,
        run: async (scope) => {
          let cancellationGuard: OwnedMemory<undefined> | undefined;
          let output: OwnedMemory<T> | undefined;
          let failure: { error: unknown } | undefined;
          const cancelScope = () => {
            if (scope.disposed) return;
            try {
              // Completed scratch stays pinned until execution settles. If an
              // opaque owner's retain fails, leave the scope live for final cleanup.
              cancellationGuard = scope.retainTreeAllocations(undefined);
              scope.dispose();
            } catch (error) {
              failure ??= { error };
            }
          };
          controller.signal.addEventListener("abort", cancelScope, { once: true });
          try {
            const scopedInputs = inputs.map((input) => scope.adopt(input.retain()));
            const value = await request.execute(scope, controller.signal, scopedInputs);
            if (controller.signal.aborted) throw aborted();
            output = scope.retainAllocations(value);
          } catch (error) {
            failure ??= { error };
          }
          this.settling++;
          try {
            controller.signal.removeEventListener("abort", cancelScope);
            try {
              scope.dispose();
            } catch (error) {
              failure ??= { error };
            }
            try {
              cancellationGuard?.release();
            } catch (error) {
              failure ??= { error };
            }
            this.active--;
            const finished = this.finish(stage);
            failure ??= finished;
            if (failure !== undefined || controller.signal.aborted) {
              try {
                output?.release();
              } catch (error) {
                failure ??= { error };
              }
              output = undefined;
            }
          } finally {
            this.settling--;
            this.drain();
          }
          if (controller.signal.aborted) reject(aborted());
          else if (failure !== undefined) reject(failure.error);
          else resolve(output as OwnedMemory<T>);
        },
      };
      request.signal?.addEventListener("abort", cancel, { once: true });
      this.queue.push(stage);
      if (request.signal?.aborted) cancel();
      else this.drain();
    });
  }

  private finish(stage: Stage): { error: unknown } | undefined {
    let failure: { error: unknown } | undefined;
    this.settling++;
    try {
      try {
        stage.detach();
      } catch (error) {
        failure = { error };
      }
      for (const input of stage.inputs) {
        try {
          input.release();
        } catch (error) {
          failure ??= { error };
        }
      }
    } finally {
      stage.inputs.length = 0;
      this.jobs.delete(stage.budget);
      this.settling--;
    }
    return failure;
  }

  private drain(): void {
    if (this.draining || this.settling > 0) return;
    this.draining = true;
    try {
      while (this.queue.length > 0) {
        const stage = this.queue[0];
        if (stage === undefined) break;
        const scope = new MemoryScope(stage.budget);
        try {
          scope.admit(stage.bytes, stage.purpose);
          if (stage.controller.signal.aborted) throw aborted();
        } catch (error) {
          scope.dispose();
          // Reclamation may synchronously cancel and remove this queued stage.
          if (this.queue[0] !== stage) continue;
          // Input handles may alias, so their reported byte sums are not an
          // exact lower bound. Let real admission decide; once active work
          // settles, retained inputs alone cannot justify continued waiting.
          if (
            this.active > 0 &&
            isLakeqlError(error) &&
            error.code === "LAKEQL_BUDGET_EXCEEDED" &&
            typeof error.details.limit === "number" &&
            stage.bytes <= error.details.limit
          )
            break;
          this.queue.shift();
          this.finish(stage);
          stage.reject(error);
          continue;
        }
        this.queue.shift();
        this.active++;
        void stage.run(scope);
      }
    } finally {
      this.draining = false;
    }
  }
}

function aborted(): LakeqlError {
  return new LakeqlError("LAKEQL_ABORTED", "Memory stage canceled");
}
