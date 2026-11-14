import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryBudget, MemoryReservation } from "./memory-budget.js";

export interface WorkUnitFanInOptions<Input, Partial, Accumulator> {
  inputs: readonly Input[];
  initial: Accumulator;
  /** Scheduler metadata only; task and accumulator payloads need their own owners. */
  memoryBudget?: MemoryBudget;
  maxConcurrentTasks?: number;
  maxBufferedPartials?: number;
  run(input: Input, index: number): Promise<Partial>;
  reduce(
    accumulator: Accumulator,
    partial: Partial,
    input: Input,
    index: number,
  ): void | Promise<void>;
  boundary?(partial: Partial, input: Input, index: number): Partial | Promise<Partial>;
}

export async function fanInWorkUnits<Input, Partial, Accumulator>(
  options: WorkUnitFanInOptions<Input, Partial, Accumulator>,
): Promise<Accumulator> {
  const maxConcurrentTasks = options.maxConcurrentTasks ?? 1;
  validateMaxConcurrentTasks(maxConcurrentTasks);
  const maxBufferedPartials = options.maxBufferedPartials ?? maxConcurrentTasks;
  validateMaxBufferedPartials(maxBufferedPartials);
  const directory = options.memoryBudget?.reserve(
    c.object + 10 * c.property,
    "work-unit scheduler",
  );
  const partials = new Map<number, { value: Partial; slot: MemoryReservation | undefined }>();
  let nextInput = 0;
  let nextReduce = 0;
  let active = 0;
  let failure: unknown;
  let failed = false;
  let stopped = false;
  let notify: (() => void) | undefined;

  const wake = () => {
    notify?.();
    notify = undefined;
  };
  const wait = () =>
    new Promise<void>((resolve) => {
      notify = resolve;
    });
  const launchReadyInputs = () => {
    while (
      !failed &&
      !stopped &&
      active < maxConcurrentTasks &&
      nextInput < options.inputs.length &&
      active + partials.size < maxBufferedPartials
    ) {
      const index = nextInput;
      nextInput += 1;
      const input = options.inputs[index] as Input;
      // Admit bookkeeping before invoking user code or constructing its promise.
      const slot = options.memoryBudget?.reserve(
        4 * c.object + 8 * c.property,
        "work-unit pending task",
      );
      active += 1;
      void runInput(input, index, slot);
    }
  };
  const runInput = async (input: Input, index: number, slot: MemoryReservation | undefined) => {
    let transferred = false;
    try {
      const partial = await options.run(input, index);
      const value =
        options.boundary === undefined ? partial : await options.boundary(partial, input, index);
      partials.set(index, { value, slot });
      transferred = true;
    } catch (error) {
      if (!failed) {
        failed = true;
        failure = error;
      }
    } finally {
      if (!transferred) slot?.release();
      active -= 1;
      wake();
    }
  };

  try {
    launchReadyInputs();
    while (nextReduce < options.inputs.length) {
      if (failed) throw failure;
      while (!failed && partials.has(nextReduce)) {
        const input = options.inputs[nextReduce] as Input;
        const partial = partials.get(nextReduce);
        partials.delete(nextReduce);
        try {
          if (partial !== undefined) {
            await options.reduce(options.initial, partial.value, input, nextReduce);
          }
        } finally {
          partial?.slot?.release();
        }
        nextReduce += 1;
      }
      if (failed) throw failure;
      launchReadyInputs();
      if (nextReduce >= options.inputs.length) break;
      await wait();
    }
    return options.initial;
  } catch (error) {
    if (!failed) {
      failed = true;
      failure = error;
    }
    throw failure;
  } finally {
    // Callers may dispose shared task storage once this promise settles. Keep
    // that boundary after all started reads and transport callbacks finish.
    stopped = true;
    while (active > 0) await wait();
    for (const partial of partials.values()) partial.slot?.release();
    partials.clear();
    directory?.release();
  }
}

export function jsonWorkUnitBoundary<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function validateMaxConcurrentTasks(maxConcurrentTasks: number): void {
  if (!Number.isInteger(maxConcurrentTasks) || maxConcurrentTasks < 1) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "maxConcurrentTasks must be a positive integer", {
      maxConcurrentTasks,
    });
  }
}

function validateMaxBufferedPartials(maxBufferedPartials: number): void {
  if (!Number.isInteger(maxBufferedPartials) || maxBufferedPartials < 1) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "maxBufferedPartials must be a positive integer", {
      maxBufferedPartials,
    });
  }
}
