import { LakeqlError } from "./errors.js";

export interface WorkUnitFanInOptions<Input, Partial, Accumulator> {
  inputs: readonly Input[];
  initial: Accumulator;
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
  const partials = new Map<number, Partial>();
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
      const input = options.inputs[index];
      if (input === undefined) continue;
      active += 1;
      void runInput(input, index);
    }
  };
  const runInput = async (input: Input, index: number) => {
    try {
      const partial = await options.run(input, index);
      partials.set(
        index,
        options.boundary === undefined ? partial : await options.boundary(partial, input, index),
      );
    } catch (error) {
      if (!failed) {
        failed = true;
        failure = error;
      }
    } finally {
      active -= 1;
      wake();
    }
  };

  try {
    launchReadyInputs();
    while (nextReduce < options.inputs.length) {
      if (failed) throw failure;
      while (!failed && partials.has(nextReduce)) {
        const input = options.inputs[nextReduce];
        const partial = partials.get(nextReduce);
        partials.delete(nextReduce);
        if (input !== undefined && partial !== undefined) {
          await options.reduce(options.initial, partial, input, nextReduce);
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
    partials.clear();
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
