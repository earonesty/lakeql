import { describe, expect, it } from "vitest";
import { fanInWorkUnits, jsonWorkUnitBoundary } from "./work-unit.js";

describe("work-unit fan-in executor", () => {
  it("runs work units concurrently but reduces partials in input order", async () => {
    let active = 0;
    let peakActive = 0;
    const completed: number[] = [];
    const reduced: number[] = [];

    const result = await fanInWorkUnits({
      inputs: [30, 10, 20],
      initial: [] as number[],
      maxConcurrentTasks: 3,
      async run(delayMs, index) {
        active += 1;
        peakActive = Math.max(peakActive, active);
        await sleep(delayMs);
        active -= 1;
        completed.push(index);
        return { index };
      },
      boundary: jsonWorkUnitBoundary,
      reduce(accumulator, partial) {
        reduced.push(partial.index);
        accumulator.push(partial.index);
      },
    });

    expect(peakActive).toBe(3);
    expect(completed).toEqual([1, 2, 0]);
    expect(reduced).toEqual([0, 1, 2]);
    expect(result).toEqual([0, 1, 2]);
  });

  it("backpressures out-of-order partials so fan-in memory stays bounded", async () => {
    let active = 0;
    let peakActive = 0;
    let firstCompleted = false;
    const startedBeforeFirstCompleted: number[] = [];

    const result = await fanInWorkUnits({
      inputs: [30, 1, 1, 1],
      initial: [] as number[],
      maxConcurrentTasks: 4,
      maxBufferedPartials: 2,
      async run(delayMs, index) {
        active += 1;
        peakActive = Math.max(peakActive, active);
        if (!firstCompleted) startedBeforeFirstCompleted.push(index);
        await sleep(delayMs);
        if (index === 0) firstCompleted = true;
        active -= 1;
        return { index };
      },
      reduce(accumulator, partial) {
        accumulator.push(partial.index);
      },
    });

    expect(peakActive).toBe(2);
    expect(startedBeforeFirstCompleted).toEqual([0, 1]);
    expect(result).toEqual([0, 1, 2, 3]);
  });

  it("decouples fan-out execution from deployment-specific transports", async () => {
    type Deployment = "browser" | "cloudflare-worker" | "supabase-edge";
    type WorkUnit = { deployment: Deployment; rows: number[] };
    type Partial = { deployment: Deployment; index: number; subtotal: number };
    const deployments: Record<Deployment, (input: WorkUnit, index: number) => Promise<Partial>> = {
      async browser(input, index) {
        return runSerializedWorkUnit(input, index);
      },
      async "cloudflare-worker"(input, index) {
        return runSerializedWorkUnit(input, index);
      },
      async "supabase-edge"(input, index) {
        return runSerializedWorkUnit(input, index);
      },
    };
    const transported: Deployment[] = [];

    const result = await fanInWorkUnits({
      inputs: [
        { deployment: "browser", rows: [1, 2] },
        { deployment: "cloudflare-worker", rows: [3, 4] },
        { deployment: "supabase-edge", rows: [5, 6] },
      ] satisfies WorkUnit[],
      initial: { subtotal: 0, reducedDeployments: [] as Deployment[] },
      maxConcurrentTasks: 3,
      async run(input, index) {
        const runner = deployments[input.deployment];
        return runner(input, index);
      },
      boundary(partial) {
        transported.push(partial.deployment);
        return jsonWorkUnitBoundary(partial);
      },
      reduce(accumulator, partial, input, index) {
        expect(partial.index).toBe(index);
        expect(partial.deployment).toBe(input.deployment);
        accumulator.subtotal += partial.subtotal;
        accumulator.reducedDeployments.push(partial.deployment);
      },
    });

    expect(transported).toEqual(["browser", "cloudflare-worker", "supabase-edge"]);
    expect(result).toEqual({
      subtotal: 21,
      reducedDeployments: ["browser", "cloudflare-worker", "supabase-edge"],
    });
  });

  it.each([
    "run",
    "boundary",
    "reduce",
  ] as const)("drains active siblings after %s failure and preserves the first error", async (phase) => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let failed!: () => void;
    const failureReached = new Promise<void>((resolve) => {
      failed = resolve;
    });
    const original = new Error("original failure");
    const started: number[] = [];
    let settled = false;
    let siblingFinished = false;
    const result = fanInWorkUnits({
      inputs: [0, 1, 2],
      initial: [],
      maxConcurrentTasks: 2,
      async run(input) {
        started.push(input);
        if (input === 1) {
          await gate;
          siblingFinished = true;
          throw new Error("later sibling failure");
        }
        if (phase === "run") {
          failed();
          throw original;
        }
        return input;
      },
      boundary(partial) {
        if (phase === "boundary") {
          failed();
          throw original;
        }
        return partial;
      },
      reduce() {
        failed();
        throw original;
      },
    });
    const observed = result.then(
      () => {
        settled = true;
        return undefined;
      },
      (error: unknown) => {
        settled = true;
        return error;
      },
    );
    await failureReached;
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(settled).toBe(false);
    expect(started).toEqual([0, 1]);
    release();
    expect(await observed).toBe(original);
    expect(siblingFinished).toBe(true);
    expect(started).toEqual([0, 1]);
  });

  it("propagates an undefined rejection instead of waiting forever", async () => {
    await expect(
      fanInWorkUnits({
        inputs: [1],
        initial: [],
        async run() {
          throw undefined;
        },
        reduce() {},
      }),
    ).rejects.toBeUndefined();
  });

  it("rejects invalid concurrency limits with a typed error", async () => {
    await expect(
      fanInWorkUnits({
        inputs: [1],
        initial: 0,
        maxConcurrentTasks: 0,
        async run(input) {
          return input;
        },
        reduce() {},
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
      details: { maxConcurrentTasks: 0 },
    });
  });

  it("rejects invalid buffered partial limits with a typed error", async () => {
    await expect(
      fanInWorkUnits({
        inputs: [1],
        initial: 0,
        maxBufferedPartials: 0,
        async run(input) {
          return input;
        },
        reduce() {},
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_TYPE_ERROR",
      details: { maxBufferedPartials: 0 },
    });
  });
});

async function runSerializedWorkUnit<Deployment extends string>(
  input: { deployment: Deployment; rows: number[] },
  index: number,
): Promise<{ deployment: Deployment; index: number; subtotal: number }> {
  const portableInput = jsonWorkUnitBoundary(input);
  return {
    deployment: portableInput.deployment,
    index,
    subtotal: portableInput.rows.reduce((sum, value) => sum + value, 0),
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
