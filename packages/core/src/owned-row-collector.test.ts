import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { collectOwnedRows, OwnedRowCollector } from "./owned-row-collector.js";

it("admits chunk bookkeeping and final array at the exact combined boundary", () => {
  for (const cap of [711, 712]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    const source = scope.allocate(100, "input rows", () => [{ id: 1 }, { id: 2 }]);
    const collector = new OwnedRowCollector(scope.budget);
    collector.append(source);
    const second = scope.allocate(100, "second input rows", () => [{ id: 3 }, { id: 4 }]);
    collector.append(second);
    expect(collector.rowCount).toBe(4);
    if (cap === 711) {
      expect(() => collector.finish()).toThrow(/budget/);
      expect(scope.budget.usedBytes).toBe(200);
    } else {
      const output = collector.finish();
      expect(output.value).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
      expect(output.value[0]).toBe(source.value[0]);
      expect(scope.budget.peakBytes).toBe(712);
      expect(scope.budget.usedBytes).toBe(328);
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(328);
      output.release();
    }
    expect(() => collector.append(source)).toThrow(/disposed/);
    collector.dispose();
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("rolls back failed append and keeps caller ownership intact", () => {
  const scope = new MemoryScope(new MemoryBudget(291));
  const source = scope.allocate(100, "input", () => [{ id: 1 }]);
  const collector = new OwnedRowCollector(scope.budget);
  expect(() => collector.append(source)).toThrow(/budget/);
  expect(collector.rowCount).toBe(0);
  expect(source.value).toEqual([{ id: 1 }]);
  collector.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("closes producers and releases yielded handles on cancellation and producer failure", async () => {
  for (const abort of [true, false]) {
    const budget = new MemoryBudget(1024);
    const controller = new AbortController();
    let closed = false;
    async function* source() {
      const scope = new MemoryScope(budget);
      try {
        yield scope.allocate(64, "row", () => [{ id: 1 }]).retain();
        if (abort) controller.abort();
        else throw new Error("producer failure");
        yield scope.allocate(64, "row", () => [{ id: 2 }]).retain();
      } finally {
        scope.dispose();
        closed = true;
      }
    }
    await expect(collectOwnedRows(source(), budget, controller.signal)).rejects.toThrow(
      abort ? /aborted/ : /producer failure/,
    );
    expect(closed).toBe(true);
    expect(budget.usedBytes).toBe(0);
  }
});

it("releases failed collection before awaiting asynchronous producer closure", async () => {
  const budget = new MemoryBudget(511);
  let enter!: () => void;
  let finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const closing = new Promise<void>((resolve) => {
    finish = resolve;
  });
  async function* source() {
    try {
      for (let id = 0; id < 2; id++) {
        const scope = new MemoryScope(budget);
        const owner = scope.allocate(64, "row", () => [{ id }]).retain();
        scope.dispose();
        yield owner;
      }
    } finally {
      enter();
      await closing;
    }
  }
  const result = collectOwnedRows(source(), budget);
  const rejected = expect(result).rejects.toThrow(/budget/);
  await entered;
  try {
    expect(budget.usedBytes).toBe(0);
    const sibling = budget.reserve(511, "sibling progress during remote cleanup");
    sibling.release();
  } finally {
    finish();
  }
  await rejected;
  expect(budget.usedBytes).toBe(0);
});

it("releases accumulated output on abort while the producer next call is stalled", async () => {
  const budget = new MemoryBudget(512);
  const controller = new AbortController();
  let enter!: () => void;
  let resume!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const waiting = new Promise<void>((resolve) => {
    resume = resolve;
  });
  async function* source() {
    const scope = new MemoryScope(budget);
    const owner = scope.allocate(64, "transferred row", () => [{ id: 1 }]).retain();
    scope.dispose();
    yield owner;
    enter();
    await waiting;
  }
  const result = collectOwnedRows(source(), budget, controller.signal);
  const rejected = expect(result).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await entered;
  try {
    expect(budget.usedBytes).toBeGreaterThan(0);
    controller.abort();
    expect(budget.usedBytes).toBe(0);
    const sibling = budget.reserve(512, "progress while cancelled producer waits");
    sibling.release();
  } finally {
    resume();
  }
  await rejected;
  expect(budget.usedBytes).toBe(0);
});

it("hands off a single batch without allocating a duplicate result array", () => {
  const scope = new MemoryScope(new MemoryBudget(292));
  const source = scope.allocate(100, "rows", () => [{ id: 1 }]);
  const collector = new OwnedRowCollector(scope.budget);
  collector.append(source);
  const output = collector.finish();
  expect(output.value).toBe(source.value);
  expect(scope.budget.peakBytes).toBe(292);
  expect(scope.budget.usedBytes).toBe(100);
  scope.dispose();
  expect(output.value).toEqual([{ id: 1 }]);
  expect(scope.budget.usedBytes).toBe(100);
  output.release();
  expect(scope.budget.usedBytes).toBe(0);
});

it("releases collected output at producer failure before asynchronous unwind", async () => {
  const budget = new MemoryBudget(512);
  const original = new Error("producer admission failed");
  let enter!: () => void;
  let resume!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const waiting = new Promise<void>((resolve) => {
    resume = resolve;
  });
  async function* source(fail: (error: unknown) => never) {
    try {
      const scope = new MemoryScope(budget);
      const owner = scope.allocate(64, "row", () => [{ id: 1 }]).retain();
      scope.dispose();
      yield owner;
      fail(original);
    } finally {
      enter();
      await waiting;
    }
  }
  const result = collectOwnedRows(source, budget);
  const rejected = expect(result).rejects.toBe(original);
  await entered;
  try {
    expect(budget.usedBytes).toBe(0);
    const sibling = budget.reserve(512, "sibling progress during failed producer unwind");
    sibling.release();
  } finally {
    resume();
  }
  await rejected;
});
