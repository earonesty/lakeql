import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { collectOwnedRows, OwnedRowCollector } from "./owned-row-collector.js";

it("admits directory promotion at the exact overlap boundary", () => {
  for (const cap of [519, 520]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    const source = scope.allocate(100, "input rows", () => [{ id: 1 }, { id: 2 }]);
    const collector = new OwnedRowCollector(scope.budget);
    collector.append(source);
    const second = scope.allocate(100, "second input rows", () => [{ id: 3 }, { id: 4 }]);
    if (cap === 519) {
      expect(() => collector.append(second)).toThrow(/budget/);
      expect(collector.rowCount).toBe(2);
      const output = collector.finish();
      expect(output.value).toBe(source.value);
      output.release();
      expect(scope.budget.usedBytes).toBe(200);
    } else {
      collector.append(second);
      expect(collector.rowCount).toBe(4);
      const output = collector.finish();
      expect(output.value).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
      expect(output.value[0]).toBe(source.value[0]);
      expect(scope.budget.peakBytes).toBe(520);
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
  const budget = new MemoryBudget(415);
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
    const sibling = budget.reserve(415, "sibling progress during remote cleanup");
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

it.each([
  false,
  true,
])("rolls back a partial copy and permits retry after promotion=%s", (promoted) => {
  const scope = new MemoryScope(new MemoryBudget(8192));
  const collector = new OwnedRowCollector(scope.budget);
  const failure = new Error("row getter failed");
  try {
    collector.append(scope.allocate(100, "first", () => [{ id: 0 }]));
    if (promoted) collector.append(scope.allocate(100, "second", () => [{ id: 1 }]));
    const broken = scope.allocate(
      100,
      "broken input",
      () =>
        new Proxy([{ id: 2 }, { id: 3 }], {
          get(target, key, receiver) {
            if (key === "1") throw failure;
            return Reflect.get(target, key, receiver);
          },
        }),
    );
    const before = scope.budget.usedBytes;
    expect(() => collector.append(broken)).toThrow(failure);
    // A failed growth may leave array backing expanded even after length rollback.
    expect(scope.budget.usedBytes).toBe(before + (promoted ? 32 : 0));
    expect(collector.rowCount).toBe(promoted ? 2 : 1);
    collector.append(scope.allocate(100, "retry", () => [{ id: 4 }]));
    const output = collector.finish();
    try {
      expect(output.value).toEqual(
        promoted ? [{ id: 0 }, { id: 1 }, { id: 4 }] : [{ id: 0 }, { id: 4 }],
      );
    } finally {
      output.release();
    }
  } finally {
    collector.dispose();
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([4, 64])("copies each source row once across %i chunks", (chunks) => {
  const budget = new MemoryBudget(1024 * 1024);
  const collector = new OwnedRowCollector(budget);
  let reads = 0;
  for (let chunk = 0; chunk < chunks; chunk++) {
    const scope = new MemoryScope(budget);
    try {
      const rows = scope.allocate(
        4096,
        "fixture chunk",
        () =>
          new Proxy(
            Array.from({ length: 1024 / chunks }, (_, row) => ({
              id: (chunk * 1024) / chunks + row,
            })),
            {
              get(target, key, receiver) {
                if (typeof key === "string" && /^[0-9]+$/.test(key)) reads++;
                return Reflect.get(target, key, receiver);
              },
            },
          ),
      );
      collector.append(rows);
    } finally {
      scope.dispose();
    }
  }
  expect(reads).toBe(1024);
  const output = collector.finish();
  try {
    expect(reads).toBe(1024);
    expect(output.value.map((row) => row.id)).toEqual(Array.from({ length: 1024 }, (_, i) => i));
  } finally {
    output.release();
    collector.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("releases private directory admission when reclamation closes collection", () => {
  const pool = new MemoryBudget(400);
  const source = new MemoryScope(pool);
  const collector = new OwnedRowCollector(pool);
  collector.append(source.allocate(100, "first", () => [{ id: 0 }]));
  const second = source.allocate(100, "second", () => [{ id: 1 }]);
  const detach = pool.registerReclaimer(() => collector.dispose());
  try {
    expect(() => collector.append(second)).toThrow(/disposed/);
    expect(pool.usedBytes).toBe(200);
    expect(() => collector.finish()).toThrow(/disposed/);
  } finally {
    detach();
    collector.dispose();
    source.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("pins active copy ownership when a getter disposes the collector, promoted=%s", (promoted) => {
  const budget = new MemoryBudget(8192);
  const scope = new MemoryScope(budget);
  const collector = new OwnedRowCollector(budget);
  collector.append(scope.allocate(100, "first", () => [{ id: 0 }]));
  if (promoted) collector.append(scope.allocate(100, "second", () => [{ id: 1 }]));
  const incoming = scope.allocate(
    100,
    "reentrant input",
    () =>
      new Proxy([{ id: 2 }], {
        get(target, key, receiver) {
          if (key === "0") {
            collector.dispose();
            scope.dispose();
            // Both the active source owners and the destination remain admitted.
            expect(budget.usedBytes).toBeGreaterThanOrEqual(296);
          }
          return Reflect.get(target, key, receiver);
        },
      }),
  );
  expect(() => collector.append(incoming)).toThrow(/disposed/);
  expect(budget.usedBytes).toBe(0);
  expect(() => collector.finish()).toThrow(/disposed/);
});

it("rejects recursive append and finish without corrupting the active append", () => {
  const scope = new MemoryScope(new MemoryBudget(8192));
  const collector = new OwnedRowCollector(scope.budget);
  const first = scope.allocate(100, "first", () => [{ id: 0 }]);
  collector.append(first);
  const incoming = scope.allocate(
    100,
    "reentrant input",
    () =>
      new Proxy([{ id: 1 }], {
        get(target, key, receiver) {
          if (key === "0") {
            expect(() => collector.append(first)).toThrow(/already active/);
            expect(() => collector.finish()).toThrow(/during collection append/);
          }
          return Reflect.get(target, key, receiver);
        },
      }),
  );
  collector.append(incoming);
  const output = collector.finish();
  expect(output.value).toEqual([{ id: 0 }, { id: 1 }]);
  scope.dispose();
  output.release();
  expect(scope.budget.usedBytes).toBe(0);
});
