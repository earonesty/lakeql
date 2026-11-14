import { expect, it, vi } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MutableMemoryLedger } from "./mutable-memory-ledger.js";

it("uses bounded chunk metadata for many small appends", () => {
  const scope = new MemoryScope(new MemoryBudget(8 * 1024 * 1024));
  const reserve = vi.spyOn(scope.budget, "reserve");
  const growth = new MutableMemoryLedger(scope);
  let count = 0;
  for (let i = 0; i < 65536; i++)
    growth.append(32, "value", () => {
      count++;
    });
  expect(count).toBe(65536);
  expect(reserve.mock.calls.length).toBeLessThan(64);
  expect(scope.budget.usedBytes).toBeGreaterThan(32 * count);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  expect(() => growth.append(1, "closed", () => {})).toThrow(/disposed/);
});

it("falls back to exact payload admission and fails before mutation one byte below", () => {
  const measured = new MemoryScope(new MemoryBudget(65536));
  const growth = new MutableMemoryLedger(measured);
  growth.append(32, "value", () => {});
  growth.trim();
  const exact = measured.budget.usedBytes;
  measured.dispose();
  for (const bytes of [exact, exact - 1]) {
    const scope = new MemoryScope(new MemoryBudget(bytes));
    const candidate = new MutableMemoryLedger(scope);
    const mutate = vi.fn();
    if (bytes === exact) {
      candidate.append(32, "value", mutate);
      expect(mutate).toHaveBeenCalledOnce();
      expect(scope.budget.usedBytes).toBe(exact);
    } else {
      expect(() => candidate.append(32, "value", mutate)).toThrow(/budget/i);
      expect(mutate).not.toHaveBeenCalled();
      expect(scope.budget.usedBytes).toBe(0);
    }
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("reclaims spare capacity for a sibling while keeping committed payload charged", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool.child());
  const growth = new MutableMemoryLedger(scope);
  growth.append(32, "value", () => {});
  const before = pool.usedBytes;
  const sibling = pool.child().reserve(pool.availableBytes + 1, "sibling");
  expect(scope.budget.usedBytes).toBeLessThan(before);
  expect(scope.budget.usedBytes).toBeGreaterThanOrEqual(32);
  sibling.release();
  growth.append(64, "next", () => {});
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("protects pending bytes from reclaimers and rolls back a trimmed new chunk", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const growth = new MutableMemoryLedger(scope);
  growth.append(32, "first", () => {});
  const before = scope.budget.usedBytes;
  expect(() =>
    growth.append(1000, "failed", () => {
      growth.trim();
      throw Error("mutation failed");
    }),
  ).toThrow("mutation failed");
  expect(scope.budget.usedBytes).toBe(before);
  expect(() =>
    growth.append(64, "failed in existing chunk", () => {
      growth.trim();
      throw Error("mutation failed");
    }),
  ).toThrow("mutation failed");
  growth.trim();
  const after = scope.budget.usedBytes;
  expect(after).toBeLessThan(before);
  growth.append(32, "next", () => {});
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects reentrant mutation and closure during a mutation without leaking", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const growth = new MutableMemoryLedger(scope);
  expect(() => growth.append(32, "outer", () => growth.append(1, "inner", () => {}))).toThrow(
    /Reentrant/,
  );
  expect(scope.budget.usedBytes).toBe(0);
  expect(() => growth.append(32, "closing", () => scope.dispose())).toThrow(/closed during/);
  expect(scope.budget.usedBytes).toBe(0);
});

it("reuses released payload capacity across replacements without new ledger entries", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const reserve = vi.spyOn(scope.budget, "reserve");
  const ledger = new MutableMemoryLedger(scope);
  ledger.append(32, "initial", () => {});
  const admitted = scope.budget.usedBytes;
  for (let i = 0; i < 10000; i++) {
    ledger.append(32, "replacement", () => {});
    ledger.release(32);
  }
  expect(ledger.usedBytes).toBe(32);
  expect(scope.budget.usedBytes).toBe(admitted);
  expect(reserve).toHaveBeenCalledOnce();
  ledger.release(32);
  ledger.trim();
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("reclaims released storage across multiple chunks including their metadata", () => {
  const scope = new MemoryScope(new MemoryBudget(4 * 1024 * 1024));
  const ledger = new MutableMemoryLedger(scope);
  for (let i = 0; i < 10000; i++) ledger.append(32, "value", () => {});
  expect(ledger.usedBytes).toBe(320000);
  ledger.release(300000);
  const before = scope.budget.usedBytes;
  ledger.trim();
  expect(ledger.usedBytes).toBe(20000);
  expect(scope.budget.usedBytes).toBeLessThan(before - 300000);
  expect(() => ledger.release(20001)).toThrow(/extent/);
  expect(ledger.usedBytes).toBe(20000);
  ledger.append(100000, "large replacement", () => {});
  ledger.release(120000);
  ledger.trim();
  expect(ledger.usedBytes).toBe(0);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("trims a partial chunk without replacing its reservation handle", () => {
  const budget = new MemoryBudget(65536);
  const reserve = budget.reserve.bind(budget);
  const handles: ReturnType<MemoryBudget["reserve"]>[] = [];
  vi.spyOn(budget, "reserve").mockImplementation((bytes, purpose) => {
    const handle = reserve(bytes, purpose);
    vi.spyOn(handle, "partition").mockImplementation(() => {
      throw Error("unexpected partition");
    });
    handles.push(handle);
    return handle;
  });
  const scope = new MemoryScope(budget);
  const ledger = new MutableMemoryLedger(scope);
  ledger.append(64, "initial", () => {});
  const handle = handles[0];
  if (!handle) throw Error("missing reservation");
  const admitted = handle.bytes;
  ledger.trim();
  expect(handle.bytes).toBeLessThan(admitted);
  expect(budget.usedBytes).toBe(handle.bytes);
  ledger.release(32);
  ledger.trim();
  expect(ledger.usedBytes).toBe(32);
  expect(budget.usedBytes).toBe(handle.bytes);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});
