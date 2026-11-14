import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { StringKeyWorkspace } from "./string-key-workspace.js";

for (const cap of [20, 128]) {
  it(`preserves last keys and reuses charged storage at cap ${cap}`, () => {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    const workspace = new StringKeyWorkspace(scope);
    const source = ["unused", "b", null, "a", "b", "__proto__"];
    for (let row = 0; row < 100; row++) {
      workspace.withKeys(
        1,
        6,
        (i) => source[i] ?? null,
        (keys) => {
          const result = new Map<string, number>();
          keys.forEach((key, index) => {
            result.set(key, index);
          });
          expect(result).toEqual(
            new Map([
              ["a", 3],
              ["b", 4],
              ["__proto__", 5],
            ]),
          );
          expect(keys.has("b")).toBe(true);
          expect(keys.has("missing")).toBe(false);
        },
      );
      expect(budget.usedBytes).toBe(cap === 20 ? 20 : 64);
    }
    workspace.dispose();
    expect(budget.usedBytes).toBe(0);
    scope.dispose();
  });
}

it("rejects below the sorted extent before reading keys", () => {
  const budget = new MemoryBudget(19);
  const scope = new MemoryScope(budget);
  const workspace = new StringKeyWorkspace(scope);
  let reads = 0;
  expect(() =>
    workspace.withKeys(
      0,
      5,
      () => {
        reads++;
        return "a";
      },
      () => {},
    ),
  ).toThrow();
  expect(reads).toBe(0);
  expect(budget.usedBytes).toBe(0);
  workspace.dispose();
  scope.dispose();
});

it("keeps nested workspaces separate and restores depth after exceptions", () => {
  const budget = new MemoryBudget(64);
  const scope = new MemoryScope(budget);
  const workspace = new StringKeyWorkspace(scope);
  expect(() =>
    workspace.withKeys(0, 2, String, (outer) => {
      workspace.withKeys(4, 6, String, (inner) => {
        expect(inner.has("5")).toBe(true);
        expect(outer.has("1")).toBe(true);
        expect(outer.has("5")).toBe(false);
        throw new Error("visitor failed");
      });
    }),
  ).toThrow("visitor failed");
  expect(budget.usedBytes).toBe(32);
  workspace.withKeys(0, 2, String, (keys) => expect(keys.has("1")).toBe(true));
  expect(budget.usedBytes).toBe(32);
  workspace.dispose();
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("falls back on colliding hashes with bounded key inspections", () => {
  const hash = (key: string) => {
    let value = 2166136261;
    for (let i = 0; i < key.length; i++) value = Math.imul(value ^ key.charCodeAt(i), 16777619);
    return value >>> 0;
  };
  const source: string[] = [];
  for (let i = 0; source.length < 64; i++) if (hash(String(i)) % 128 === 0) source.push(String(i));
  const workspace = new StringKeyWorkspace();
  let reads = 0;
  workspace.withKeys(
    0,
    source.length,
    (i) => {
      reads++;
      return source[i] ?? null;
    },
    (keys) => {
      const actual = new Map<string, number>();
      keys.forEach((key, i) => {
        actual.set(key, i);
      });
      expect(actual).toEqual(new Map(source.map((key, i) => [key, i])));
      for (const key of source) expect(keys.has(key)).toBe(true);
      expect(keys.has("absent")).toBe(false);
    },
  );
  expect(reads).toBeLessThan(3000);
  workspace.dispose();
});

it("reuses an admitted extent without spending it again on each row", () => {
  const budget = new MemoryBudget(64);
  const scope = new MemoryScope(budget);
  scope.admit(64, "key workspace");
  const workspace = new StringKeyWorkspace(scope);
  for (let row = 0; row < 100; row++) {
    workspace.withKeys(0, 8, String, (keys) => expect(keys.has("7")).toBe(true));
  }
  expect(scope.availableAllocationBytes).toBe(0);
  workspace.dispose();
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("validates source extents and accepts empty input without budget", () => {
  const budget = new MemoryBudget(0);
  const scope = new MemoryScope(budget);
  const workspace = new StringKeyWorkspace(scope);
  for (const [start, end] of [
    [-1, 0],
    [2, 1],
    [0.5, 1],
    [0, Infinity],
    [0, 0x100000000],
  ]) {
    expect(() => workspace.withKeys(start ?? 0, end ?? 0, String, () => {})).toThrow(/extent/);
  }
  workspace.withKeys(
    0,
    0,
    () => {
      throw new Error("unused");
    },
    (keys) => {
      expect(keys.has("absent")).toBe(false);
      keys.forEach(() => {
        throw new Error("empty");
      });
    },
  );
  expect(budget.usedBytes).toBe(0);
  workspace.dispose();
  scope.dispose();
});

it("ignores stale slots when a reused hash workspace shrinks", () => {
  const workspace = new StringKeyWorkspace();
  workspace.withKeys(0, 64, String, (keys) => expect(keys.has("63")).toBe(true));
  workspace.withKeys(100, 102, String, (keys) => {
    const actual = new Map<string, number>();
    keys.forEach((key, index) => {
      actual.set(key, index);
    });
    expect(actual).toEqual(
      new Map([
        ["100", 100],
        ["101", 101],
      ]),
    );
    expect(keys.has("63")).toBe(false);
  });
  workspace.dispose();
});
