import { MATERIALIZATION_CHARGES, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { planCompactAllocation, readAdmittedCompactHeader } from "./compact-allocation.js";
import type { CompactProjection } from "./compact-header.js";

const reader = (bytes: number[]) => ({
  view: new DataView(Uint8Array.from(bytes).buffer),
  offset: 0,
});

it("admits raw compact structures before construction at exact capacity", () => {
  const bytes = [0x15, 2, 0x18, 3, 65, 66, 67, 0];
  const cost = MATERIALIZATION_CHARGES;
  const expected =
    cost.object +
    2 * (cost.property + cost.string + 17 * cost.stringCodeUnit + cost.bigint) +
    cost.view;
  const input = reader(bytes);
  expect(planCompactAllocation(input)).toBe(expected);
  expect(input.offset).toBe(0);
  const scope = new MemoryScope(new MemoryBudget(expected));
  const raw = readAdmittedCompactHeader(input, scope);
  expect(raw.field_1).toBe(1);
  expect(raw.field_2).toEqual(Uint8Array.of(65, 66, 67));
  expect((raw.field_2 as Uint8Array).buffer).toBe(input.view.buffer);
  const retained = scope.retainAllocations(raw);
  scope.dispose();
  expect(retained.value.field_1).toBe(1);
  expect(retained.budget.usedBytes).toBe(expected);
  retained.release();
  expect(retained.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(expected - 1));
  const rejected = reader(bytes);
  expect(() => readAdmittedCompactHeader(rejected, tight)).toThrow(/budget/);
  expect(rejected.offset).toBe(0);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("preflights streamed projections without conversion and unwinds visitor failure", () => {
  const bytes = [0x19, 0x2c, 0x15, 2, 0, 0x15, 4, 0, 0];
  const seen: unknown[] = [];
  const projection: CompactProjection = {
    1: {
      list: { 1: "i32" },
      visit(value) {
        seen.push(value);
      },
    },
  };
  const capacity = planCompactAllocation(reader(bytes), projection);
  expect(seen).toEqual([]);
  const tight = new MemoryScope(new MemoryBudget(capacity - 1));
  expect(() => readAdmittedCompactHeader(reader(bytes), tight, projection)).toThrow(/budget/);
  expect(seen).toEqual([]);
  tight.dispose();
  const scope = new MemoryScope(new MemoryBudget(capacity));
  readAdmittedCompactHeader(reader(bytes), scope, projection);
  expect(seen).toEqual([{ field_1: 1 }, { field_1: 2 }]);
  scope.dispose();
  const failed = new MemoryScope(new MemoryBudget(capacity));
  expect(() =>
    readAdmittedCompactHeader(reader(bytes), failed, {
      1: {
        list: { 1: "i32" },
        visit() {
          throw new Error("conversion failed");
        },
      },
    }),
  ).toThrow("conversion failed");
  expect(failed.budget.usedBytes).toBe(0);
  failed.dispose();
});

it("validates malformed extents before admission and does not charge borrowed bytes twice", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  expect(() => readAdmittedCompactHeader(reader([0x18, 9, 1]), scope)).toThrow(/Truncated/);
  expect(scope.budget.peakBytes).toBe(0);
  const small = planCompactAllocation(reader([0x18, 1, 42, 0]));
  const large = planCompactAllocation(reader([0x18, 100, ...Array(100).fill(42), 0]));
  expect(large).toBe(small);
  scope.dispose();
});
