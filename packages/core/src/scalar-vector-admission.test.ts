import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { scalarVectorValue, type Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { OwnedAggregateState } from "./owned-aggregate-state.js";
import { TimestampValue } from "./timestamp.js";

it("admits packed text before touching its payload and preserves Unicode and row windows", () => {
  const data = new TextEncoder().encode("skipé😀");
  let reads = 0;
  const vector: Vector = {
    type: "utf8-view",
    length: 1,
    rowOffset: 1,
    ranges: new Uint32Array([0, 4, 4, data.length - 4]),
    get data() {
      reads++;
      return data;
    },
  };
  const bytes = c.view + c.string + c.stringCodeUnit * (data.length - 4);
  const rejected = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => scalarVectorValue(vector, 0, rejected)).toThrow(/budget/i);
  expect(reads).toBe(0);
  expect(rejected.budget.usedBytes).toBe(0);
  rejected.dispose();
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(scalarVectorValue(vector, 0, scope)).toBe("é😀");
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("propagates admission through union and dictionary indirection", () => {
  const text: Vector = {
    type: "utf8-view",
    data: new TextEncoder().encode("abcd"),
    fixedLength: 2,
    length: 2,
  };
  const dict: Vector = { type: "dict", indices: new Uint32Array([1, 0]), dictionary: text };
  const union: Vector = {
    type: "union",
    tags: new Uint8Array([0]),
    offsets: new Uint32Array([0]),
    children: [dict],
  };
  const bytes = c.view + c.string + 4;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(scalarVectorValue(union, 0, scope)).toBe("cd");
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
});

it("admits timestamp objects and skips allocations for null or borrowed scalars", () => {
  const vector: Vector = {
    type: "timestamp",
    values: new BigInt64Array([-123n, 1n]),
    unit: "micros",
    isAdjustedToUTC: false,
    valid: new Uint8Array([1, 0]),
  };
  const scope = new MemoryScope(new MemoryBudget(c.timestamp + c.bigint));
  expect(scalarVectorValue(vector, 1, scope)).toBeNull();
  expect(scope.budget.usedBytes).toBe(0);
  expect(scalarVectorValue({ type: "utf8", values: ["borrowed"] }, 0, scope)).toBe("borrowed");
  expect(scope.budget.usedBytes).toBe(0);
  expect(scalarVectorValue(vector, 0, scope)).toEqual(
    new TimestampValue(-123000n, "micros", false),
  );
  expect(scope.budget.usedBytes).toBe(c.timestamp + c.bigint);
  scope.dispose();
});

it("releases per-row decode scratch while retaining only distinct accumulator values", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { value: { op: "count_distinct" } });
  const vector: Vector = {
    type: "utf8-view",
    data: new TextEncoder().encode("é😀"),
    fixedLength: 6,
    length: 1,
  };
  owner.updateVectorValue("value", vector, 0);
  const used = parent.budget.usedBytes;
  for (let i = 0; i < 1024; i++) owner.updateVectorValue("value", vector, 0);
  expect(parent.budget.usedBytes).toBe(used);
  const result = owner.finish();
  expect(result.value).toEqual({ value: 1 });
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("releases decoded binary scratch when aggregate type validation rejects it", () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const owner = new OwnedAggregateState(parent, { value: { op: "count" } });
  const used = parent.budget.usedBytes;
  expect(() =>
    owner.updateVectorValue(
      "value",
      { type: "binary-view", data: new Uint8Array(2), fixedLength: 2, length: 1 },
      0,
    ),
  ).toThrow(/aggregate scalar/);
  expect(parent.budget.usedBytes).toBe(used);
  owner.dispose();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("admits signed and unsigned bigint reads and wide timestamp construction", () => {
  const vectors: Vector[] = [
    { type: "i64", values: new BigInt64Array([-123n]) },
    { type: "u64", values: new BigUint64Array([123n]) },
    {
      type: "timestamp96",
      low: new BigUint64Array([1n]),
      high: new Int32Array([1]),
      unit: "nanos",
      isAdjustedToUTC: true,
    },
  ];
  for (const vector of vectors) {
    const bytes = vector.type === "timestamp96" ? c.timestamp + c.bigint + c.object : c.bigint;
    const rejected = new MemoryScope(new MemoryBudget(bytes - 1));
    expect(() => scalarVectorValue(vector, 0, rejected)).toThrow(/budget/i);
    expect(rejected.budget.usedBytes).toBe(0);
    rejected.dispose();
    const scope = new MemoryScope(new MemoryBudget(bytes));
    expect(scalarVectorValue(vector, 0, scope)).toEqual(scalarVectorValue(vector, 0));
    expect(scope.budget.usedBytes).toBe(bytes);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});
