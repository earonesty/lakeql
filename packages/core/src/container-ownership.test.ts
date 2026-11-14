import { expect, it } from "vitest";
import type { Batch } from "./batch.js";
import {
  MATERIALIZATION_CHARGES,
  materializeOwnedBatchRows,
  planBatchMaterialization,
} from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory, retainContainerPayload } from "./memory-scope.js";
import { OwnedRowCollector } from "./owned-row-collector.js";

it("replaces only declared container storage while a sibling keeps the original valid", () => {
  const pool = new MemoryBudget(4096);
  const source = new MemoryScope(pool);
  const row = source.allocate(96, "payload", () => ({ value: 7 })).value;
  const directory = source.allocateContainer(80, "directory", () => [row]).value;
  const input = source.retainAllocations(directory);
  const sibling = input.retain();
  source.dispose();
  const target = new MemoryScope(pool.child(2048));
  try {
    retainContainerPayload(input, target);
    input.release();
    expect(sibling.value).toEqual([{ value: 7 }]);
    expect(pool.usedBytes).toBe(176);
    expect(target.budget.usedBytes).toBe(96);
    sibling.release();
    expect(pool.usedBytes).toBe(96);
    expect(row.value).toBe(7);
  } finally {
    input.release();
    sibling.release();
    target.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps mixed array and payload ownership intact at its existing exact boundary", () => {
  const pool = new MemoryBudget(100);
  const source = new MemoryScope(pool);
  const input = source.allocate(100, "mixed array and payload", () => [{ value: 7 }]);
  const target = new MemoryScope(pool.child(100));
  retainContainerPayload(input, target);
  source.dispose();
  expect(pool.usedBytes).toBe(100);
  expect(target.budget.usedBytes).toBe(100);
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("rolls back failed payload transfer without releasing the source or prior target output", () => {
  const pool = new MemoryBudget(4096);
  const source = new MemoryScope(pool);
  source.allocate(96, "payload", () => undefined);
  const array = source.allocateContainer(80, "directory", () => [{ value: 7 }]).value;
  const input = source.retainAllocations(array);
  source.dispose();
  const target = new MemoryScope(pool.child(37));
  target.allocate(37, "prior output", () => undefined);
  try {
    expect(() => retainContainerPayload(input, target)).toThrow(/budget/);
    expect(target.budget.usedBytes).toBe(37);
    expect(pool.usedBytes).toBe(213);
    expect(input.value).toEqual([{ value: 7 }]);
  } finally {
    target.dispose();
    input.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("collected binary rows retain borrowed backing after source and sibling directory release", () => {
  const pool = new MemoryBudget(16384);
  const collector = new OwnedRowCollector(pool);
  const siblings = [];
  let payloadAndBackingBytes = 0;
  for (let i = 0; i < 2; i++) {
    const source = new MemoryScope(pool);
    const data = Uint8Array.of(i, i + 1);
    const input = source.allocate<Batch>(2, "binary backing", () => ({
      rowCount: 1,
      columns: { value: { type: "binary-view", data, fixedLength: 2, length: 1 } },
    }));
    payloadAndBackingBytes +=
      planBatchMaterialization(input.value).retainedBytes -
      MATERIALIZATION_CHARGES.array -
      MATERIALIZATION_CHARGES.arraySlot +
      input.bytes;
    const rows = materializeOwnedBatchRows(input);
    siblings.push(rows.retain());
    collector.append(rows);
    rows.release();
    source.dispose();
  }
  const output = collector.finish();
  try {
    for (const sibling of siblings) sibling.release();
    expect(output.value.map((row) => [...(row.value as Uint8Array)])).toEqual([
      [0, 1],
      [1, 2],
    ]);
    expect(pool.usedBytes).toBe(
      payloadAndBackingBytes +
        MATERIALIZATION_CHARGES.array +
        2 * MATERIALIZATION_CHARGES.arraySlot,
    );
    const independent = output.retain();
    output.release();
    expect(independent.value[1]?.value).toEqual(Uint8Array.of(1, 2));
    independent.release();
  } finally {
    for (const sibling of siblings) sibling.release();
    output.release();
    collector.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps opaque external container owners pinned conservatively", () => {
  const pool = new MemoryBudget(100);
  const source = new MemoryScope(pool);
  const internal = source.allocateContainer(100, "external backing", () => [{ value: 7 }]);
  const wrap = <T>(owner: OwnedMemory<T>): OwnedMemory<T> => ({
    get value() {
      return owner.value;
    },
    bytes: owner.bytes,
    budget: owner.budget,
    retain: () => wrap(owner.retain()),
    retainIn: (budget) => wrap(owner.retainIn(budget)),
    release: () => owner.release(),
  });
  const external = wrap(internal.retain());
  const target = new MemoryScope(pool);
  retainContainerPayload(external, target);
  external.release();
  source.dispose();
  expect(pool.usedBytes).toBe(100);
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("does not leak a retained pin when the destination is already closed", () => {
  const pool = new MemoryBudget(100);
  const source = new MemoryScope(pool);
  const input = source.allocate(100, "mixed backing", () => [{}]);
  const target = new MemoryScope(pool);
  target.dispose();
  expect(() => retainContainerPayload(input, target)).toThrow(/disposed/);
  source.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("admits replacement overlap exactly and releases all partial collection on one-byte shortage", () => {
  const execute = (cap: number) => {
    const pool = new MemoryBudget(cap);
    const collector = new OwnedRowCollector(pool);
    let peak = 0;
    let payloadBytes = 0;
    try {
      for (let chunk = 0; chunk < 2; chunk++) {
        const source = new MemoryScope(pool);
        try {
          const input = source.allocate<Batch>(0, "borrowed numeric fixture", () => ({
            rowCount: 4,
            columns: { value: { type: "i32", values: Int32Array.of(1, 2, 3, 4) } },
          }));
          const plan = planBatchMaterialization(input.value);
          payloadBytes +=
            plan.retainedBytes -
            MATERIALIZATION_CHARGES.array -
            4 * MATERIALIZATION_CHARGES.arraySlot;
          const rows = materializeOwnedBatchRows(input);
          try {
            collector.append(rows);
          } finally {
            rows.release();
          }
        } finally {
          source.dispose();
        }
      }
      const output = collector.finish();
      try {
        expect(output.value.map((row) => row.value)).toEqual([1, 2, 3, 4, 1, 2, 3, 4]);
        expect(pool.usedBytes).toBe(
          payloadBytes + MATERIALIZATION_CHARGES.array + 8 * MATERIALIZATION_CHARGES.arraySlot,
        );
        peak = pool.peakBytes;
      } finally {
        output.release();
      }
    } finally {
      collector.dispose();
      expect(pool.usedBytes).toBe(0);
    }
    return peak;
  };
  const peak = execute(16384);
  expect(execute(peak)).toBe(peak);
  expect(() => execute(peak - 1)).toThrow(/budget/);
});

it.each([
  "separate",
  "combined",
] as const)("%s materialization preserves forecasts while declaring its container lifetime", (containerOwnership) => {
  const batch: Batch = {
    rowCount: 3,
    columns: { value: { type: "i32", values: Int32Array.of(7, 8, 9) } },
  };
  const plan = planBatchMaterialization(batch, Uint8Array.of(1, 0, 1));
  const pool = new MemoryBudget(plan.allocationBytes + 2048);
  const source = new MemoryScope(pool);
  const target = new MemoryScope(pool);
  try {
    const output = plan.execute(source, containerOwnership);
    expect(output.value).toEqual([{ value: 7 }, { value: 9 }]);
    expect(pool.usedBytes).toBe(plan.retainedBytes);
    retainContainerPayload(output, target);
    source.dispose();
    expect(pool.usedBytes).toBe(
      plan.retainedBytes -
        (containerOwnership === "separate"
          ? MATERIALIZATION_CHARGES.array + 2 * MATERIALIZATION_CHARGES.arraySlot
          : 0),
    );
  } finally {
    source.dispose();
    target.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});
