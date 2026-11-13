import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type PackedByteVector } from "./batch.js";
import { concatAllocationBytes } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatOwnedBatches } from "./vector-sort.js";

function packed(
  type: PackedByteVector["type"],
  values: readonly (Uint8Array | null)[],
): PackedByteVector {
  const parts = [Uint8Array.of(99), ...values];
  const data = new Uint8Array(parts.reduce((sum, value) => sum + (value?.length ?? 0), 0));
  const ranges = new Uint32Array(parts.length * 2);
  let offset = 0;
  parts.forEach((value, row) => {
    ranges[row * 2] = offset;
    ranges[row * 2 + 1] = value?.length ?? 0;
    if (value !== null) {
      data.set(value, offset);
      offset += value.length;
    }
  });
  return {
    type,
    data,
    ranges,
    rowOffset: 1,
    length: values.length,
    valid: Uint8Array.from(values, (value) => (value === null ? 0 : 1)),
  };
}

it.each([
  "utf8-view",
  "binary-view",
] as const)("admits and copies direct %s without materializing rows", (type) => {
  const encode = (value: string) => new TextEncoder().encode(value);
  const values =
    type === "utf8-view"
      ? [encode("\ufeffé中"), null, encode(""), encode("💡"), encode("tail")]
      : [Uint8Array.of(255, 0), null, new Uint8Array(), Uint8Array.of(128), Uint8Array.of(9)];
  const batches: Batch[] = [values.slice(0, 3), values.slice(3)].map((part) => ({
    rowCount: part.length,
    columns: { x: packed(type, part) },
  }));
  const bytes = 5 * 9 + values.reduce((sum, value) => sum + (value?.length ?? 0), 0);
  expect(concatAllocationBytes(batches)).toBe(bytes);
  const expected = structuredClone(batches.flatMap(materializeBatchRows));
  for (const cap of [bytes, bytes - 1]) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    const inputs = batches.map((batch) => scope.retainAllocations(batch));
    try {
      if (cap < bytes) {
        expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
        expect(budget.peakBytes).toBe(0);
      } else {
        const output = concatOwnedBatches(inputs);
        try {
          expect(output.value.columns.x?.type).toBe(type);
          expect(materializeBatchRows(output.value)).toEqual(expected);
          expect(budget.usedBytes).toBe(bytes);
          for (const batch of batches) {
            const vector = batch.columns.x as PackedByteVector;
            vector.data.fill(0);
          }
          expect(materializeBatchRows(output.value)).toEqual(expected);
        } finally {
          output.release();
        }
      }
    } finally {
      for (const input of inputs) input.release();
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});

it("copies fixed-width sliced binary payload and omits null bytes", () => {
  const batches: Batch[] = [
    {
      rowCount: 2,
      columns: {
        x: {
          type: "binary-view",
          data: Uint8Array.of(99, 99, 1, 2, 3, 4),
          fixedLength: 2,
          rowOffset: 1,
          length: 2,
          valid: Uint8Array.of(1, 0),
        },
      },
    },
    {
      rowCount: 1,
      columns: { x: { type: "binary-view", data: Uint8Array.of(5, 6), fixedLength: 2, length: 1 } },
    },
  ];
  expect(concatAllocationBytes(batches)).toBe(31);
  const budget = new MemoryBudget(31);
  const scope = new MemoryScope(budget);
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  try {
    const output = concatOwnedBatches(inputs);
    try {
      expect(materializeBatchRows(output.value)).toEqual([
        { x: Uint8Array.of(1, 2) },
        { x: null },
        { x: Uint8Array.of(5, 6) },
      ]);
    } finally {
      output.release();
    }
  } finally {
    for (const input of inputs) input.release();
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it.each([24, 23])("charges packed input/output overlap and detaches output at cap %s", (cap) => {
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  const batches: Batch[] = [1, 3].map((value) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "binary-view",
        data: scope.allocate(2, "packed input", () => Uint8Array.of(value, value + 1)).value,
        fixedLength: 2,
        length: 1,
      },
    },
  }));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  scope.dispose();
  try {
    if (cap === 23) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(budget.peakBytes).toBe(4);
    } else {
      const output = concatOwnedBatches(inputs);
      try {
        expect(budget.peakBytes).toBe(24);
        for (const input of inputs) input.release();
        expect(budget.usedBytes).toBe(20);
        expect(materializeBatchRows(output.value)).toEqual([
          { x: Uint8Array.of(1, 2) },
          { x: Uint8Array.of(3, 4) },
        ]);
      } finally {
        output.release();
      }
    }
  } finally {
    for (const input of inputs) input.release();
  }
  expect(budget.usedBytes).toBe(0);
});
