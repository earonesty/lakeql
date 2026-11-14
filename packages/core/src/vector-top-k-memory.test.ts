import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, batchFromColumns, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { timestampFromEpoch } from "./timestamp.js";
import { vectorTopKBatch, vectorTopKOwnedBatch } from "./vector-sort.js";

it.each([
  "bigint",
  "timestamp",
  "timestamp96",
  "packed text",
] as const)("admits both %s comparison scalars and releases them between comparisons", (kind) => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  let batch: Batch;
  let scalarBytes: number;
  if (kind === "packed text") {
    const data = scope.allocate(512, "packed source", () => new Uint8Array(512)).value;
    for (const [index, character] of [122, 98, 109, 97].entries())
      data.fill(character, index * 128, (index + 1) * 128);
    batch = {
      rowCount: 4,
      columns: { key: { type: "utf8-view", data, fixedLength: 128, length: 4 } },
    };
    scalarBytes = c.view + c.string + 128 * c.stringCodeUnit;
  } else {
    batch = batchFromColumns(
      {
        key: [9n, 2n, 7n, 1n].map((value) =>
          kind === "bigint"
            ? value
            : timestampFromEpoch(kind === "timestamp96" ? (1n << 70n) + value : value, "nanos"),
        ),
      },
      scope,
    );
    scalarBytes =
      kind === "bigint"
        ? c.bigint
        : c.timestamp + c.bigint + (kind === "timestamp96" ? c.object : 0);
  }
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const order = [{ column: "key" }];
  const expected = materializeBatchRows(vectorTopKBatch(batch, order, { limit: 2 }));
  const required = input.bytes + c.view + 8 + 2 * scalarBytes;
  const tight = budget.child(required - 1);
  const tightPin = input.retainIn(tight);
  expect(() => vectorTopKOwnedBatch(tightPin, order, { limit: 2 })).toThrow(/budget/i);
  expect(tight.usedBytes).toBe(input.bytes);
  tightPin.release();
  expect(tight.usedBytes).toBe(0);
  const exact = budget.child(required);
  const exactPin = input.retainIn(exact);
  const result = vectorTopKOwnedBatch(exactPin, order, { limit: 2 });
  expect(materializeBatchRows(result.value)).toEqual(expected);
  expect(exact.peakBytes).toBe(required);
  result.release();
  exactPin.release();
  expect(exact.usedBytes).toBe(0);
  input.release();
  expect(budget.usedBytes).toBe(0);
});

it("rejects packed text scalar allocation before decoding when the first read cannot fit", () => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const data = scope.allocate(512, "packed source", () => new Uint8Array(512)).value;
  const batch: Batch = {
    rowCount: 4,
    columns: { key: { type: "utf8-view", data, fixedLength: 128, length: 4 } },
  };
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const child = budget.child(input.bytes + c.view + 8 + c.view + c.string + 256 - 1);
  const pin = input.retainIn(child);
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(() => vectorTopKOwnedBatch(pin, [{ column: "key" }], { limit: 2 })).toThrow(/budget/i);
    expect(decode).not.toHaveBeenCalled();
  } finally {
    decode.mockRestore();
    pin.release();
    input.release();
  }
  expect(budget.usedBytes).toBe(0);
});

it("matches reference top-k across selection, nulls, ties, offsets, and limits", () => {
  const budget = new MemoryBudget(1 << 20);
  const scope = new MemoryScope(budget);
  const batch = batchFromColumns(
    {
      id: Array.from({ length: 129 }, (_, i) => i),
      score: Array.from({ length: 129 }, (_, i) => (i % 7 === 0 ? null : (i * 17) % 23)),
    },
    scope,
  );
  const input = scope.retainAllocations(batch);
  scope.dispose();
  for (const direction of ["asc", "desc"] as const) {
    for (const nulls of ["first", "last"] as const) {
      for (const limit of [0, 1, 7, 128, 200]) {
        for (const offset of [0, 3, 130]) {
          for (const selection of [
            undefined,
            Uint8Array.from({ length: 129 }, (_, i) => (i % 3 === 0 ? 1 : 0)),
          ]) {
            const order = [{ column: "score", direction, nulls }];
            const options = { limit, offset };
            const expected = materializeBatchRows(
              vectorTopKBatch(batch, order, options, selection),
            );
            const output = vectorTopKOwnedBatch(input, order, options, selection);
            expect(materializeBatchRows(output.value)).toEqual(expected);
            output.release();
            expect(budget.usedBytes).toBe(input.bytes);
          }
        }
      }
    }
  }
  input.release();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  0, 1, 10,
])("admits heap, offset view and output overlap at exact capacity (offset=%s)", (offset) => {
  function run(maxBytes: number) {
    const budget = new MemoryBudget(maxBytes);
    const scope = new MemoryScope(budget);
    const batch = batchFromColumns({ id: [9, 2, 7, 1] }, scope);
    const input = scope.retainAllocations(batch);
    scope.dispose();
    try {
      const output = vectorTopKOwnedBatch(input, [{ column: "id" }], { limit: 2, offset });
      input.release();
      expect(materializeBatchRows(output.value)).toEqual(
        [1, 2, 7, 9].slice(offset, offset + 2).map((id) => ({ id })),
      );
      output.release();
      return budget.peakBytes;
    } finally {
      input.release();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow();
});

it("sizes the admitted heap from actual selected rows rather than an oversized limit", () => {
  const root = new MemoryBudget(65536);
  const scope = new MemoryScope(root);
  const batch = batchFromColumns({ id: Array.from({ length: 128 }, (_, i) => i) }, scope);
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const selected = new Uint8Array(128);
  selected[73] = 1;
  // Candidate view, one index and one Float64 output, plus pinned input.
  const child = root.child(input.bytes + c.view + 4 + 8);
  const pin = input.retainIn(child);
  const output = vectorTopKOwnedBatch(pin, [{ column: "id" }], { limit: 1000000 }, selected);
  expect(materializeBatchRows(output.value)).toEqual([{ id: 73 }]);
  expect(child.peakBytes).toBe(input.bytes + c.view + 12);
  output.release();
  pin.release();
  expect(child.usedBytes).toBe(0);
  input.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  "ascending",
  "descending",
  "equal",
])("preserves reference results for %s selected input", (shape) => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const batch = batchFromColumns(
    {
      id: Array.from({ length: 64 }, (_, i) => i),
      score: Array.from({ length: 64 }, (_, i) =>
        shape === "ascending" ? i : shape === "descending" ? -i : 1,
      ),
    },
    scope,
  );
  const input = scope.retainAllocations(batch);
  scope.dispose();
  for (const direction of ["asc", "desc"] as const) {
    for (const selection of [
      undefined,
      Uint8Array.from({ length: 64 }, (_, i) => (i % 3 === 0 ? 1 : 0)),
      new Uint8Array(64),
    ]) {
      for (const options of [
        { limit: 7, offset: 3 },
        { limit: 100 },
        { limit: 0 },
        { limit: 2, offset: 70 },
      ]) {
        const order = [{ column: "score", direction }];
        const output = vectorTopKOwnedBatch(input, order, options, selection);
        expect(materializeBatchRows(output.value)).toEqual(
          materializeBatchRows(vectorTopKBatch(batch, order, options, selection)),
        );
        output.release();
      }
    }
  }
  input.release();
  expect(budget.usedBytes).toBe(0);
});
