import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { sharedGatherPlan } from "./gather-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { compactGatherPlan } from "./packed-gather.js";
import { gatherOwnedBatch } from "./vector-sort.js";

const descriptors = (columns: Record<string, number>) =>
  2 * c.object +
  2 * c.property +
  Object.entries(columns).reduce(
    (sum, [name, properties]) =>
      sum +
      c.property +
      c.string +
      name.length * c.stringCodeUnit +
      c.object +
      properties * c.property,
    0,
  );
const numericDescriptors = descriptors({ value: 2 });
const packedDescriptors = descriptors({ raw: 4 });
const sharedRawDescriptors = descriptors({ raw: 3 });

it("admits a complete compact output across scalar and packed columns", () => {
  const budget = new MemoryBudget(4096);
  const scope = new MemoryScope(budget);
  const data = scope.allocate(1024, "payload", () => new Uint8Array(1024)).value;
  data.set([10, 20]);
  const input = scope.retainAllocations({
    rowCount: 2,
    columns: {
      payload: { type: "binary-view" as const, data, fixedLength: 1, length: 2 },
      number: { type: "i32" as const, values: Int32Array.of(3, 4) },
    },
  });
  scope.dispose();
  const output = gatherOwnedBatch(input, [1, 0]);
  expect(output.bytes).toBe(26 + 3 * c.view + descriptors({ payload: 4, number: 2 })); // 16 ranges + 2 payload + 8 scalar values.
  expect(budget.peakBytes).toBe(1050 + 3 * c.view + descriptors({ payload: 4, number: 2 }));
  input.release();
  expect(budget.usedBytes).toBe(26 + 3 * c.view + descriptors({ payload: 4, number: 2 }));
  expect(materializeBatchRows(output.value)).toEqual([
    { payload: Uint8Array.of(20), number: 4 },
    { payload: Uint8Array.of(10), number: 3 },
  ]);
  output.release();
  expect(budget.usedBytes).toBe(0);
});

it("transfers shared nested backing between sibling jobs without double charging the pool", () => {
  const root = new MemoryBudget(100 + 256 + c.view + descriptors({ list: 3 }));
  const producer = root.child();
  const consumer = root.child();
  const stage = new MemoryScope(producer);
  const batch = batchFromColumns({ list: [[1, 2], []] }, stage);
  const input = stage.retainAllocations(batch);
  stage.dispose();
  expect(root.usedBytes).toBe(28 + 256);
  const output = gatherOwnedBatch(input, [0, 1, 0], consumer);
  expect(root.usedBytes).toBe(40 + 256 + c.view + descriptors({ list: 3 }));
  expect(producer.usedBytes).toBe(28 + 256);
  expect(consumer.usedBytes).toBe(40 + 256 + c.view + descriptors({ list: 3 }));
  input.release();
  expect(producer.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(40 + 256 + c.view + descriptors({ list: 3 }));
  expect(materializeBatchRows(output.value)).toEqual([
    { list: [1, 2] },
    { list: [] },
    { list: [1, 2] },
  ]);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  40 + c.view + numericDescriptors,
  39 + c.view + numericDescriptors,
])("admits numeric gather overlap at exact boundary %i", (limit) => {
  const root = new MemoryBudget(limit);
  const stage = new MemoryScope(root);
  const input = stage.retainAllocations(batchFromColumns({ value: [1, 2, 3] }, stage));
  stage.dispose();
  if (limit === 39 + c.view + numericDescriptors) {
    expect(() => gatherOwnedBatch(input, [2, 0])).toThrow();
    expect(root.usedBytes).toBe(24);
  } else {
    const output = gatherOwnedBatch(input, [2, 0]);
    expect(root.peakBytes).toBe(40 + c.view + numericDescriptors);
    input.release();
    expect(root.usedBytes).toBe(16 + c.view + numericDescriptors);
    expect(materializeBatchRows(output.value)).toEqual([{ value: 3 }, { value: 1 }]);
    output.release();
  }
  input.release();
  expect(root.usedBytes).toBe(0);
});

it("rejects invalid and cross-pool gathers without leaking source pins", () => {
  const root = new MemoryBudget(100);
  const scope = new MemoryScope(root);
  const input = scope.retainAllocations(batchFromColumns({ value: [1] }, scope));
  scope.dispose();
  expect(() => gatherOwnedBatch(input, [-1])).toThrow();
  expect(root.usedBytes).toBe(8);
  const other = new MemoryBudget(100);
  expect(() => gatherOwnedBatch(input, [0], other)).toThrow();
  expect(other.usedBytes).toBe(0);
  input.release();
  expect(() => gatherOwnedBatch(input, [0])).toThrow();
  expect(root.usedBytes).toBe(0);
});

it("keeps empty dictionary backing charged while its reference survives", () => {
  const root = new MemoryBudget(100 + 256 + c.view + descriptors({ list: 3 }));
  const scope = new MemoryScope(root);
  const input = scope.retainAllocations(batchFromColumns({ list: [[1, 2]] }, scope));
  scope.dispose();
  const bytes = root.usedBytes;
  const output = gatherOwnedBatch(input, []);
  input.release();
  expect(output.value.rowCount).toBe(0);
  expect(root.usedBytes).toBe(bytes + c.view + descriptors({ list: 3 }));
  output.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  1033 + 2 * c.view + packedDescriptors,
  1032 + 2 * c.view + packedDescriptors,
  1027 + c.view + sharedRawDescriptors,
])("measures packed compaction against available headroom (%i)", (limit) => {
  const root = new MemoryBudget(limit);
  const scope = new MemoryScope(root);
  const data = scope.allocate(1024, "fixture backing", () => new Uint8Array(1024)).value;
  data[17] = 255;
  const input = scope.retainAllocations({
    rowCount: 2,
    columns: {
      raw: { type: "binary-view" as const, data, fixedLength: 1, rowOffset: 16, length: 2 },
    },
  });
  scope.dispose();
  if (limit === 1027 + c.view + sharedRawDescriptors) {
    expect(() => gatherOwnedBatch(input, [1])).toThrow();
    expect(root.usedBytes).toBe(1024);
  } else {
    const output = gatherOwnedBatch(input, [1]);
    input.release();
    expect(output.value.columns.raw?.type).toBe(
      limit === 1033 + 2 * c.view + packedDescriptors ? "binary-view" : "dict",
    );
    expect(root.usedBytes).toBe(
      limit === 1033 + 2 * c.view + packedDescriptors
        ? 9 + 2 * c.view + packedDescriptors
        : 1028 + c.view + sharedRawDescriptors,
    );
    expect(materializeBatchRows(output.value)).toEqual([{ raw: Uint8Array.of(255) }]);
    output.release();
  }
  input.release();
  expect(root.usedBytes).toBe(0);
});

it("compacts exact UTF8 bytes with nulls, empty values, repeats, and a row offset", () => {
  const root = new MemoryBudget(4096);
  const scope = new MemoryScope(root);
  const data = scope.allocate(1024, "fixture backing", () => new Uint8Array(1024)).value;
  data.set(new TextEncoder().encode("猫"), 12);
  const ranges = scope.allocate(32, "ranges", () =>
    Uint32Array.of(0, 0, 12, 3, 15, 0, 15, 0),
  ).value;
  const valid = scope.allocate(3, "valid", () => Uint8Array.of(1, 0, 1)).value;
  const input = scope.retainAllocations({
    rowCount: 3,
    columns: {
      text: { type: "utf8-view" as const, data, ranges, valid, rowOffset: 1, length: 3 },
    },
  });
  scope.dispose();
  const output = gatherOwnedBatch(input, [2, 0, 1, 0]);
  input.release();
  expect(root.usedBytes).toBe(32 + 6 + 4 + 3 * c.view + descriptors({ text: 5 }));
  data.fill(0);
  expect(materializeBatchRows(output.value)).toEqual([
    { text: "" },
    { text: "猫" },
    { text: null },
    { text: "猫" },
  ]);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("compacts nested dictionaries while preserving validity at every level", () => {
  const root = new MemoryBudget(4096);
  const scope = new MemoryScope(root);
  const data = scope.allocate(1024, "payload", () => new Uint8Array(1024)).value;
  data.set(new TextEncoder().encode("éx"));
  const ranges = scope.allocate(24, "ranges", () => Uint32Array.of(0, 2, 2, 1, 3, 0)).value;
  const leafValid = scope.allocate(3, "leaf validity", () => Uint8Array.of(1, 0, 1)).value;
  const innerIndices = scope.allocate(16, "inner indices", () => Uint32Array.of(2, 0, 1, 0)).value;
  const innerValid = scope.allocate(4, "inner validity", () => Uint8Array.of(1, 1, 1, 0)).value;
  const outerIndices = scope.allocate(20, "outer indices", () =>
    Uint32Array.of(0, 1, 2, 3, 1),
  ).value;
  const outerValid = scope.allocate(5, "outer validity", () => Uint8Array.of(1, 1, 1, 1, 0)).value;
  const input = scope.retainAllocations({
    rowCount: 5,
    columns: {
      text: {
        type: "dict" as const,
        indices: outerIndices,
        valid: outerValid,
        dictionary: {
          type: "dict" as const,
          indices: innerIndices,
          valid: innerValid,
          dictionary: {
            type: "utf8-view" as const,
            data,
            ranges,
            valid: leafValid,
            length: 3,
          },
        },
      },
    },
  });
  scope.dispose();
  const output = gatherOwnedBatch(input, [1, 0, 2, 3, 4, 1]);
  input.release();
  expect(output.value.columns.text?.type).toBe("utf8-view");
  expect(root.usedBytes).toBe(48 + 4 + 6 + 3 * c.view + descriptors({ text: 5 }));
  expect(materializeBatchRows(output.value)).toEqual([
    { text: "é" },
    { text: "" },
    { text: null },
    { text: null },
    { text: null },
    { text: "é" },
  ]);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("detaches dictionary numeric leaves without losing bigint precision or timestamp metadata", () => {
  const leaves: import("./batch.js").Vector[] = [
    { type: "f32", values: Float32Array.of(1.25, 2.5) },
    { type: "i64", values: BigInt64Array.of(-9223372036854775808n, 9223372036854775807n) },
    { type: "u64", values: BigUint64Array.of(0n, 18446744073709551615n) },
    { type: "bool", values: Uint8Array.of(1, 0) },
    {
      type: "timestamp",
      values: BigInt64Array.of(-123456789n, 987654321n),
      unit: "micros",
      isAdjustedToUTC: false,
    },
  ];
  for (const leaf of leaves) {
    const root = new MemoryBudget(4096);
    const scope = new MemoryScope(root);
    scope.allocate(1024, "retained decoder backing", () => new Uint8Array(1024));
    const indices = scope.allocate(16, "indices", () => Uint32Array.of(1, 0, 1, 0)).value;
    const valid = scope.allocate(4, "validity", () => Uint8Array.of(1, 0, 1, 1)).value;
    const batch = {
      rowCount: 4,
      columns: { value: { type: "dict" as const, indices, valid, dictionary: leaf } },
    };
    const input = scope.retainAllocations(batch);
    const expected = materializeBatchRows(batch);
    scope.dispose();
    const output = gatherOwnedBatch(input, [2, 1, 3]);
    input.release();
    expect(output.value.columns.value?.type).toBe(leaf.type);
    expect(materializeBatchRows(output.value)).toEqual([expected[2], expected[1], expected[3]]);
    if (!("values" in leaf) || !ArrayBuffer.isView(leaf.values))
      throw new Error("Expected typed leaf");
    expect(root.usedBytes).toBe(
      3 * leaf.values.BYTES_PER_ELEMENT +
        3 +
        2 * c.view +
        descriptors({ value: leaf.type === "timestamp" ? 5 : 3 }),
    );
    output.release();
    expect(root.usedBytes).toBe(0);
  }
});

it.each([
  0, -1,
])("chooses numeric dictionary copy or view from complete overlap, delta=%i", (delta) => {
  const root = new MemoryBudget(65536);
  const scope = new MemoryScope(root);
  const values = scope.allocate(16 + c.view, "dictionary values and view", () =>
    BigUint64Array.of(0n, 18446744073709551615n),
  ).value;
  const indices = scope.allocate(128 + c.view, "dictionary indices and view", () =>
    Uint32Array.from({ length: 32 }, (_, i) => i % 2),
  ).value;
  const batch = {
    rowCount: 32,
    columns: {
      value: { type: "dict" as const, indices, dictionary: { type: "u64" as const, values } },
    },
  };
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const positions = Array.from({ length: 32 }, (_, i) => i);
  const compact = compactGatherPlan(batch, positions);
  if (compact === undefined) throw new Error("missing compact plan");
  const compactBytes = compact.typedBytes + compact.viewBytes + compact.descriptorBytes;
  const shared = sharedGatherPlan(batch, positions);
  const sharedBytes = shared.typedBytes + shared.viewBytes + shared.descriptorBytes;
  expect(sharedBytes).toBeLessThan(compactBytes);
  expect(compactBytes).toBeLessThan(input.bytes + sharedBytes);
  const output = gatherOwnedBatch(input, positions, root.child(input.bytes + compactBytes + delta));
  const inputBytes = input.bytes;
  input.release();
  expect(output.value.columns.value?.type).toBe(delta === 0 ? "u64" : "dict");
  expect(root.usedBytes).toBe(delta === 0 ? compactBytes : inputBytes + sharedBytes);
  expect(materializeBatchRows(output.value)).toEqual(
    positions.map((i) => ({ value: i % 2 ? 18446744073709551615n : 0n })),
  );
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("shares admitted indices across text and binary gathers without copying JS value arrays", () => {
  // Borrowed source values isolate the new output allocation contract.
  const budget = new MemoryBudget(12 + c.view + descriptors({ text: 3, binary: 3 }));
  const scope = new MemoryScope(budget);
  const text = { type: "utf8" as const, values: ["a", "b"], valid: Uint8Array.of(1, 0) };
  const binary = { type: "binary" as const, values: [Uint8Array.of(1), Uint8Array.of(2)] };
  const input = scope.retainAllocations({ rowCount: 2, columns: { text, binary } });
  scope.dispose();
  const output = gatherOwnedBatch(input, [1, 0, 1]);
  const gatheredText = output.value.columns.text;
  const gatheredBinary = output.value.columns.binary;
  if (gatheredText?.type !== "dict" || gatheredBinary?.type !== "dict")
    throw new Error("Expected dictionary-backed gather");
  expect(gatheredText.indices).toBe(gatheredBinary.indices);
  expect(gatheredText.dictionary).toBe(text);
  expect(gatheredBinary.dictionary).toBe(binary);
  expect(gatheredText.valid).toBeUndefined();
  expect(output.bytes).toBe(12 + c.view + descriptors({ text: 3, binary: 3 }));
  input.release();
  expect(materializeBatchRows(output.value)).toEqual([
    { text: null, binary: Uint8Array.of(2) },
    { text: "a", binary: Uint8Array.of(1) },
    { text: null, binary: Uint8Array.of(2) },
  ]);
  output.release();
  expect(budget.usedBytes).toBe(0);
});
