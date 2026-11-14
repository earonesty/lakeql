import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatPackedVectors, packedVectorConcatPlan } from "./packed-gather.js";

it("plans selected payload, nested dictionary nulls and existing validity exactly", () => {
  const leaf: Vector = {
    type: "utf8",
    values: ["猫", "unused long payload", "x"],
    valid: Uint8Array.of(1, 1, 0),
  };
  const inner: Vector = { type: "dict", indices: Uint32Array.of(0, 2), dictionary: leaf };
  const input: Vector = { type: "dict", indices: Uint32Array.of(0, 1, 0), dictionary: inner };
  for (const outer of [false, true]) {
    const plan = packedVectorConcatPlan([input], outer);
    expect(plan?.totalBytes).toBe(33);
    expect(plan?.allocationBytes).toBe(outer ? 30 : 33);
    if (plan === undefined) throw new Error("missing plan");
    const root = new MemoryBudget(plan.totalBytes);
    const scope = new MemoryScope(root);
    const validity = outer
      ? scope.allocate(3, "outer validity", () => new Uint8Array(3)).value
      : undefined;
    scope.admit(plan.allocationBytes, "packed expansion");
    const output = concatPackedVectors([input], validity, scope);
    if (output === undefined) throw new Error("missing output");
    expect([0, 1, 2].map((i) => vectorValue(output, i))).toEqual(["猫", null, "猫"]);
    expect(root.usedBytes).toBe(plan.totalBytes);
    expect(scope.availableAllocationBytes).toBe(0);
    scope.dispose();
    expect(root.usedBytes).toBe(0);
    const tooSmall = new MemoryScope(new MemoryBudget(plan.allocationBytes - 1));
    expect(() => tooSmall.admit(plan.allocationBytes, "packed expansion")).toThrow();
    expect(tooSmall.budget.usedBytes).toBe(0);
    tooSmall.dispose();
  }
});

it("reports unsupported mixed kinds and preserves unpaired surrogate strings", () => {
  expect(packedVectorConcatPlan([{ type: "utf8", values: ["\ud800"] }], false)).toBeUndefined();
  expect(
    packedVectorConcatPlan(
      [
        { type: "utf8", values: ["a"] },
        { type: "binary", values: [Uint8Array.of(1)] },
      ],
      false,
    ),
  ).toBeUndefined();
  expect(packedVectorConcatPlan([{ type: "utf8", values: [] }], false)?.totalBytes).toBe(0);
});
