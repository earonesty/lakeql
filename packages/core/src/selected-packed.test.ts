import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { planSelectedConcat } from "./selected-concat.js";
import { selectVector } from "./vector-selection.js";

it.each([1, 100])("plans selected nested packed values with %s repetitions", (repetitions) => {
  const inputs: Vector[] = [0, 1].map((part) => ({
    type: "dict",
    indices: new Uint32Array(repetitions),
    dictionary: {
      type: "list",
      offsets: Int32Array.of(0, 3),
      child:
        part === 0
          ? { type: "utf8", values: ["猫", "", "hidden"], valid: Uint8Array.of(1, 1, 0) }
          : {
              type: "utf8-view",
              data: Uint8Array.of(0, 0x61, 0xff),
              ranges: Uint32Array.of(0, 1, 1, 1, 2, 1),
              rowOffset: 1,
              length: 2,
            },
    },
  }));
  // Second list has two children; sliced packed offsets must remain intact.
  const second = inputs[1];
  if (second?.type === "dict" && second.dictionary.type === "list")
    second.dictionary.offsets[1] = 2;
  const plan = planSelectedConcat(inputs.map(selectVector));
  if (plan === undefined) throw new Error("Missing nested packed plan");
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  scope.admit(plan.bytes, "nested packed output");
  const output = plan.execute(scope);
  for (let row = 0; row < repetitions * 2; row++)
    expect(vectorValue(output, row)).toEqual(
      vectorValue(inputs[Math.floor(row / repetitions)] as Vector, row % repetitions),
    );
  if (output.type !== "list") throw new Error("Expected list");
  expect(output.child.type).toBe(repetitions === 100 ? "dict" : "utf8-view");
  expect(budget.peakBytes).toBe(plan.bytes);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("declines lossy selected strings and rejects invalid packed ranges before allocation", () => {
  expect(planSelectedConcat([selectVector({ type: "utf8", values: ["\ud800"] })])).toBeUndefined();
  expect(() =>
    planSelectedConcat([
      selectVector({
        type: "binary-view",
        data: Uint8Array.of(1),
        ranges: Uint32Array.of(1, 2),
        length: 1,
      }),
    ]),
  ).toThrow(/range/);
});
