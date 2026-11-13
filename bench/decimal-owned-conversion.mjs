import assert from "node:assert/strict";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { convertDecimalValues } from "../packages/parquet/dist/decimal.js";

const element = {
  name: "amount",
  type: "INT64",
  converted_type: "DECIMAL",
  precision: 15,
  scale: 4,
};
for (const count of [10000, 100000]) {
  const input = BigInt64Array.from(
    { length: count },
    (_, index) => BigInt(index - count / 2) * 17n,
  );
  const expected = Float64Array.from(input, (value) => Number(`${value}e-4`));
  for (const mode of ["array-then-vector", "owned-typed"]) {
    const times = [];
    for (let repeat = 0; repeat < 10; repeat++) {
      const scope = new MemoryScope(new MemoryBudget(count * 8));
      try {
        const start = performance.now();
        const result =
          mode === "owned-typed"
            ? convertDecimalValues(input, element, "amount", scope)
            : Float64Array.from(convertDecimalValues(input, element, "amount"));
        const elapsed = performance.now() - start;
        assert.deepEqual(result, expected);
        if (repeat >= 3) times.push(elapsed);
      } finally {
        scope.dispose();
      }
      assert.equal(scope.budget.usedBytes, 0);
    }
    times.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        count,
        mode,
        medianMs: times[3],
        samples: 7,
        outputBytes: count * 8,
        scope:
          "Local dense integer-decimal conversion; borrowed input; baseline intermediate JS array untracked; not cloud or whole-heap evidence",
      }),
    );
  }
}
