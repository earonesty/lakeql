import { expect, it } from "vitest";
import { timestampFromEpoch } from "./timestamp.js";

it("formats wide and negative fractional timestamps in workerd", () => {
  expect(timestampFromEpoch(-1n, "nanos").toString()).toBe("1969-12-31T23:59:59.999999999Z");
  const cycle = 146097n * 86400000n;
  const leapDay = 951782400000n;
  expect(timestampFromEpoch(leapDay + cycle * 1000n, "millis").toString()).toBe(
    "+402000-02-29T00:00:00.000Z",
  );
  expect(timestampFromEpoch(leapDay - cycle * 1000n, "millis").toString()).toBe(
    "-398000-02-29T00:00:00.000Z",
  );
  for (const epoch of [-(1n << 95n), (1n << 95n) - 1n]) {
    const value = timestampFromEpoch(epoch, "millis");
    expect(value.toString()).toMatch(/^[+-]\d{6,}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/u);
    expect(JSON.stringify(value)).toBe(JSON.stringify(value.toString()));
  }
});

it("promotes wide timestamp batches within exact Worker allocation bounds", async () => {
  const { batchFromColumns, vectorValue } = await import("./batch.js");
  const { MemoryBudget } = await import("./memory-budget.js");
  const { MemoryScope } = await import("./memory-scope.js");
  const { concatOwnedBatches } = await import("./vector-sort.js");
  const wide = timestampFromEpoch(1n << 64n, "nanos");
  const narrow = timestampFromEpoch(1n, "nanos");
  const pool = new MemoryBudget(44);
  const scope = new MemoryScope(pool);
  const left = scope.retainAllocations(batchFromColumns({ at: [wide] }, scope));
  const right = scope.retainAllocations(batchFromColumns({ at: [narrow] }, scope));
  const output = concatOwnedBatches([left, right]);
  expect(pool.peakBytes).toBe(44);
  const at = output.value.columns.at;
  if (at === undefined) throw new Error("Missing output");
  expect(at.type).toBe("timestamp96");
  expect(vectorValue(at, 0)).toEqual(wide);
  expect(vectorValue(at, 1)).toEqual(narrow);
  output.release();
  left.release();
  right.release();
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(11));
  expect(() => batchFromColumns({ at: [wide] }, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});
