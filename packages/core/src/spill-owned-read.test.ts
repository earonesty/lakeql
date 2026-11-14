import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemorySpillAdapter } from "./runtime.js";

it("admits spill read copies at exact capacity and preserves stored bytes on denial", async () => {
  const spill = new MemorySpillAdapter();
  const bytes = new Uint8Array([1, 2, 3]);
  const ref = await spill.write("entry", bytes);
  const required = c.view + bytes.length;
  for (const cap of [required - 1, required]) {
    const pool = new MemoryBudget(cap + 17);
    const sibling = pool.reserve(17, "sibling");
    const budget = pool.child(cap);
    if (cap < required) await expect(spill.readOwned(ref, budget)).rejects.toThrow(/budget/i);
    else {
      const owner = await spill.readOwned(ref, budget);
      expect(budget.usedBytes).toBe(required);
      owner.value[0] = 9;
      expect(await spill.read(ref)).toEqual(bytes);
      owner.release();
    }
    expect(pool.usedBytes).toBe(17);
    sibling.release();
    expect(await spill.usage()).toEqual({ entries: 1, byteSize: 3 });
  }
});

it("does not reserve for missing spill data", async () => {
  const pool = new MemoryBudget(65536);
  await expect(new MemorySpillAdapter().readOwned("missing", pool)).rejects.toMatchObject({
    code: "LAKEQL_OBJECT_NOT_FOUND",
  });
  expect(pool.usedBytes).toBe(0);
});
