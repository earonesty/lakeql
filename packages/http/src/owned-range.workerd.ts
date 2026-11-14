import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { httpStore } from "./index.js";

it.each([200, 206])("collects exact owned HTTP windows in workerd (status=%i)", async (status) => {
  for (const cap of [3, 4]) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    let requests = 0;
    const store = httpStore({
      baseUrl: "https://fixture.invalid/",
      fetch: async () => {
        requests++;
        const chunks = status === 200 ? [[0, 1, 2], [3], [4, 5, 6, 7]] : [[2], [3, 4], [5]];
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              for (const chunk of chunks) controller.enqueue(Uint8Array.from(chunk));
              controller.close();
            },
          }),
          { status },
        );
      },
    });
    const pending = store.getRangeOwned?.("file", { offset: 2, length: 4 }, scope);
    if (cap === 3) {
      await expect(pending).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      expect(requests).toBe(0);
    } else {
      const owner = await pending;
      expect(owner?.value).toEqual(Uint8Array.of(2, 3, 4, 5));
      expect(owner?.value.buffer.byteLength).toBe(4);
      expect(budget.peakBytes).toBe(4);
      owner?.release();
    }
    expect(budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("rejects truncated owned HTTP windows without retained storage in workerd", async () => {
  const scope = new MemoryScope(new MemoryBudget(4));
  const store = httpStore({
    baseUrl: "https://fixture.invalid/",
    fetch: async () => new Response(Uint8Array.of(1, 2), { status: 206 }),
  });
  await expect(store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope)).rejects.toThrow(
    /range/,
  );
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
