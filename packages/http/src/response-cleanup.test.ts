import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { HttpObjectStore } from "./index.js";

it.each([
  "get",
  "head",
  "range",
  "ownedHead",
  "ownedRange",
  "put",
  "delete",
] as const)("%s discards error bodies even when cancellation rejects", async (operation) => {
  for (const status of [404, 503]) {
    for (const rejectCancel of [false, true]) {
      let canceled = 0;
      const store = new HttpObjectStore({
        baseUrl: "https://example.test",
        fetch: async () =>
          new Response(
            new ReadableStream({
              cancel() {
                canceled++;
                if (rejectCancel) throw new Error("cancel failed");
              },
            }),
            { status },
          ),
      });
      const scope = new MemoryScope(new MemoryBudget(4096));
      const result =
        operation === "get"
          ? store.get("file")
          : operation === "head"
            ? store.head("file")
            : operation === "range"
              ? store.getRange("file", { offset: 0, length: 2 })
              : operation === "ownedHead"
                ? store.headOwned("file", scope)
                : operation === "ownedRange"
                  ? store.getRangeOwned("file", { offset: 0, length: 2 }, scope)
                  : operation === "put"
                    ? store.put("file", new Uint8Array(1))
                    : store.delete("file");
      if (status === 404 && ["get", "head", "ownedHead"].includes(operation)) {
        await expect(result).resolves.toBeNull();
      } else {
        await expect(result).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });
      }
      expect(canceled).toBe(1);
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  }
});

it.each(["put", "delete"] as const)("%s discards successful response bodies", async (method) => {
  let canceled = 0;
  const store = new HttpObjectStore({
    baseUrl: "https://example.test",
    fetch: async () =>
      new Response(
        new ReadableStream({
          cancel() {
            canceled++;
          },
        }),
      ),
  });
  if (method === "put") await store.put("file", new Uint8Array(1));
  else await store.delete("file");
  expect(canceled).toBe(1);
});
