import { expect, it, vi } from "vitest";
import { closeAsyncIterators } from "./close-iterators.js";

it("closes all iterators and waits for pending cleanup after rejection", async () => {
  const pending = Promise.withResolvers<IteratorResult<unknown>>();
  const error = new Error("cleanup failed");
  const last = vi.fn(async () => ({ done: true as const, value: undefined }));
  let completed = false;
  const result = closeAsyncIterators([
    {
      return: () => {
        throw error;
      },
    },
    { return: () => pending.promise },
    { return: last },
    {},
  ]).catch((cause: unknown) => {
    completed = true;
    return cause;
  });
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  expect(last).toHaveBeenCalledTimes(1);
  expect(completed).toBe(false);
  pending.resolve({ done: true, value: undefined });
  expect(await result).toBe(error);
});

it("accepts empty and optional cleanup and propagates undefined rejection", async () => {
  await expect(closeAsyncIterators([])).resolves.toBeUndefined();
  await expect(closeAsyncIterators([{}])).resolves.toBeUndefined();
  await expect(
    closeAsyncIterators([
      {
        return: async () => {
          throw undefined;
        },
      },
    ]),
  ).rejects.toBeUndefined();
});
