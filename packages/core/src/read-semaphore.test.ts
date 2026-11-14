import { describe, expect, it } from "vitest";
import { ReadSemaphore } from "./read-semaphore.js";

describe("read-slot handoff", () => {
  it("keeps the slot reserved while a queued reader resumes", async () => {
    const semaphore = new ReadSemaphore(1);
    const first = await semaphore.acquire();
    const queued = semaphore.acquire();
    first();
    let bypassed = false;
    const newcomer = semaphore.acquire().then((release) => {
      bypassed = true;
      return release;
    });
    const second = await queued;
    await Promise.resolve();
    expect(bypassed).toBe(false);
    first(); // Duplicate cleanup cannot release the second reader's slot.
    await Promise.resolve();
    expect(bypassed).toBe(false);
    second();
    const last = await newcomer;
    last();
  });

  it("passes a transferred slot onward if its recipient is aborted before resuming", async () => {
    const semaphore = new ReadSemaphore(1);
    const first = await semaphore.acquire();
    const controller = new AbortController();
    const queued = semaphore.acquire(controller.signal).catch((error) => error);
    const next = semaphore.acquire();
    first();
    controller.abort("handoff cancellation");
    expect(await queued).toMatchObject({
      code: "LAKEQL_ABORTED",
      details: { reason: "handoff cancellation" },
    });
    const release = await next;
    release();
  });

  it("unlinks cancelled queue entries and preserves FIFO order", async () => {
    const semaphore = new ReadSemaphore(1);
    const first = await semaphore.acquire();
    const order: number[] = [];
    const controllers = Array.from({ length: 500 }, () => new AbortController());
    const waiting = controllers.map((controller, i) =>
      semaphore.acquire(controller.signal).then(
        (release) => {
          order.push(i);
          release();
          return "read";
        },
        (error) => {
          expect(error.code).toBe("LAKEQL_ABORTED");
          return "cancelled";
        },
      ),
    );
    for (let i = 0; i < controllers.length; i += 2) controllers[i]?.abort();
    first();
    const results = await Promise.all(waiting);
    expect(results.filter((result) => result === "read")).toHaveLength(250);
    expect(order).toEqual(Array.from({ length: 250 }, (_, i) => i * 2 + 1));
    const release = await semaphore.acquire();
    release();
    expect(() => new ReadSemaphore(0)).toThrow();
    const aborted = new AbortController();
    aborted.abort(new Error("cancelled"));
    await expect(semaphore.acquire(aborted.signal)).rejects.toMatchObject({
      code: "LAKEQL_ABORTED",
      details: { reason: "cancelled" },
    });
  });
});
