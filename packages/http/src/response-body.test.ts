import { MemoryBudget, MemoryScope, withObjectStoreReadControls } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { MemoryStageScheduler } from "../../core/src/memory-stage-scheduler.js";
import { httpStore } from "./index.js";
import { boundedResponseBody, ownedResponseBody } from "./response-body.js";

function streamed(parts: number[], headers: HeadersInit = {}) {
  let index = 0;
  let cancelled = false;
  const response = new Response(
    new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          const size = parts[index++];
          if (size === undefined) controller.close();
          else controller.enqueue(new Uint8Array(size).fill(index));
        },
        cancel() {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    ),
    { headers },
  );
  return { response, cancelled: () => cancelled };
}

describe("bounded HTTP payload consumption", () => {
  it("handles many tiny chunks without retaining an array of chunk objects", async () => {
    const input = streamed(Array.from({ length: 10_000 }, () => 1));
    const bytes = await boundedResponseBody(input.response, 10_000);
    expect(bytes.byteLength).toBe(10_000);
    expect(bytes[0]).toBe(1);
    expect(bytes[9999]).toBe(10_000 % 256);
  });

  it("accepts the exact decoded limit with unknown length and cancels one byte over", async () => {
    const exact = streamed([2, 3]);
    await expect(boundedResponseBody(exact.response, 5)).resolves.toEqual(
      Uint8Array.of(1, 1, 2, 2, 2),
    );
    const tooLarge = streamed([2, 4, 100]);
    await expect(boundedResponseBody(tooLarge.response, 5)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
      details: { limit: 5, actual: 6 },
    });
    expect(tooLarge.cancelled()).toBe(true);
  });

  it("rejects advertised oversize before pulling and detects misleading small lengths", async () => {
    const advertised = streamed([10], { "Content-Length": "10" });
    await expect(boundedResponseBody(advertised.response, 5)).rejects.toMatchObject({
      details: { actual: 10 },
    });
    expect(advertised.cancelled()).toBe(true);
    const misleading = streamed([3, 3, 100], { "Content-Length": "1" });
    await expect(boundedResponseBody(misleading.response, 5)).rejects.toMatchObject({
      details: { actual: 6 },
    });
    expect(misleading.cancelled()).toBe(true);
  });

  it("checks decoded compressed bytes rather than trusting encoded Content-Length", async () => {
    const response = streamed([3, 3, 100], { "Content-Encoding": "gzip", "Content-Length": "2" });
    await expect(boundedResponseBody(response.response, 5)).rejects.toMatchObject({
      details: { actual: 6 },
    });
    expect(response.cancelled()).toBe(true);
    const small = streamed([1], { "Content-Encoding": "gzip", "Content-Length": "20" });
    await expect(boundedResponseBody(small.response, 5)).resolves.toHaveProperty("byteLength", 1);
    await expect(boundedResponseBody(new Response(null), 0)).resolves.toHaveProperty(
      "byteLength",
      0,
    );
  });

  it("enforces limits on full-body fallback and size probes", async () => {
    const store = httpStore({
      baseUrl: "https://test.invalid/",
      maxResponseBytes: 5,
      fetch: async () => streamed([3, 3, 100]).response,
    });
    await expect(store.getRange("ignored-range", { offset: 0, length: 1 })).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    await expect(store.head("probe")).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(store.get("whole")).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  });

  it("bounds fallback cache residency and supports disabling it", async () => {
    let requests = 0;
    const create = (maxFullObjectCacheBytes: number) =>
      httpStore({
        baseUrl: "https://test.invalid/",
        maxResponseBytes: 5,
        maxFullObjectCacheBytes,
        fetch: async () => {
          requests += 1;
          return new Response(Uint8Array.of(1, 2, 3, 4));
        },
      });
    const store = create(4);
    await store.getRange("a", { offset: 1, length: 2 });
    await store.getRange("a", { offset: 1, length: 2 });
    expect(requests).toBe(1);
    await store.getRange("b", { offset: 1, length: 2 });
    await store.getRange("a", { offset: 1, length: 2 });
    expect(requests).toBe(3);
    const disabled = create(0);
    await disabled.getRange("a", { offset: 1, length: 2 });
    await disabled.getRange("a", { offset: 1, length: 2 });
    expect(requests).toBe(5);
    await expect(disabled.getRange("a", { offset: 0, length: 0 })).resolves.toHaveProperty(
      "byteLength",
      0,
    );
    expect(requests).toBe(5);
    for (const bytes of [-1, NaN, Infinity, 0.5]) {
      expect(() => create(bytes)).toThrow();
      expect(() =>
        httpStore({ baseUrl: "https://test.invalid/", maxResponseBytes: bytes }),
      ).toThrow();
    }
  });
});

describe("owned HTTP response allocations", () => {
  it("charges replacement overlap and falls back to exact growth under pressure", async () => {
    const budget = new MemoryBudget(9);
    const scope = new MemoryScope(budget);
    const owner = await ownedResponseBody(streamed([4, 1]).response, 16, scope);
    expect(owner.value).toEqual(Uint8Array.of(1, 1, 1, 1, 2));
    expect(owner.bytes).toBe(5);
    expect(budget.peakBytes).toBe(9);
    expect(budget.usedBytes).toBe(5);
    const retained = owner.retain();
    scope.dispose();
    expect(budget.usedBytes).toBe(5);
    retained.release();
    expect(budget.usedBytes).toBe(0);
  });

  it("rejects growth before allocation when old and new buffers cannot coexist", async () => {
    const budget = new MemoryBudget(8);
    const input = streamed([4, 1, 1]);
    await expect(
      ownedResponseBody(input.response, 16, new MemoryScope(budget)),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(input.cancelled()).toBe(true);
    expect(budget.peakBytes).toBe(4);
    expect(budget.usedBytes).toBe(0);
  });

  it("keeps the full backing allocation charged when returning a shorter view", async () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const owner = await ownedResponseBody(
      streamed([3], { "Content-Length": "8" }).response,
      8,
      scope,
    );
    expect(owner.value.byteLength).toBe(3);
    expect(owner.bytes).toBe(8);
    expect(budget.usedBytes).toBe(8);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  });

  it("cancels pending body reads on disposal while keeping their buffers charged until unwind", async () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    let started: (() => void) | undefined;
    let cancelledBytes: number | undefined;
    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });
    const response = new Response(
      new ReadableStream<Uint8Array>(
        {
          pull() {
            started?.();
            return new Promise<void>(() => {});
          },
          cancel() {
            cancelledBytes = budget.usedBytes;
          },
        },
        { highWaterMark: 0 },
      ),
      { headers: { "Content-Length": "8" } },
    );
    const read = ownedResponseBody(response, 8, scope);
    await ready;
    scope.dispose();
    expect(cancelledBytes).toBe(8);
    expect(budget.usedBytes).toBe(8);
    await expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(budget.usedBytes).toBe(0);
  });

  it("retains only the requested window when Range is ignored", async () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const store = httpStore({
      baseUrl: "https://test.invalid/",
      fetch: async () =>
        new Response(Uint8Array.of(1, 2, 3, 4), { headers: { "Content-Length": "4" } }),
    });
    const owner = await store.getRangeOwned?.("a", { offset: 1, length: 1 }, scope);
    expect(owner?.value).toEqual(Uint8Array.of(2));
    expect(owner?.bytes).toBe(1);
    expect(budget.usedBytes).toBe(1);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  });
});

it("releases owned results aborted at the controlled-read handoff", async () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  const controller = new AbortController();
  const source = httpStore({
    baseUrl: "https://test.invalid/",
    fetch: async () => {
      controller.abort("handoff");
      return new Response(Uint8Array.of(1, 2), { status: 206 });
    },
  });
  const controlled = withObjectStoreReadControls(source, {
    signal: controller.signal,
    maxConcurrentReads: 1,
  });
  await expect(
    controlled.getRangeOwned?.("a", { offset: 0, length: 2 }, scope),
  ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("does not start HTTP work for a disposed ownership scope", async () => {
  let calls = 0;
  const scope = new MemoryScope(new MemoryBudget(8));
  scope.dispose();
  const store = httpStore({
    baseUrl: "https://test.invalid/",
    fetch: async () => {
      calls += 1;
      return new Response(Uint8Array.of(1));
    },
  });
  await expect(store.getRangeOwned?.("a", { offset: 0, length: 1 }, scope)).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  expect(calls).toBe(0);
});

it("passes scope cancellation to an HTTP request awaiting headers", async () => {
  const scope = new MemoryScope(new MemoryBudget(8));
  let started: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  const store = httpStore({
    baseUrl: "https://test.invalid/",
    fetch: async (_input, init) => {
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new Error("transport aborted")), {
          once: true,
        });
        started?.();
      });
    },
  });
  const read = store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
  await ready;
  scope.dispose();
  await expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(scope.budget.usedBytes).toBe(0);
});

it("propagates query read-control cancellation into pending owned HTTP I/O", async () => {
  const controller = new AbortController();
  const scope = new MemoryScope(new MemoryBudget(8));
  let started: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  let cancelled = false;
  const source = httpStore({
    baseUrl: "https://test.invalid/",
    fetch: async (_input, init) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => {
            cancelled = true;
            reject(new Error("cancelled"));
          },
          { once: true },
        );
        started?.();
      }),
  });
  const store = withObjectStoreReadControls(source, {
    signal: controller.signal,
    maxConcurrentReads: 1,
  });
  const read = store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
  await ready;
  controller.abort();
  await expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(cancelled).toBe(true);
  expect(scope.disposed).toBe(false);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("propagates query read-control cancellation into pending owned HTTP metadata", async () => {
  const controller = new AbortController();
  const scope = new MemoryScope(new MemoryBudget(4096));
  let started: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  let cancelled = false;
  const source = httpStore({
    baseUrl: "https://test.invalid/",
    fetch: async (_input, init) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => {
            cancelled = true;
            reject(new Error("cancelled"));
          },
          { once: true },
        );
        started?.();
      }),
  });
  const store = withObjectStoreReadControls(source, {
    signal: controller.signal,
    maxConcurrentReads: 1,
  });
  const read = store.headOwned?.("file", scope);
  await ready;
  controller.abort();
  await expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(cancelled).toBe(true);
  expect(scope.disposed).toBe(false);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("admits HTTP range storage before dispatch and transfers it without double charging", async () => {
  for (const cap of [3, 4]) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    let requests = 0;
    const store = httpStore({
      baseUrl: "https://test.invalid/",
      fetch: async () => {
        requests++;
        expect(budget.usedBytes).toBe(4);
        return new Response(new Uint8Array([1, 2, 3, 4]), {
          status: 206,
          headers: { "Content-Length": "4" },
        });
      },
    });
    const pending = store.getRangeOwned?.("file", { offset: 0, length: 4 }, scope);
    if (cap === 3) {
      await expect(pending).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      expect(requests).toBe(0);
    } else {
      const owner = await pending;
      if (owner === undefined) throw new Error("Missing owned result");
      expect([...owner.value]).toEqual([1, 2, 3, 4]);
      expect(budget.peakBytes).toBe(4);
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("cleans preflight reservations on fetch failure and supports ignored ranges", async () => {
  for (const fail of [true, false]) {
    const budget = new MemoryBudget(12);
    const scope = new MemoryScope(budget);
    const store = httpStore({
      baseUrl: "https://test.invalid/",
      fetch: async () => {
        expect(budget.usedBytes).toBe(4);
        if (fail) throw new Error("network failure");
        return new Response(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]), {
          headers: { "Content-Length": "8" },
        });
      },
    });
    const pending = store.getRangeOwned?.("file", { offset: 2, length: 4 }, scope);
    if (fail) await expect(pending).rejects.toThrow("network failure");
    else {
      const owner = await pending;
      if (owner === undefined) throw new Error("Missing owned result");
      expect([...owner.value]).toEqual([2, 3, 4, 5]);
      expect(budget.peakBytes).toBe(4);
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("aborts a scheduled HTTP read through its allocation scope", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const controller = new AbortController();
  let transportAborted = false;
  const store = httpStore({
    baseUrl: "https://test.invalid/",
    fetch: async (_input, init) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => {
            transportAborted = true;
            reject(new Error("transport aborted"));
          },
          { once: true },
        );
      }),
  });
  const read = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "HTTP stage",
    signal: controller.signal,
    async execute(scope) {
      const child = scope.childScope();
      child.allocate(4, "decode scratch", () => new Uint8Array(4));
      const owner = await store.getRangeOwned?.("file", { offset: 0, length: 4 }, child);
      return owner?.value;
    },
  });
  const rejected = expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(root.usedBytes).toBe(8);
  controller.abort();
  expect(transportAborted).toBe(true);
  expect(root.usedBytes).toBe(8);
  await rejected;
  expect(root.usedBytes).toBe(0);
});
