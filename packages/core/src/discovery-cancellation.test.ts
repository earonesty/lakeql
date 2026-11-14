import { expect, it } from "vitest";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memoryCache } from "./runtime.js";
import type { ObjectInfo } from "./store.js";

it.each([
  "head",
  "list-empty",
  "list-value",
  "cache-get",
  "cache-set",
] as const)("rejects cancellation while %s discovery settles", async (mode) => {
  const store = memoryStore();
  await store.put("data/one.parquet", new Uint8Array(1));
  const controller = new AbortController();
  let enter!: () => void, finish!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let settled = false,
    planned = false;
  const wait = async () => {
    enter();
    await pending;
    settled = true;
  };
  const cache = memoryCache<ObjectInfo[]>();
  if (mode === "head") {
    const head = store.head.bind(store);
    store.head = async (path) => {
      await wait();
      return head(path);
    };
  } else if (mode.startsWith("list")) {
    store.list = async function* () {
      try {
        await wait();
        if (mode === "list-value") yield { path: "data/one.parquet", size: 1 };
      } finally {
        settled = true;
      }
    };
  } else if (mode === "cache-get") {
    cache.get = async () => {
      await wait();
      return { value: [{ path: "data/one.parquet", size: 1 }] };
    };
  } else
    cache.set = async () => {
      await wait();
    };
  const lake = new Lake({
    store,
    planningCache: cache,
    budget: { signal: controller.signal },
    scanner: {
      async planObjects(objects) {
        planned = true;
        return objects;
      },
      scan() {
        throw Error("unexpected scan");
      },
    },
  });
  const work = lake
    .path(mode.startsWith("list") ? "data/*.parquet" : "data/one.parquet")
    .planTasks();
  const checked = expect(work).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await entered;
  controller.abort();
  expect(settled).toBe(false);
  finish();
  await checked;
  expect(settled).toBe(true);
  expect(planned).toBe(false);
});

it("closes listing at the first matching object beyond maxFiles", async () => {
  const store = memoryStore();
  let advanced = 0,
    closed = false;
  store.list = async function* () {
    try {
      for (const path of ["data/skip.txt", "data/a.parquet", "data/b.parquet", "data/c.parquet"]) {
        advanced++;
        yield { path, size: 1 };
      }
    } finally {
      closed = true;
    }
  };
  const lake = new Lake({
    store,
    budget: { maxFiles: 1 },
    scanner: {
      scan() {
        throw Error("unexpected scan");
      },
    },
  });
  await expect(lake.path("data/").planTasks()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(advanced).toBe(3);
  expect(closed).toBe(true);
});

it.each([
  "data/one.parquet",
  "data/*.parquet",
  "data/",
])("enforces zero maxFiles on cold and cached discovery of %s", async (source) => {
  const store = memoryStore();
  await store.put("data/one.parquet", new Uint8Array(1));
  const planningCache = memoryCache<ObjectInfo[]>();
  const scanner = {
    scan() {
      throw Error("unexpected scan");
    },
  };
  const constrained = new Lake({ store, planningCache, scanner, budget: { maxFiles: 0 } });
  await expect(constrained.path(source).planTasks()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  const allowed = new Lake({ store, planningCache, scanner, budget: { maxFiles: 1 } });
  expect(await allowed.path(source).planTasks()).toHaveLength(1);
  await expect(constrained.path(source).planTasks()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
});
