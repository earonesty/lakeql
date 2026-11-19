import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { jsonSafeValue } from "./evaluator.js";
import { gt, isNotNull } from "./expr.js";
import { stableStringify } from "./manifest.js";
import { memoryStore } from "./memory-store.js";
import { estimateBatchBytes } from "./physical.js";
import { Lake, type ScanOptions } from "./query.js";
import type { ObjectInfo } from "./store.js";
import type { Row } from "./types.js";

const bytes = (value: unknown) =>
  new TextEncoder().encode(stableStringify(jsonSafeValue(value))).length;

async function fixture(rows: Row[], maxMemoryBytes?: number, signal?: AbortSignal) {
  const store = memoryStore();
  await store.put("input", new Uint8Array(1));
  let object: ObjectInfo | undefined;
  for await (const entry of store.list("input")) object = entry;
  const payloadRows: number[] = [];
  let closed = 0;
  const lake = new Lake({
    store,
    budget: {
      ...(maxMemoryBytes === undefined ? {} : { maxMemoryBytes }),
      ...(signal === undefined ? {} : { signal }),
    },
    scanner: {
      async *scan() {
        yield [];
        throw Error("expected deferred scan");
      },
      async *scanVectorBatches(_path: string, options: ScanOptions) {
        const payload = options.columns?.includes("payload") === true;
        try {
          for (
            let index = options.rowStart ?? 0;
            index < (options.rowEnd ?? rows.length);
            index++
          ) {
            if (payload) payloadRows.push(index);
            const row = rows[index];
            if (row === undefined) throw Error("missing fixture row");
            yield {
              rowOffset: index,
              batch: batchFromColumns(
                Object.fromEntries(
                  (options.columns ?? Object.keys(row)).map((name) => [name, [row[name] ?? null]]),
                ),
              ),
            };
          }
        } finally {
          if (payload) closed++;
        }
      },
    },
  });
  return {
    lake,
    object,
    payloadRows,
    get closed() {
      return closed;
    },
  };
}

it.each([
  false,
  true,
])("rejects wide deferred payloads and closes the scan (ordered=%s)", async (ordered) => {
  const f = await fixture(
    [0, 1, 2].map((score) => ({ score, payload: "x".repeat(8192) })),
    2048,
  );
  let query = f.lake
    .path("input")
    .select(["score", "payload"])
    .where(gt("score", -1))
    .batchSize(1)
    .limit(2);
  if (ordered) query = query.orderBy([{ column: "score" }]);
  await expect(query.toArray()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { metric: "operator memory bytes", limit: 2048 },
  });
  expect(f.payloadRows).toEqual([0]);
  expect(f.closed).toBe(1);
});

it.each([
  false,
  true,
])("enforces exact live-reference/output/decoded-batch caps with Unicode and binary payloads (ordered=%s)", async (ordered) => {
  const rows = [0, 1, 2, 3].map((score) => ({
    score,
    payload: "雪🙂\n".repeat(256),
    blob: Uint8Array.of(0, 255, score),
  }));
  const f = await fixture(rows);
  const refs = [1, 2].map((rowIndex) => ({
    path: "input",
    object: f.object,
    rowIndex,
    keys: { score: rowIndex },
  }));
  const entries = [1, 2].map((index) => [`input\u001f${index}`, rows[index]]);
  const cap =
    bytes(refs) +
    refs.length * Float64Array.BYTES_PER_ELEMENT +
    bytes([entries[0]]) +
    estimateBatchBytes(
      batchFromColumns({
        payload: [rows[1]?.payload],
        blob: [rows[1]?.blob],
      }),
    );
  const run = async (maxMemoryBytes?: number) => {
    const next = await fixture(rows, maxMemoryBytes);
    let query = next.lake
      .path("input")
      .select(["score", "payload", "blob"])
      .where(gt("score", -1))
      .batchSize(1)
      .offset(1)
      .limit(2);
    if (ordered) query = query.orderBy([{ column: "score" }]);
    return query.toArray();
  };
  expect(await run()).toEqual(rows.slice(1, 3));
  expect(await run(cap)).toEqual(rows.slice(1, 3));
  await expect(run(cap - 1)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { actual: cap, limit: cap - 1 },
  });
});

it.each([
  false,
  true,
])("does not materialize payloads for empty results or zero limit (ordered=%s)", async (ordered) => {
  for (const limit of [0, 2]) {
    const f = await fixture([{ score: 1, payload: "x".repeat(8192) }], 2048);
    let query = f.lake
      .path("input")
      .select(["score", "payload"])
      .where(gt("score", 99))
      .limit(limit);
    if (ordered) query = query.orderBy([{ column: "score" }]);
    expect(await query.toArray()).toEqual([]);
    expect(f.payloadRows).toEqual([]);
  }
});

it.each([
  false,
  true,
])("streams a total result larger than its live allowance with backpressure (ordered=%s)", async (ordered) => {
  const rows = Array.from({ length: 20 }, (_, score) => ({ score, payload: "x".repeat(1500) }));
  const f = await fixture(rows, 12000);
  let query = f.lake
    .path("input")
    .select(["score", "payload"])
    .where(gt("score", -1))
    .batchSize(4)
    .limit(20);
  if (ordered) query = query.orderBy([{ column: "score", direction: "desc" }]);
  const iterator = query.run().batches()[Symbol.asyncIterator]();
  const first = await iterator.next();
  expect(first.done).toBe(false);
  expect(first.value).toEqual([rows[ordered ? 19 : 0]]);
  expect(f.payloadRows).toHaveLength(1);
  await Promise.resolve();
  expect(f.payloadRows).toHaveLength(1);
  const out = [...(first.value ?? [])];
  for (;;) {
    const next = await iterator.next();
    if (next.done) break;
    expect(next.value.length).toBeLessThanOrEqual(4);
    out.push(...next.value);
  }
  expect(out).toEqual(ordered ? [...rows].reverse() : rows);
  expect(bytes(out)).toBeGreaterThan(12000);
});

it.each([
  false,
  true,
])("closes without fetching future payloads on consumer return or paused abort (ordered=%s)", async (ordered) => {
  for (const abort of [false, true]) {
    const controller = new AbortController();
    const f = await fixture(
      Array.from({ length: 12 }, (_, score) => ({ score, payload: "x".repeat(1000) })),
      10000,
      controller.signal,
    );
    let query = f.lake.path("input").select(["score", "payload"]).where(gt("score", -1)).limit(12);
    if (ordered) query = query.orderBy([{ column: "score" }]);
    const iterator = query.run().batches()[Symbol.asyncIterator]();
    await iterator.next();
    const reads = f.payloadRows.length;
    if (abort) {
      controller.abort("paused");
      await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    } else await iterator.return?.();
    expect(f.payloadRows).toHaveLength(reads);
    expect(f.closed).toBe(1);
  }
});

it("shrinks unexpectedly wide windows without losing or duplicating output", async () => {
  const rows = [16, 1400, 1400, 1400].map((width, score) => ({
    score,
    payload: "x".repeat(width),
  }));
  const f = await fixture(rows, 6000);
  const out = await f.lake
    .path("input")
    .select(["score", "payload"])
    .orderBy([{ column: "score" }])
    .batchSize(4)
    .limit(4)
    .toArray();
  expect(out).toEqual(rows);
  expect(f.payloadRows.filter((index) => index === 1).length).toBeGreaterThan(1);
});

it("preserves global order and stable ties while windows cross source files", async () => {
  const store = memoryStore();
  const sources: Record<string, Row[]> = {
    "input/a": [
      { score: 5, payload: "a5" },
      { score: 1, payload: "a1" },
    ],
    "input/b": [
      { score: 4, payload: "b4" },
      { score: 1, payload: "b1" },
    ],
  };
  for (const path of Object.keys(sources)) await store.put(path, new Uint8Array(1));
  const lake = new Lake({
    store,
    budget: { maxMemoryBytes: 4096 },
    scanner: {
      async *scan() {
        yield [];
        throw Error("expected vector path");
      },
      async *scanVectorBatches(path, options) {
        const rows = sources[path] ?? [];
        for (let index = options.rowStart ?? 0; index < (options.rowEnd ?? rows.length); index++) {
          const row = rows[index];
          if (row === undefined) throw Error("missing row");
          yield {
            rowOffset: index,
            batch: batchFromColumns(
              Object.fromEntries((options.columns ?? []).map((name) => [name, [row[name]]])),
            ),
          };
        }
      },
    },
  });
  expect(
    await lake
      .path("input/*")
      .select(["payload"])
      .batchSize(2)
      .orderBy([{ column: "score", direction: "desc" }])
      .limit(4)
      .toArray(),
  ).toEqual([{ payload: "a5" }, { payload: "b4" }, { payload: "a1" }, { payload: "b1" }]);
});

it("charges computed projection output while it overlaps the decoded input", async () => {
  const f = await fixture([{ score: 0, payload: "x".repeat(1000) }], 3500);
  await expect(
    f.lake
      .path("input")
      .select(["score", "payload"])
      .project({
        upper: { kind: "call", fn: "upper", args: [{ kind: "column", name: "payload" }] },
      })
      .where(gt("score", -1))
      .limit(1)
      .toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("reports a later oversized row after a valid streamed prefix and stops future payload reads", async () => {
  const rows = [16, 10000, 16].map((width, score) => ({ score, payload: "x".repeat(width) }));
  const f = await fixture(rows, 6000);
  const iterator = f.lake
    .path("input")
    .select(["score", "payload"])
    .orderBy([{ column: "score" }])
    .limit(3)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  expect(first.value).toEqual([rows[0]]);
  await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(f.payloadRows).not.toContain(2);
  expect(first.value).toEqual([rows[0]]);
});

it("does not open a ranking scan for ordered LIMIT zero", async () => {
  const store = memoryStore();
  await store.put("input", new Uint8Array(1));
  let scans = 0;
  const lake = new Lake({
    store,
    budget: { maxRowsDecoded: 0, maxFiles: 0 },
    scanner: {
      async *scan() {
        scans++;
        yield [];
      },
      async *scanVectorBatches() {
        scans++;
        yield { rowOffset: 0, batch: batchFromColumns({ score: [1] }) };
      },
    },
  });
  expect(
    await lake
      .path("input")
      .select(["score", "payload"])
      .orderBy([{ column: "score" }])
      .limit(0)
      .toArray(),
  ).toEqual([]);
  expect(scans).toBe(0);
});

it("coalesces measured zero-byte decoded columns instead of probing each row", async () => {
  const rows = Array.from({ length: 32 }, (_, score) => ({ score, payload: null }));
  const f = await fixture(rows, 16384);
  expect(
    await f.lake
      .path("input")
      .select(["score", "payload"])
      .orderBy([{ column: "score" }])
      .limit(rows.length)
      .toArray(),
  ).toEqual(rows);
  expect(f.closed).toBe(2);
});

it.each([
  "resume",
  "return",
  "abort",
] as const)("releases the original reference charge after caller list mutation (%s)", async (action) => {
  const rows = Array.from({ length: 3 }, (_, score) => ({
    tags: [`tag-${score}`],
    payload: `value-${score}`,
  }));
  const controller = new AbortController();
  const f = await fixture(rows, 8000, controller.signal);
  const iterator = f.lake
    .path("input")
    .select(["tags", "payload"])
    .where(isNotNull("tags"))
    .batchSize(1)
    .limit(rows.length)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  const tags = first.value?.[0]?.tags;
  if (!Array.isArray(tags)) throw Error("missing yielded tags");
  tags.push("x".repeat(12000));
  expect(rows[0]?.tags).toEqual(["tag-0"]);
  if (action === "resume") {
    const remaining: Row[] = [];
    for (;;) {
      const next = await iterator.next();
      if (next.done) break;
      remaining.push(...next.value);
    }
    expect(remaining).toEqual(rows.slice(1));
  } else if (action === "return") {
    await expect(iterator.return?.()).resolves.toMatchObject({ done: true });
    expect(f.payloadRows).toEqual([0]);
  } else {
    controller.abort("after output mutation");
    await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(f.payloadRows).toEqual([0]);
  }
});

it("never serializes caller-owned cyclic output during reference release", async () => {
  const rows = [
    { tags: ["first"], payload: "a" },
    { tags: ["second"], payload: "b" },
  ];
  const f = await fixture(rows, 8000);
  const iterator = f.lake
    .path("input")
    .select(["tags", "payload"])
    .where(isNotNull("tags"))
    .batchSize(1)
    .limit(2)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  const tags = first.value?.[0]?.tags;
  if (!Array.isArray(tags)) throw Error("missing tags");
  tags.push(tags);
  expect((await iterator.next()).value).toEqual([rows[1]]);
  expect((await iterator.next()).done).toBe(true);
});

it("releases the full admission charge when a caller shrinks a yielded list", async () => {
  const rows = [
    { tags: ["t".repeat(1000)], payload: "small" },
    { tags: ["second"], payload: "x".repeat(2000) },
  ];
  const f = await fixture(rows, 7000);
  const iterator = f.lake
    .path("input")
    .select(["tags", "payload"])
    .where(isNotNull("tags"))
    .batchSize(1)
    .limit(2)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  const tags = first.value?.[0]?.tags;
  if (!Array.isArray(tags)) throw Error("missing tags");
  tags.length = 0;
  expect((await iterator.next()).value).toEqual([rows[1]]);
  expect((await iterator.next()).done).toBe(true);
});

it("uses admission charges for mutable predicate keys in ordered top-k streams too", async () => {
  const rows = [
    { score: 1, tags: ["one"], payload: "a" },
    { score: 2, tags: ["two"], payload: "b" },
  ];
  const f = await fixture(rows, 8000);
  const iterator = f.lake
    .path("input")
    .select(["score", "tags", "payload"])
    .where(isNotNull("tags"))
    .orderBy([{ column: "score" }])
    .batchSize(1)
    .limit(2)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  const tags = first.value?.[0]?.tags;
  if (!Array.isArray(tags)) throw Error("missing tags");
  tags.push("x".repeat(12000));
  expect((await iterator.next()).value).toEqual([rows[1]]);
  expect((await iterator.next()).done).toBe(true);
});
