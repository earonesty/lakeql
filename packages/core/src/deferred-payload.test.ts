import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { jsonSafeValue } from "./evaluator.js";
import { gt } from "./expr.js";
import { stableStringify } from "./manifest.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type ScanOptions } from "./query.js";
import type { ObjectInfo } from "./store.js";
import type { Row } from "./types.js";

const bytes = (value: unknown) =>
  new TextEncoder().encode(stableStringify(jsonSafeValue(value))).length;

async function fixture(rows: Row[], maxMemoryBytes?: number) {
  const store = memoryStore();
  await store.put("input", new Uint8Array(1));
  let object: ObjectInfo | undefined;
  for await (const entry of store.list("input")) object = entry;
  const payloadRows: number[] = [];
  let closed = 0;
  const lake = new Lake({
    store,
    budget: { ...(maxMemoryBytes === undefined ? {} : { maxMemoryBytes }) },
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
])("enforces exact reference-plus-result caps with Unicode and binary payloads (ordered=%s)", async (ordered) => {
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
  const cap = bytes(refs) + bytes(entries);
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
