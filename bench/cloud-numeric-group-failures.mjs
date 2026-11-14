// Real R2 range responses with deterministic failures at the owned adapter boundary.
import assert from "node:assert/strict";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { httpStore } from "../packages/http/dist/index.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { prepareNumericColumnPages } from "../packages/parquet/dist/numeric-page-alignment.js";
import { planNumericPageGroup } from "../packages/parquet/dist/numeric-page-group.js";
import { DEFAULT_PARSERS } from "../packages/parquet/node_modules/hyparquet/src/convert.js";
import { getSchemaPath } from "../packages/parquet/node_modules/hyparquet/src/schema.js";

const baseUrl = "https://pub-9d5bcb33a5384d79875a943eef183b6d.r2.dev/bench/";
const path = "window-events.parquet";
let responses = 0;
const store = httpStore({
  baseUrl,
  fetch: async (input, init) => {
    const response = await fetch(input, {
      ...init,
      signal: AbortSignal.any([AbortSignal.timeout(30000), ...(init?.signal ? [init.signal] : [])]),
    });
    if (init?.method !== "HEAD") {
      assert.ok(new Headers(init?.headers).has("Range"));
      assert.equal(response.status, 206);
      responses++;
    }
    return response;
  },
});
const file = await asyncBufferFromStore(store, path);
const metadata = await readParquetMetadataFromFile(file);
const inspection = new MemoryScope(new MemoryBudget(65536));
const entries = [];
try {
  for (const name of ["event_id", "amount"]) {
    const column = metadata.row_groups[0].columns.find(
      (entry) => entry.meta_data.path_in_schema.join(".") === name,
    ).meta_data;
    const schemaPath = getSchemaPath(metadata.schema, column.path_in_schema);
    const decoder = {
      ...column,
      pathInSchema: column.path_in_schema,
      schemaPath,
      element: schemaPath.at(-1).element,
      parsers: DEFAULT_PARSERS,
    };
    const start = Number(column.data_page_offset);
    for await (const page of prepareNumericColumnPages(
      file,
      decoder,
      { start, end: start + Number(column.total_compressed_size) },
      inspection,
      { start: 0, end: 1 },
    ))
      entries.push(page);
  }
} finally {
  inspection.dispose();
}
assert.equal(inspection.budget.usedBytes, 0);
assert.equal(entries.length, 2);
const reports = [];
for (const mode of ["peer-failure", "cancel"]) {
  const ready = Promise.withResolvers();
  const release = Promise.withResolvers();
  const failing = Promise.withResolvers();
  const failure = new Error("Injected peer failure after real HTTP response");
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  let arrivals = 0,
    pending = 0,
    aborted = 0;
  const heldBytes = [];
  const responseStart = responses;
  const source = {
    ...file,
    async sliceOwned(start, end, transport) {
      const owner = await file.sliceOwned(start, end, transport);
      // The delayed adapter owns a settlement pin independently of the caller scope.
      const settlementPin = owner.retain();
      const index = arrivals++;
      heldBytes.push(owner.value.byteLength);
      pending++;
      transport.signal.addEventListener("abort", () => aborted++, { once: true });
      if (arrivals === entries.length) ready.resolve();
      try {
        await ready.promise;
        if (mode === "peer-failure" && index === 0) {
          owner.release();
          failing.resolve();
          throw failure;
        }
        // Deliberately delay adapter settlement after the actual response arrives.
        await release.promise;
        return owner;
      } finally {
        settlementPin.release();
        pending--;
      }
    },
  };
  const plan = planNumericPageGroup(entries, source);
  assert.ok(plan);
  let settled = false;
  const execution = plan
    .executeOwned(source, scope, 2)
    .then(
      (owners) => {
        for (const owner of owners) owner.release();
        throw new Error("Expected failure");
      },
      (error) => error,
    )
    .finally(() => {
      settled = true;
    });
  const watchdog = setTimeout(() => {
    ready.reject(new Error("Timed out waiting for both real responses"));
    release.resolve();
  }, 30000);
  try {
    await ready.promise;
    assert.equal(responses - responseStart, 2);
    if (mode === "cancel") scope.dispose();
    else {
      await failing.promise;
      // Flush rejection handling so group disposal reaches the other transport.
      await new Promise((resolve) => setImmediate(resolve));
    }
    assert.equal(settled, false);
    assert.ok(pending > 0);
    assert.equal(aborted, 2);
    const heldAfterAbort = budget.usedBytes;
    assert.ok(heldAfterAbort >= Math.min(...heldBytes));
    release.resolve();
    const error = await execution;
    if (mode === "peer-failure") assert.equal(error, failure);
    else assert.equal(error.code, "LAKEQL_ABORTED");
    assert.equal(pending, 0);
    scope.dispose();
    assert.equal(budget.usedBytes, 0);
    reports.push({
      mode,
      responses: responses - responseStart,
      heldBytes,
      heldAfterAbort,
      peakBytes: budget.peakBytes,
      admittedBytes: plan.bytes,
      releasedBytes: budget.usedBytes,
    });
  } finally {
    clearTimeout(watchdog);
    release.resolve();
    await execution;
    scope.dispose();
  }
}
// Interrupt consumption of actual remote bytes, distinct from adapter settlement delay.
for (const mode of ["stream-error", "short-stream"]) {
  const target = entries[0].page;
  const targetRange = `bytes=${target.bodyOffset}-${target.bodyEnd - 1}`;
  const failure = new Error("Injected interruption while consuming real R2 bytes");
  let deliveredBytes = 0,
    upstreamCanceled = false,
    rangedResponses = 0;
  const interruptedStore = httpStore({
    baseUrl,
    fetch: async (input, init) => {
      const response = await fetch(input, {
        ...init,
        signal: AbortSignal.any([
          AbortSignal.timeout(30000),
          ...(init?.signal ? [init.signal] : []),
        ]),
      });
      if (init?.method === "HEAD") return response;
      assert.equal(response.status, 206);
      rangedResponses++;
      if (new Headers(init?.headers).get("Range") !== targetRange) return response;
      const reader = response.body.getReader();
      let sent = false;
      const body = new ReadableStream({
        async pull(controller) {
          if (!sent) {
            sent = true;
            const chunk = await reader.read();
            assert.equal(chunk.done, false);
            const partial = chunk.value.subarray(
              0,
              Math.max(1, Math.floor(chunk.value.byteLength / 2)),
            );
            deliveredBytes += partial.byteLength;
            controller.enqueue(partial);
            return;
          }
          await reader.cancel();
          upstreamCanceled = true;
          if (mode === "stream-error") controller.error(failure);
          else controller.close();
        },
        async cancel(reason) {
          await reader.cancel(reason);
          upstreamCanceled = true;
        },
      });
      return new Response(body, { status: response.status, headers: response.headers });
    },
  });
  const interruptedFile = await asyncBufferFromStore(interruptedStore, path);
  const discoveryResponses = rangedResponses;
  rangedResponses = 0;
  const plan = planNumericPageGroup(entries, interruptedFile);
  assert.ok(plan);
  const budget = new MemoryBudget(plan.bytes);
  const scope = new MemoryScope(budget);
  let caught;
  try {
    await plan.executeOwned(interruptedFile, scope, 2).then(
      (owners) => {
        for (const owner of owners) owner.release();
        throw new Error("Unexpected successful interrupted read");
      },
      (error) => {
        caught = error;
      },
    );
    assert.ok(caught);
    if (mode === "stream-error") assert.equal(caught, failure);
    assert.ok(deliveredBytes > 0 && deliveredBytes < target.bodyEnd - target.bodyOffset);
    assert.equal(upstreamCanceled, true);
    assert.equal(budget.usedBytes, 0);
    reports.push({
      mode,
      discoveryResponses,
      rangedResponses,
      deliveredBytes,
      expectedBodyBytes: target.bodyEnd - target.bodyOffset,
      errorCode: caught.code ?? null,
      errorMessage: caught.message,
      peakBytes: budget.peakBytes,
      admittedBytes: plan.bytes,
      releasedBytes: budget.usedBytes,
    });
  } finally {
    scope.dispose();
  }
}
console.log(
  JSON.stringify(
    {
      source: baseUrl + path,
      scope:
        "Real HTTP 206 body reads; injected adapter settlement failures and response-consumption interruption/truncation; not a real server outage; excludes metadata and JS heap",
      reports,
    },
    null,
    2,
  ),
);
