// Run pnpm bench:query-state. This measures in-memory row execution, not cloud I/O.
import assert from "node:assert/strict";
import { Lake, memoryStore } from "../packages/core/dist/index.js";

const reports = [];
for (const kind of ["grouped", "median", "distinct", "sort"]) {
  const count = 1000;
  const rows = Array.from({ length: count }, (_, id) => ({
    id,
    group: id,
    value: 1000000000 + id,
    text: `${id}:`.padEnd(64, "x"),
  }));
  const store = memoryStore();
  await store.put("input", new Uint8Array(1));
  const run = async () => {
    const lake = new Lake({
      store,
      budget: { maxMemoryBytes: 64 * 1024 * 1024 },
      scanner: {
        async *scan() {
          yield rows;
        },
      },
    });
    if (kind === "grouped" || kind === "median") {
      const result = await lake
        .path("input")
        .run()
        .aggregateWithState(kind === "grouped" ? ["group"] : [], {
          total: { op: kind === "grouped" ? "sum" : "median", column: "value" },
        });
      assert.equal(result.rows.length, kind === "grouped" ? count : 1);
      return;
    }
    const query =
      kind === "sort"
        ? lake.path("input").orderBy([{ column: "id", direction: "desc" }])
        : lake.path("input").distinct();
    const result = await query.toArray();
    assert.equal(result.length, count);
    assert.equal(result[0].id, kind === "sort" ? count - 1 : 0);
  };
  await run();
  const samples = [];
  for (let trial = 0; trial < 5; trial++) {
    const start = performance.now();
    await run();
    samples.push(performance.now() - start);
  }
  let encodedCharacters = 0;
  const encode = TextEncoder.prototype.encode;
  TextEncoder.prototype.encode = function (input) {
    encodedCharacters += input?.length ?? 0;
    return encode.call(this, input);
  };
  try {
    await run();
  } finally {
    TextEncoder.prototype.encode = encode;
  }
  reports.push({
    kind,
    rows: count,
    medianMs: samples.sort((a, b) => a - b)[2],
    encodedCharacters,
  });
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      scope:
        "In-memory public query paths with a 64 MiB serialized limit. One warmup/five timed samples; encoding-volume instrumentation runs separately. Includes query finalization/checkpoint serialization and assertions. Does not measure total heap or remote throughput.",
      reports,
    },
    null,
    2,
  ),
);
