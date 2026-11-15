import assert from "node:assert/strict";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  vectorTopKOwnedBatch,
} from "../packages/core/dist/index.js";

const results = [];
const rows = 2048;
const warmupRounds = Number(process.env.LAKEQL_PACKED_ORDER_WARMUPS ?? 2);
const measuredRounds = 6;
for (const kind of [
  "numeric",
  "ascii",
  "unicode",
  "ascii-unicode-tail",
  "common-prefix",
  "unicode-prefix",
  "dictionary",
]) {
  for (const storage of kind === "numeric" ? ["numeric"] : ["flat", "dictionary"]) {
    for (const width of kind === "numeric" ? [0] : [16, 1024]) {
      const cardinality = kind === "dictionary" ? 64 : rows;
      const texts = Array.from({ length: cardinality }, (_, i) => {
        const key = String(i).padStart(6, "0");
        return kind === "common-prefix"
          ? key.padStart(width, "x")
          : kind === "unicode-prefix"
            ? "雪".repeat(Math.max(0, width - 6)) + key
            : kind === "unicode"
              ? `雪${key}`.padEnd(width, "x")
              : kind === "ascii-unicode-tail"
                ? `${key}界`.padEnd(width, "x")
                : key.padEnd(width, "x");
      });
      const values = Array.from({ length: rows }, (_, i) =>
        kind === "numeric" ? (i * 17) % rows : texts[(i * 17) % cardinality],
      );
      const ids = Int32Array.from({ length: rows }, (_, i) => i);
      const selection = Uint8Array.from({ length: rows }, (_, i) => (i % 7 === 0 ? 0 : 1));
      const expected = Array.from({ length: rows }, (_, id) => ({ id, key: values[id] }))
        .filter(({ id }) => selection[id] === 1)
        .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.id - b.id))
        .slice(7, 71);
      let key;
      let inputBytes = ids.byteLength + selection.byteLength + 2048;
      if (kind === "numeric") {
        key = { type: "f64", values: Float64Array.from(values) };
        inputBytes += key.values.byteLength;
      } else {
        const encoder = new TextEncoder();
        const fixedLength = encoder.encode(texts[0]).length;
        const packedRows = storage === "flat" ? rows : cardinality;
        const packed = {
          type: "utf8-view",
          data: new Uint8Array(packedRows * fixedLength),
          fixedLength,
          length: packedRows,
        };
        for (let i = 0; i < packedRows; i++) {
          const text = texts[storage === "flat" ? (i * 17) % cardinality : i];
          packed.data.set(encoder.encode(text), i * fixedLength);
        }
        if (storage === "dictionary") {
          const indices = Uint32Array.from({ length: rows }, (_, i) => (i * 17) % cardinality);
          key = { type: "dict", dictionary: packed, indices };
          inputBytes += indices.byteLength;
        } else key = packed;
        inputBytes += packed.data.byteLength;
      }
      const batch = { rowCount: rows, columns: { id: { type: "i32", values: ids }, key } };
      const times = [];
      let incrementalPeak = 0;
      for (let round = 0; round < warmupRounds + measuredRounds; round++) {
        const pool = new MemoryBudget(64 * 1024 * 1024);
        const scope = new MemoryScope(pool);
        const input = scope.allocate(inputBytes, "precreated fixture", () => batch).retain();
        scope.dispose();
        const start = performance.now();
        const output = vectorTopKOwnedBatch(
          input,
          [{ column: "key" }],
          { offset: 7, limit: 64 },
          selection,
        );
        if (round >= warmupRounds) times.push(performance.now() - start);
        incrementalPeak = Math.max(incrementalPeak, pool.peakBytes - inputBytes);
        assert.deepEqual(materializeBatchRows(output.value), expected);
        output.release();
        input.release();
        assert.equal(pool.usedBytes, 0);
      }
      times.sort((a, b) => a - b);
      results.push({
        kind,
        storage,
        width,
        rows,
        elapsedMs: times[3],
        incrementalPeakBytes: incrementalPeak,
        afterReleaseBytes: 0,
      });
    }
  }
}
console.log(
  JSON.stringify({
    warmupRounds,
    measuredRounds,
    cases: results,
    elapsedMs: results.reduce((sum, value) => sum + value.elapsedMs, 0),
    scope:
      "Owned top-k over numeric and packed/dictionary vector fixtures, stable selection parity checked. Fixture creation excluded; incremental modeled workspace, not process heap or cloud throughput.",
  }),
);
