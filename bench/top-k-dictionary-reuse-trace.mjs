import assert from "node:assert/strict";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  vectorTopKOwnedBatch,
} from "../packages/core/dist/index.js";

const rows = 8192;
const width = 128;
const encoder = new TextEncoder();
const capacities = [1, 8, 64, 256];
const cases = [];
for (const cardinality of [16, 1024, 8192]) {
  const data = new Uint8Array(cardinality * width);
  const texts = Array.from({ length: cardinality }, (_, index) => `${index}:`.padEnd(width, "x"));
  texts.forEach((text, index) => {
    data.set(encoder.encode(text), index * width);
  });
  for (const stride of [1, 16, 128]) {
    for (const limit of [3, 100]) {
      const pool = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(pool);
      const indices = Uint32Array.from({ length: rows }, (_, row) => (row * 17) % cardinality);
      const valid = Uint8Array.from({ length: rows }, (_, row) => Number(row % 11 !== 0));
      const batch = {
        rowCount: rows,
        columns: {
          id: { type: "i32", values: Int32Array.from({ length: rows }, (_, i) => i) },
          key: {
            type: "dict",
            indices,
            valid,
            dictionary: { type: "utf8-view", data, fixedLength: width, length: cardinality },
          },
        },
      };
      const input = scope.allocate(
        data.byteLength + indices.byteLength + valid.byteLength + rows * 4 + 4096,
        "diagnostic source backing and descriptors",
        () => batch,
      );
      const selection =
        stride === 1
          ? undefined
          : Uint8Array.from({ length: rows }, (_, i) => Number(i % stride === 0));
      const expected = Array.from({ length: rows }, (_, id) => ({
        id,
        key: valid[id] ? texts[indices[id]] : null,
      }))
        .filter((row) => row.id % stride === 0)
        .sort((a, b) => {
          if (a.key === null || b.key === null) {
            if (a.key !== b.key) return a.key === null ? 1 : -1;
          } else if (a.key !== b.key) return a.key < b.key ? -1 : 1;
          return a.id - b.id;
        })
        .slice(0, limit);
      const simulations = capacities.map((capacity) => ({
        capacity,
        entries: new Map(),
        hits: 0,
        misses: 0,
      }));
      const direct = capacities.flatMap((capacity) =>
        ["modulo", "mixed"].map((kind) => ({
          capacity,
          kind,
          entries: new Array(capacity),
          hits: 0,
          misses: 0,
        })),
      );
      const unique = new Set();
      let decodes = 0;
      const original = TextDecoder.prototype.decode;
      let output;
      TextDecoder.prototype.decode = function (bytes, options) {
        if (bytes?.buffer === data.buffer) {
          assert.equal(bytes.byteLength, width);
          const key = (bytes.byteOffset - data.byteOffset) / width;
          unique.add(key);
          decodes++;
          for (const state of direct) {
            const slot =
              state.kind === "modulo"
                ? key % state.capacity
                : state.capacity === 1
                  ? 0
                  : Math.imul(key, 0x9e3779b1) >>> (32 - Math.log2(state.capacity));
            if (state.entries[slot] === key) state.hits++;
            else state.misses++;
            state.entries[slot] = key;
          }
          for (const state of simulations) {
            if (state.entries.has(key)) {
              state.hits++;
              state.entries.delete(key);
            } else state.misses++;
            state.entries.set(key, true);
            if (state.entries.size > state.capacity)
              state.entries.delete(state.entries.keys().next().value);
          }
        }
        return original.call(this, bytes, options);
      };
      try {
        output = vectorTopKOwnedBatch(
          input,
          [{ column: "key", nulls: "last" }],
          { limit },
          selection,
        );
      } finally {
        TextDecoder.prototype.decode = original;
      }
      try {
        assert.deepEqual(materializeBatchRows(output.value), expected);
      } finally {
        output.release();
        scope.dispose();
      }
      assert.equal(pool.usedBytes, 0);
      for (const state of [...simulations, ...direct])
        assert.equal(state.hits + state.misses, decodes);
      cases.push({
        cardinality,
        stride,
        limit,
        rows,
        selectedRows: Math.ceil(rows / stride),
        decodes,
        uniqueDecodedEntries: unique.size,
        simulations: simulations.map(({ entries: _, ...state }) => state),
        directMappedSimulations: direct.map(({ entries: _, ...state }) => state),
        finalAdmittedBytes: pool.usedBytes,
      });
    }
  }
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      scope:
        "Diagnostic trace of current synchronous top-k packed dictionary decoding. Simulated LRU hit counts are not timing, memory admission, or a production cache policy. Instrumentation/selection/oracle allocations excluded from modeled pool; all outputs verified against independent stable sorting. No cloud claim.",
      rows,
      width,
      cases,
    },
    null,
    2,
  ),
);
