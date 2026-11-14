import { performance } from "node:perf_hooks";
import {
  concatBatches,
  concatStructRouting,
  MemoryBudget,
  MemoryScope,
  vectorValue,
} from "../packages/core/dist/index.js";

const results = [];
for (const shape of ["binary", "list"]) {
  for (const payloadLength of [32, 4096]) {
    const count = 512,
      width = 1;
    const vectors = Array.from({ length: count }, (_, row) => ({
      type: "struct",
      length: 1,
      fields: Object.fromEntries(
        Array.from({ length: width }, (_, column) => [
          `c${column}`,
          shape === "binary"
            ? {
                type: "binary-view",
                data: new Uint8Array(payloadLength).fill(row % 251),
                fixedLength: payloadLength,
                length: 1,
              }
            : {
                type: "list",
                offsets: Int32Array.of(0, payloadLength),
                child: { type: "i32", values: new Int32Array(payloadLength).fill(row + column) },
              },
        ]),
      ),
    }));
    const batches = vectors.map((vector) => ({ rowCount: 1, columns: vector.fields }));
    const times = { routing: [], copying: [] };
    const memory = {};
    for (let round = 0; round < 16; round++) {
      for (const kind of round % 2 ? ["copying", "routing"] : ["routing", "copying"]) {
        const scope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
        const start = performance.now();
        const output =
          kind === "routing"
            ? concatStructRouting(vectors, scope).fields
            : concatBatches(batches, scope).columns;
        const elapsed = performance.now() - start;
        memory[kind] = { retainedBytes: scope.budget.usedBytes, peakBytes: scope.budget.peakBytes };
        for (let column = 0; column < width; column++) {
          const vector = output[`c${column}`];
          for (let row = 0; row < count; row++) {
            const value = vectorValue(vector, row);
            if (value.length !== payloadLength) throw Error("length mismatch");
            for (const item of value)
              if (item !== (shape === "binary" ? row % 251 : row + column))
                throw Error("value mismatch");
          }
        }
        scope.dispose();
        if (scope.budget.usedBytes !== 0) throw Error("leaked allocation");
        if (round >= 4) times[kind].push(elapsed);
      }
    }
    const timing = Object.fromEntries(
      Object.entries(times).map(([kind, samples]) => {
        const sorted = [...samples].sort((a, b) => a - b);
        return [
          kind,
          {
            medianMs: (sorted[5] + sorted[6]) / 2,
            minMs: sorted[0],
            maxMs: sorted.at(-1),
            samplesMs: samples,
          },
        ];
      }),
    );
    results.push({ shape, payloadLength, rows: count, fields: width, timing, memory });
  }
}
console.log(
  JSON.stringify(
    {
      description:
        "Local prebuilt one-row binary/list structs; 4 warmups and 12 alternating samples. Inputs excluded from tracked charges; values verified outside timing; no total heap claim.",
      results,
    },
    null,
    2,
  ),
);
