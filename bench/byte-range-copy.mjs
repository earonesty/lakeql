import assert from "node:assert/strict";

const median = (x) => x.sort((a, b) => a - b)[Math.floor(x.length / 2)];
const results = [];
for (const width of [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 65536]) {
  const count = Math.max(100, Math.min(100000, Math.floor(16000000 / Math.max(1, width))));
  const source = Uint8Array.from({ length: width + 17 }, (_, i) => i % 251),
    target = new Uint8Array(width + 13);
  const funcs = {
    native: () => target.set(source.subarray(7, 7 + width), 5),
    loop: () => {
      for (let j = 0; j < width; j++) target[5 + j] = source[7 + j];
    },
  };
  const samples = { native: [], loop: [] };
  for (let i = 0; i < 10; i++)
    for (const name of i % 2 ? ["loop", "native"] : ["native", "loop"]) {
      const start = performance.now();
      for (let n = 0; n < count; n++) funcs[name]();
      const elapsed = performance.now() - start;
      if (i >= 2) samples[name].push(elapsed);
      assert.deepEqual(target.subarray(5, 5 + width), source.subarray(7, 7 + width));
    }
  results.push({ width, count, nativeMs: median(samples.native), loopMs: median(samples.loop) });
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      warmups: 2,
      samples: 8,
      scope:
        "Local byte-copy microbenchmark; borrowed source and output buffers excluded. Alternating variants. No cloud or whole-query claim; no production threshold selected.",
      cases: results,
    },
    null,
    2,
  ),
);
