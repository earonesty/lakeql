import assert from "node:assert/strict";
import { mapConcurrent } from "../packages/core/dist/concurrent-map.js";
import { ReadSemaphore } from "../packages/core/dist/read-semaphore.js";

const concurrency = 4;
for (const count of [10000, 100000]) {
  const input = Array.from({ length: count }, (_, index) => index);
  const expected = input.map((value) => value * 2);
  for (const mode of ["eager", "bounded"]) {
    const samples = [];
    let report;
    for (let trial = 0; trial < 8; trial++) {
      const semaphore = new ReadSemaphore(concurrency);
      let pending = 0;
      let peakPending = 0;
      let active = 0;
      let peakActive = 0;
      const read = async (value) => {
        pending++;
        peakPending = Math.max(peakPending, pending);
        const release = await semaphore.acquire();
        active++;
        peakActive = Math.max(peakActive, active);
        try {
          await Promise.resolve();
          return value * 2;
        } finally {
          active--;
          pending--;
          release();
        }
      };
      const start = performance.now();
      const output = await (mode === "eager"
        ? Promise.all(input.map(read))
        : mapConcurrent(input, concurrency, read));
      const elapsed = performance.now() - start;
      assert.deepEqual(output, expected);
      assert.equal(pending, 0);
      assert.equal(active, 0);
      assert.equal(peakActive, concurrency);
      assert.equal(peakPending, mode === "eager" ? count : concurrency);
      if (trial >= 3) samples.push(elapsed);
      report = { count, mode, concurrency, peakPending, peakActive };
    }
    samples.sort((a, b) => a - b);
    console.log(
      JSON.stringify({
        ...report,
        medianMs: samples[2],
        samples: samples.length,
        scope:
          "microtask transport simulation using real read semaphore; excludes network and decode; pending counts include active callbacks",
      }),
    );
  }
}
