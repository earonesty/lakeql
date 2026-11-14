import { writeFileSync } from "node:fs";
import { MemoryBudget, MemoryScope } from "../packages/core/dist/index.js";
import { buildUnionRouting } from "../packages/core/dist/union-routing.js";

const results = [];
for (const rows of [1024, 8192, 65536]) {
  const modes = {};
  for (const split of [false, true]) {
    const times = [];
    let retainedBytes = 0;
    let peakBytes = 0;
    for (let round = 0; round < 16; round++) {
      const budget = new MemoryBudget(64 * 1024 * 1024);
      const scope = new MemoryScope(budget);
      const scratch = split ? scope.childScope() : undefined;
      const start = performance.now();
      const routing = buildUnionRouting(rows, 10, (row) => row % 10, scope, scratch);
      scratch?.dispose();
      const elapsed = performance.now() - start;
      if (round >= 4) times.push(elapsed);
      retainedBytes = budget.usedBytes;
      peakBytes = budget.peakBytes;
      for (let row = 0; row < rows; row++) {
        if (routing.tags[row] !== row % 10 || routing.offsets[row] !== Math.floor(row / 10))
          throw Error("Incorrect routing");
      }
      scope.dispose();
      if (budget.usedBytes !== 0) throw Error("Routing ownership leak");
    }
    times.sort((a, b) => a - b);
    modes[split ? "releasedConstructionScratch" : "retainedConstructionScratch"] = {
      retainedBytes,
      peakBytes,
      medianMs: (times[5] + times[6]) / 2,
    };
  }
  results.push({ rows, domains: 10, modes });
}
writeFileSync(
  new URL("../docs/benchmarks/union-routing-2026-11-13.json", import.meta.url),
  `${JSON.stringify(
    {
      scope:
        "Local routing construction only; explicit allocation accounting, not measured VM heap. Four warmups and twelve measured rounds per mode, not interleaved. Validate every tag/offset outside timing. Both modes use the current implementation; compare retaining versus releasing construction-only indices, not historical CPU performance. Child payloads and cloud transport excluded.",
      results,
    },
    null,
    2,
  )}\n`,
);
