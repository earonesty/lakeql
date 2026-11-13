import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const directory = mkdtempSync(join(tmpdir(), "lakeql-union-bench-"));
const results = [];
try {
  for (const script of ["union-concat-scaling.mjs", "union-selection-scaling.mjs"]) {
    const samples = new Map();
    // Separate Node processes make startup/JIT treatment consistent across runs.
    // Discard the first complete run; correctness checks execute in every run.
    for (let run = 0; run < 8; run++) {
      const output = join(directory, "result.json");
      execFileSync(
        process.execPath,
        [fileURLToPath(new URL(script, import.meta.url)), pathToFileURL(output).href],
        { stdio: "pipe" },
      );
      const report = JSON.parse(readFileSync(output, "utf8"));
      if (run === 0) continue;
      for (const result of report.results) {
        for (const [mode, metrics] of Object.entries(result.modes)) {
          const key = `${result.appends}:${mode}`;
          const entry = samples.get(key) ?? {
            appends: result.appends,
            rows: result.rows,
            mode,
            runs: [],
          };
          entry.runs.push(metrics);
          samples.set(key, entry);
        }
      }
    }
    for (const entry of samples.values()) {
      for (const metric of ["retainedBytes", "peakBytes", "unionDepth"])
        if (new Set(entry.runs.map((run) => run[metric])).size !== 1)
          throw Error(`Non-deterministic ${metric}: ${script}/${entry.mode}/${entry.appends}`);
      const elapsed = entry.runs.map((run) => run.elapsedMs).sort((a, b) => a - b);
      results.push({
        script,
        ...entry,
        elapsedMs: {
          min: elapsed[0],
          median: elapsed[3],
          max: elapsed[6],
        },
      });
    }
  }
  writeFileSync(
    new URL(
      process.argv[2] ?? "../docs/benchmarks/union-ownership-repeated-2026-11-13.json",
      import.meta.url,
    ),
    `${JSON.stringify(
      {
        scope:
          "Seven measured fresh-process runs per script after one discarded run. Each child checks values and final zero ownership. Memory metrics are explicit accounting, not VM heap. Reported timing ranges are descriptive, not causal speedup evidence or confidence intervals. Local execution only; no cloud transport.",
        node: process.version,
        results,
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(directory, { recursive: true, force: true });
}
