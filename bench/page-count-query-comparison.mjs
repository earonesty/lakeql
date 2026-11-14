import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const baseline = "c6a848a";
const source = execFileSync("git", ["show", `${baseline}:packages/core/src/physical.ts`], {
  encoding: "utf8",
});
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
}).outputText;
const directory = await mkdtemp(join(tmpdir(), "lakeql-identity-compare-"));
try {
  const compiledPath = join(directory, "physical.mjs");
  await writeFile(compiledPath, compiled);
  const hook = join(directory, "hook.mjs");
  const target = new URL("../packages/core/dist/physical.js", import.meta.url);
  await writeFile(
    hook,
    `import { registerHooks } from "node:module";
import { readFileSync } from "node:fs";
const source = readFileSync(${JSON.stringify(compiledPath)}, "utf8");
registerHooks({load(url, context, next) { return url === ${JSON.stringify(target.href)} ? {format:"module", source, shortCircuit:true} : next(url, context); }});
`,
  );
  const benchmark = fileURLToPath(new URL("./page-count-query.mjs", import.meta.url));
  const runs = { prior: [], current: [] };
  for (let round = 0; round < 5; round++) {
    for (const variant of round % 2 ? ["current", "prior"] : ["prior", "current"]) {
      const args = variant === "prior" ? ["--import", hook, benchmark] : [benchmark];
      const result = JSON.parse(execFileSync(process.execPath, args, { encoding: "utf8" }));
      if (round > 0) runs[variant].push(result);
    }
  }
  const median = (values) => {
    values.sort((a, b) => a - b);
    return (values[1] + values[2]) / 2;
  };
  console.log(
    JSON.stringify(
      {
        baseline,
        baselineSourceSha256: createHash("sha256").update(source).digest("hex"),
        currentSourceSha256: createHash("sha256")
          .update(await readFile(target))
          .digest("hex"),
        node: process.version,
        warmupPairs: 1,
        measuredPairs: 4,
        scope:
          "Alternating fresh Node processes; only physical.ts is replaced by the prior implementation, with current dependencies. Local 40,000-row many-page fixture query, not cloud throughput. Source store/runtime overhead excluded from pool metrics. Value parity is covered by external conformance; benchmark checks count and release.",
        priorMedianMs: median(runs.prior.map((r) => r.elapsedMs)),
        currentMedianMs: median(runs.current.map((r) => r.elapsedMs)),
        runs,
      },
      null,
      2,
    ),
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
