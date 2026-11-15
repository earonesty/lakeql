import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const baseline = process.env.LAKEQL_COMPARE_BASELINE ?? "c6a848a";
const moduleName = process.env.LAKEQL_COMPARE_MODULE ?? "physical";
const moduleNames = moduleName.split(",");
if (moduleNames.some((name) => !/^(?:(?:core|parquet|http)\/)?[a-z-]+$/.test(name)))
  throw new Error("Invalid comparison module");
const modules = await Promise.all(
  moduleNames.map(async (name) => {
    const [packageName, sourceName] = name.includes("/") ? name.split("/") : ["core", name];
    const source = execFileSync(
      "git",
      ["show", `${baseline}:packages/${packageName}/src/${sourceName}.ts`],
      {
        encoding: "utf8",
      },
    );
    const target = new URL(`../packages/${packageName}/dist/${sourceName}.js`, import.meta.url);
    return {
      name,
      target: target.href,
      compiled: ts.transpileModule(source, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
      }).outputText,
      baselineSourceSha256: createHash("sha256").update(source).digest("hex"),
      currentSourceSha256: createHash("sha256")
        .update(await readFile(target))
        .digest("hex"),
    };
  }),
);
const directory = await mkdtemp(join(tmpdir(), "lakeql-identity-compare-"));
try {
  const hook = join(directory, "hook.mjs");
  await writeFile(
    hook,
    `import { registerHooks } from "node:module";
const sources = new Map(${JSON.stringify(modules.map((entry) => [entry.target, entry.compiled]))});
registerHooks({load(url, context, next) { return sources.has(url) ? {format:"module", source:sources.get(url), shortCircuit:true} : next(url, context); }});
`,
  );
  const benchmarkName = process.env.LAKEQL_COMPARE_BENCHMARK ?? "page-count-query.mjs";
  if (!/^[a-z-]+\.mjs$/.test(benchmarkName)) throw new Error("Invalid benchmark name");
  const benchmark = fileURLToPath(new URL(`./${benchmarkName}`, import.meta.url));
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
        benchmarkName,
        moduleName,
        modules: modules.map(({ name, baselineSourceSha256, currentSourceSha256 }) => ({
          name,
          baselineSourceSha256,
          currentSourceSha256,
        })),
        node: process.version,
        warmupPairs: 1,
        measuredPairs: 4,
        scope:
          "Alternating fresh Node processes; the named modules are replaced together by their prior implementations, with current dependencies. The recorded benchmark identifies its fixture and checks; these are local queries, not cloud throughput. Source store/runtime overhead excluded from pool metrics. Value parity is covered by external conformance; benchmark checks count and release.",
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
