import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";

const root = fileURLToPath(new URL("../", import.meta.url));
const revision = process.env.LAKEQL_TOPK_BASELINE_REF ?? "384c4f5";
const reportPath = resolve(
  process.env.LAKEQL_KEY_CACHE_REPORT ?? "docs/benchmarks/top-k-reader-comparison-2026-11-14.json",
);
const directory = mkdtempSync(`${tmpdir()}/lakeql-reader-comparison-`);
const hashes = {};
try {
  for (const name of ["scalar-read-workspace", "vector-sort"]) {
    const source = execFileSync("git", ["show", `${revision}:packages/core/src/${name}.ts`], {
      cwd: root,
      encoding: "utf8",
    });
    hashes[name] = createHash("sha256").update(source).digest("hex");
    const rewritten = source.replace(
      /from "\.\/([^"]+)"/g,
      (_, file) =>
        `from "${pathToFileURL(file === "scalar-read-workspace.js" ? `${directory}/scalar-read-workspace.mjs` : `${root}/packages/core/dist/${file}`).href}"`,
    );
    writeFileSync(
      `${directory}/${name}.mjs`,
      ts.transpileModule(rewritten, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
      }).outputText,
    );
  }
  execFileSync(process.execPath, ["bench/top-k-key-cache-shapes.mjs"], {
    cwd: root,
    env: {
      ...process.env,
      LAKEQL_TOPK_COMPARE_MODULE: pathToFileURL(`${directory}/vector-sort.mjs`).href,
      LAKEQL_KEY_CACHE_REPORT: reportPath,
    },
    stdio: "inherit",
  });
  const report = JSON.parse(readFileSync(reportPath, "utf8"));
  report.comparison = {
    baselineRevision: execFileSync("git", ["rev-parse", revision], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
    baselineSourceHashes: hashes,
    candidateModuleHashes: Object.fromEntries(
      ["scalar-read-workspace", "vector-sort"].map((name) => [
        name,
        createHash("sha256")
          .update(readFileSync(`${root}/packages/core/dist/${name}.js`))
          .digest("hex"),
      ]),
    ),
    variants: {
      direct: "baseline reader and top-k implementation",
      scalarReuse: "current built reader and top-k implementation",
    },
    limitation:
      "Baseline reader/top-k modules use current unchanged dependencies; this is a two-module comparison, not a historical whole-revision build.",
  };
  writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
} finally {
  rmSync(directory, { recursive: true, force: true });
}
