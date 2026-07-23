import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";

const root = new URL("../", import.meta.url);
const readme = readFileSync(new URL("packages/webgpu/README.md", root), "utf8");
const example = readFileSync(new URL("packages/webgpu/examples/browser.ts", root), "utf8").trim();
const readmeExample = readme.match(
  /<!-- source: examples\/browser\.ts -->\s*```ts\n([\s\S]*?)\n```/,
)?.[1];
if (readmeExample !== example) {
  console.error(
    "packages/webgpu/README.md must embed packages/webgpu/examples/browser.ts verbatim",
  );
  process.exit(1);
}

const projects = [
  "packages/core",
  "packages/parquet",
  "packages/iceberg",
  "packages/http",
  "packages/r2",
  "packages/s3",
  "packages/geo",
  "packages/sql",
  "packages/cli",
  "packages/lakeql",
  "packages/webgpu",
  "packages/webgpu/tsconfig.examples.json",
  "fixtures",
];

for (const project of projects) {
  const result = spawnSync("pnpm", ["exec", "tsc", "-p", project, "--noEmit"], {
    stdio: "inherit",
    shell: process.platform === "win32",
  });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

const workerdResult = spawnSync("pnpm", ["exec", "tsc", "-p", "tsconfig.workerd.json"], {
  stdio: "inherit",
  shell: process.platform === "win32",
});
if (workerdResult.status !== 0) process.exit(workerdResult.status ?? 1);
