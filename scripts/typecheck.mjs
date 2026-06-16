import { spawnSync } from "node:child_process";

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
  "fixtures",
];

for (const project of projects) {
  const result = spawnSync("pnpm", ["exec", "tsc", "-p", project, "--noEmit"], {
    stdio: "inherit",
    shell: process.platform === "win32",
  });
  if (result.status !== 0) process.exit(result.status ?? 1);
}
