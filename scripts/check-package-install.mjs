import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const temporaryRoot = mkdtempSync(join(tmpdir(), "lakeql-package-install-"));
const packageDirectory = join(temporaryRoot, "packages");
const consumerDirectory = join(temporaryRoot, "consumer");
mkdirSync(packageDirectory);
mkdirSync(consumerDirectory);

try {
  const lakeqlTarball = pack("packages/lakeql");
  const lanceTarball = pack("packages/lance");
  const webGpuTarball = pack("packages/webgpu");

  writeFileSync(
    join(consumerDirectory, "package.json"),
    `${JSON.stringify({ name: "lakeql-install-smoke", private: true, type: "module" }, null, 2)}\n`,
  );
  writeFileSync(
    join(consumerDirectory, "smoke.mjs"),
    `import {
  batchFromVectors,
  gt,
  physicalInputFromBatch,
} from "lakeql";
import { SUPPORTED_LANCE_STORAGE_VERSION } from "lakeql-lance";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";

const batch = batchFromVectors({
  score: { type: "f32", values: Float32Array.of(0.25, 0.75) },
});
const fragment = {
  id: "published-package-contract",
  input: physicalInputFromBatch(batch),
  operators: [{ kind: "select", predicate: gt("score", 0.5) }],
  output: { kind: "selection" },
  estimates: {
    rowCount: 2,
    inputBytes: 8,
    outputBytes: 2,
    dispatchCount: 1,
  },
};
const backend = new WebGpuPhysicalBackend(() => {
  throw new Error("The install smoke test must not acquire a GPU device");
});
if (!backend.assess(fragment, {}).supported) {
  throw new Error("The installed WebGPU plugin rejected a supported LakeQL fragment");
}
backend.close();
if (typeof browserWebGpuRuntime !== "function") {
  throw new Error("The installed browser entrypoint is unavailable");
}
if (SUPPORTED_LANCE_STORAGE_VERSION !== "2.0") {
  throw new Error("The installed Lance entrypoint is unavailable");
}
`,
  );
  writeFileSync(
    join(consumerDirectory, "smoke.ts"),
    `import type { ObjectStore, PhysicalExecutionBackend } from "lakeql";
import type { OpenLanceDatasetOptions } from "lakeql-lance";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";
import type { BrowserWebGpuConstants, BrowserWebGpuSource } from "lakeql-webgpu/browser";

declare const store: ObjectStore;
const lanceOptions: OpenLanceDatasetOptions = { store, path: "dataset.lance" };
const backend: PhysicalExecutionBackend = new WebGpuPhysicalBackend(() => {
  throw new Error("The type smoke test must not acquire a GPU device");
});
const browserSource: BrowserWebGpuSource = {};
declare const browserConstants: BrowserWebGpuConstants;
void lanceOptions;
void backend;
void browserSource;
void browserConstants;
`,
  );

  run(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      "--no-package-lock",
      lakeqlTarball,
      lanceTarball,
      webGpuTarball,
    ],
    consumerDirectory,
  );
  run("node", ["smoke.mjs"], consumerDirectory);
  run(
    join(root, "node_modules/.bin/tsc"),
    [
      "--noEmit",
      "--strict",
      "--target",
      "ES2022",
      "--module",
      "ESNext",
      "--moduleResolution",
      "bundler",
      "--lib",
      "ES2023,DOM,DOM.Iterable",
      "smoke.ts",
    ],
    consumerDirectory,
  );

  for (const plugin of ["lakeql-lance", "lakeql-webgpu"]) {
    const installedManifest = JSON.parse(
      readFileSync(join(consumerDirectory, `node_modules/${plugin}/package.json`), "utf8"),
    );
    if (installedManifest.dependencies?.["lakeql-core"] !== undefined) {
      throw new Error(`${plugin} must not depend on the private lakeql-core workspace`);
    }
    if (installedManifest.peerDependencies?.lakeql === undefined) {
      throw new Error(`${plugin} must declare lakeql as its public host peer`);
    }
  }
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}

function pack(directory) {
  const output = execFileSync(
    "pnpm",
    [
      "--dir",
      directory,
      "--config.ignore-scripts=true",
      "pack",
      "--pack-destination",
      packageDirectory,
      "--json",
    ],
    {
      cwd: root,
      encoding: "utf8",
    },
  );
  const manifest = JSON.parse(output);
  if (typeof manifest.filename !== "string") {
    throw new Error(`pnpm pack did not return a tarball for ${directory}`);
  }
  return manifest.filename;
}

function run(command, args, cwd) {
  const environment = { ...process.env };
  for (const key of Object.keys(environment)) {
    if (
      [
        "npm_config__jsr_registry",
        "npm_config_npm_globalconfig",
        "npm_config_verify_deps_before_run",
      ].includes(key.toLowerCase())
    ) {
      delete environment[key];
    }
  }
  execFileSync(command, args, {
    cwd,
    env: environment,
    stdio: "inherit",
  });
}
