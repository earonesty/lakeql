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
    join(consumerDirectory, "int96.parquet"),
    readFileSync(join(root, "fixtures/external/parquet-testing/data/int96_from_spark.parquet")),
  );
  for (const name of [
    "map_no_value.parquet",
    "repeated_primitive_no_list.parquet",
    "nation.dict-malformed.parquet",
  ])
    writeFileSync(
      join(consumerDirectory, name),
      readFileSync(join(root, "fixtures/external/parquet-testing/data", name)),
    );
  writeFileSync(
    join(consumerDirectory, "variant-binary.parquet"),
    readFileSync(join(root, "fixtures/external/parquet-testing/shredded_variant/case-030.parquet")),
  );
  writeFileSync(
    join(consumerDirectory, "variant-decimal.parquet"),
    readFileSync(join(root, "fixtures/external/parquet-testing/shredded_variant/case-070.parquet")),
  );

  writeFileSync(
    join(consumerDirectory, "shredded-decimal.parquet"),
    readFileSync(join(root, "fixtures/external/parquet-testing/shredded_variant/case-028.parquet")),
  );
  writeFileSync(
    join(consumerDirectory, "smoke.mjs"),
    `import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import {
  memoryStore,
  MemoryBudget,
  createParquetLake,
  readParquetObjects,
  timestampEpochForUnit,
  batchFromVectors,
  batchFromColumns,
  decimalFromParts,
  materializeBatchRows,
  MemoryScope,
  materializeOwnedBatchRows,
  gt,
  physicalInputFromBatch,
} from "lakeql";
import { SUPPORTED_LANCE_STORAGE_VERSION } from "lakeql-lance";
import { browserWebGpuRuntime } from "lakeql-webgpu/browser";
import { WebGpuPhysicalBackend } from "lakeql-webgpu";

// Real Spark fixture, tested after npm install outside workspace patch settings.
const store = memoryStore();
await store.put("int96.parquet", readFileSync(new URL("int96.parquet", import.meta.url)));
const rows = await readParquetObjects(store, "int96.parquet", { batchSize: 2 });
assert.deepEqual(rows.map(({ a }) => a === null ? null : timestampEpochForUnit(a, "nanos")),
  [1704141296123456000n, 1704070800000000000n, 253402225200000000000n,
   1735599600000000000n, null, 9089380393200000000000n]);
await store.put("map.parquet", readFileSync(new URL("map_no_value.parquet", import.meta.url)));
const maps = await readParquetObjects(store, "map.parquet", { batchSize: 1 });
assert.deepEqual(maps.map(({ my_map_no_v }) => my_map_no_v),
  [[1, 2, 3], [4, 5, 6], [7, 8, 9]].map(keys => Object.fromEntries(keys.map(key => [key, null]))));
await store.put("repeated.parquet", readFileSync(new URL("repeated_primitive_no_list.parquet", import.meta.url)));
const repeated = await readParquetObjects(store, "repeated.parquet", { batchSize: 1 });
assert.deepEqual(repeated.map(({ Int32_list }) => Int32_list), [[0, 1, 2, 3], [], [4], [5, 6, 7, 8]]);
// Recovery and stream ownership must survive bundling and a clean npm install.
await store.put("nation.parquet", readFileSync(new URL("nation.dict-malformed.parquet", import.meta.url)));
const recovered = await readParquetObjects(store, "nation.parquet", { batchSize: 1 });
const expectedKeys = Array.from({ length: 25 }, (_, nation_key) => ({ nation_key }));
assert.deepEqual(recovered.map(({ nation_key }) => ({ nation_key })), expectedKeys);
assert.equal(new TextDecoder().decode(recovered[0].name), "ALGERIA");
assert.equal(new TextDecoder().decode(recovered[24].name), "UNITED STATES");
const pool = new MemoryBudget(64 * 1024 * 1024);
const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
await store.put("variant-binary.parquet", readFileSync(new URL("variant-binary.parquet", import.meta.url)));
const binaryExpected = [{ id: 1, var: new Uint8Array([10, 11, 12, 13]) }];
assert.deepEqual(await readParquetObjects(store, "variant-binary.parquet"), binaryExpected);
const binaryOwned = await lake.path("variant-binary.parquet").batchSize(1).run().toOwnedArray();
assert.deepEqual(binaryOwned.value, binaryExpected);
binaryOwned.release();
assert.equal(pool.usedBytes, 0);
const json = await new Response(lake.path("nation.parquet").select(["nation_key"]).batchSize(1).streamJson()).text();
assert.equal(json, JSON.stringify(expectedKeys));
const ndjson = await new Response(lake.path("nation.parquet").select(["nation_key"]).batchSize(3).streamNdjson()).text();
assert.equal(ndjson, expectedKeys.map(row => JSON.stringify(row) + "\\n").join(""));
const csv = await new Response(lake.path("nation.parquet").select(["nation_key"]).batchSize(1).streamCsv()).text();
assert.equal(csv, "nation_key\\n" + expectedKeys.map(row => row.nation_key).join("\\n") + "\\n");
assert.equal(pool.usedBytes, 0);
const cancelled = lake.path("nation.parquet").batchSize(1).streamNdjson().getReader();
assert.equal((await cancelled.read()).done, false);
await cancelled.cancel();
assert.equal(pool.usedBytes, 0);
await store.put("variant-decimal.parquet", readFileSync(new URL("variant-decimal.parquet", import.meta.url)));
const decimalFixtureRows = await readParquetObjects(store, "variant-decimal.parquet");
assert.equal(String(decimalFixtureRows[0].var), "123456789.987654321");
const decimalFixtureOwned = await lake.path("variant-decimal.parquet").run().toOwnedArray();
assert.equal(String(decimalFixtureOwned.value[0].var), "123456789.987654321");
decimalFixtureOwned.release();
await store.put("shredded-decimal.parquet", readFileSync(new URL("shredded-decimal.parquet", import.meta.url)));
const shreddedDecimalFixtureRows = await readParquetObjects(store, "shredded-decimal.parquet");
assert.equal(String(shreddedDecimalFixtureRows[0].var), "9876543210.123456789");
const shreddedDecimalFixtureOwned = await lake.path("shredded-decimal.parquet")
  .where(gt("var", decimalFromParts(9876543210123456788n, 9))).run().toOwnedArray();
assert.equal(String(shreddedDecimalFixtureOwned.value[0].var), "9876543210.123456789");
shreddedDecimalFixtureOwned.release();
const mixedNumericOwned = await lake.path("shredded-decimal.parquet")
  .where(gt("var", 9876543210)).run().toOwnedArray();
assert.equal(String(mixedNumericOwned.value[0].var), "9876543210.123456789");
mixedNumericOwned.release();
assert.equal(pool.usedBytes, 0);
const exactDecimal = decimalFromParts(123456789987654321n, 9);
const decimalRows = materializeBatchRows(batchFromColumns({ amount: [exactDecimal, null] }));
assert.deepEqual(decimalRows, [{ amount: exactDecimal }, { amount: null }]);
assert.equal(JSON.stringify(decimalRows), '[{"amount":"123456789.987654321"},{"amount":null}]');
const decimalScope = new MemoryScope(pool);
const decimalInput = decimalScope.retainAllocations(batchFromColumns({ amount: [exactDecimal, null] }, decimalScope));
decimalScope.dispose();
const decimalOwned = materializeOwnedBatchRows(decimalInput);
decimalInput.release();
assert.equal(String(decimalOwned.value[0].amount), "123456789.987654321");
decimalOwned.release();
assert.equal(pool.usedBytes, 0);
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
