import cp from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";

const root = process.cwd(),
  dest = fs.mkdtempSync("/tmp/lakeql-parquet-baseline-");
const commit = cp.execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
for (const name of ["core", "parquet"]) {
  const dir = path.join(dest, "packages", name);
  fs.mkdirSync(path.join(dir, "dist"), { recursive: true });
  fs.mkdirSync(path.join(dir, "node_modules"), { recursive: true });
  const files = cp
    .execFileSync("git", ["ls-tree", "-r", "--name-only", commit, `packages/${name}/src`], {
      encoding: "utf8",
    })
    .trim()
    .split("\n")
    .filter((f) => f.endsWith(".ts") && !f.endsWith(".test.ts") && !f.endsWith(".workerd.ts"));
  fs.writeFileSync(
    path.join(dir, "package.json"),
    cp.execFileSync("git", ["show", `${commit}:packages/${name}/package.json`]),
  );
  for (const file of files) {
    const source = cp.execFileSync("git", ["show", `${commit}:${file}`], { encoding: "utf8" });
    const output = path.join(dir, "dist", file.split("/src/")[1].replace(/\.ts$/, ".js"));
    fs.mkdirSync(path.dirname(output), { recursive: true });
    fs.writeFileSync(
      output,
      ts.transpileModule(source, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
      }).outputText,
    );
  }
  for (const dep of fs.readdirSync(path.join(root, "packages", name, "node_modules"))) {
    if (dep.startsWith(".")) continue;
    const target =
      dep === "lakeql-core"
        ? path.join(dest, "packages/core")
        : fs.realpathSync(path.join(root, "packages", name, "node_modules", dep));
    fs.symlinkSync(target, path.join(dir, "node_modules", dep));
  }
}

const corpus = path.join(root, "fixtures/external/parquet-testing");
function files(dir) {
  return fs
    .readdirSync(dir)
    .sort()
    .flatMap((name) => {
      const file = path.join(dir, name);
      return fs.statSync(file).isDirectory()
        ? files(file)
        : file.endsWith(".parquet")
          ? [file]
          : [];
    });
}
function failure(error) {
  const cause = error?.cause ?? error?.details?.cause;
  return {
    code: error?.code ?? error?.name,
    message: String(error?.message ?? error),
    ...(cause === undefined ? {} : { cause: failure(cause) }),
  };
}
function digest(rows, binaryAsText = false) {
  return createHash("sha256")
    .update(
      JSON.stringify(rows, (_key, value) =>
        typeof value === "bigint"
          ? { bigint: String(value) }
          : value instanceof Uint8Array
            ? binaryAsText
              ? new TextDecoder().decode(value)
              : { bytes: Array.from(value) }
            : value,
      ),
    )
    .digest("hex");
}
try {
  const baseline = await import(pathToFileURL(path.join(dest, "packages/parquet/dist/index.js")));
  const baselineCore = await import(pathToFileURL(path.join(dest, "packages/core/dist/index.js")));
  const current = await import(pathToFileURL(path.join(root, "packages/parquet/dist/index.js")));
  const currentCore = await import(pathToFileURL(path.join(root, "packages/core/dist/index.js")));
  async function read(api, core, file) {
    const store = core.memoryStore(),
      key = path.relative(corpus, file);
    await store.put(key, fs.readFileSync(file));
    try {
      const rows = await api.readParquetObjects(store, key);
      return {
        status: "decoded",
        rows: rows.length,
        digest: digest(rows),
        binaryAsTextDigest: digest(rows, true),
      };
    } catch (error) {
      return { status: "rejected", error: failure(error) };
    }
  }
  const results = [];
  for (const file of files(corpus)) {
    const before = await read(baseline, baselineCore, file),
      after = await read(current, currentCore, file);
    results.push({
      file: path.relative(corpus, file),
      sha256: createHash("sha256").update(fs.readFileSync(file)).digest("hex"),
      baseline: before,
      current: after,
    });
  }
  const summary = {
    files: results.length,
    baselineDecoded: results.filter((r) => r.baseline.status === "decoded").length,
    currentDecoded: results.filter((r) => r.current.status === "decoded").length,
    newRejections: results
      .filter((r) => r.baseline.status === "decoded" && r.current.status === "rejected")
      .map((r) => r.file),
    newDecodes: results
      .filter((r) => r.baseline.status === "rejected" && r.current.status === "decoded")
      .map((r) => r.file),
    changedBeyondBinaryTextRepresentation: results
      .filter(
        (r) =>
          r.baseline.status === "decoded" &&
          r.current.status === "decoded" &&
          r.baseline.binaryAsTextDigest !== r.current.binaryAsTextDigest,
      )
      .map((r) => r.file),
    changedDecodedValues: results
      .filter(
        (r) =>
          r.baseline.status === "decoded" &&
          r.current.status === "decoded" &&
          r.baseline.digest !== r.current.digest,
      )
      .map((r) => r.file),
  };
  const report = {
    baselineCommit: commit,
    lockfileSha256: createHash("sha256")
      .update(fs.readFileSync(path.join(root, "pnpm-lock.yaml")))
      .digest("hex"),
    dependencyPolicy:
      "Both versions use the current installed dependencies; only core/parquet source differs.",
    summary,
    results,
  };
  fs.writeFileSync(
    path.join(root, "docs/validation/parquet-conformance-baseline.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  console.log(JSON.stringify(summary, null, 2));
} finally {
  fs.rmSync(dest, { recursive: true, force: true });
}
