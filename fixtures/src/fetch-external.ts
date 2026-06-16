import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  cpSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const parquetTestingRepo = "https://github.com/apache/parquet-testing.git";
const parquetTestingBranch = "master";
const externalRoot = fileURLToPath(new URL("../external/", import.meta.url));
const parquetTestingDest = join(externalRoot, "parquet-testing");
const icebergReferenceDir = join(externalRoot, "iceberg-reference");
const checksumPath = join(externalRoot, "CHECKSUMS.txt");
const dryRun = process.argv.includes("--dry-run");
const force = process.argv.includes("--force");
const verifyOnly = process.argv.includes("--verify-only");
const updateChecksums = process.argv.includes("--update-checksums");

if (dryRun) {
  console.log(
    `would fetch ${parquetTestingRepo}#${parquetTestingBranch} into ${parquetTestingDest}`,
  );
  console.log(`would verify vendored Iceberg checksums from ${checksumPath}`);
  process.exit(0);
}

if (verifyOnly) {
  verifyVendoredIcebergChecksums();
  process.exit(0);
}

if (updateChecksums) {
  updateVendoredIcebergChecksums();
  verifyVendoredIcebergChecksums();
  process.exit(0);
}

if (existsSync(parquetTestingDest) && !force) {
  console.log(`external parquet-testing fixtures already exist at ${parquetTestingDest}`);
  verifyVendoredIcebergChecksums();
  process.exit(0);
}

const tempRoot = fileURLToPath(new URL(`laql-external-${process.pid}/`, `file://${tmpdir()}/`));
const checkout = join(tempRoot, "parquet-testing");

rmSync(tempRoot, { recursive: true, force: true });
mkdirSync(tempRoot, { recursive: true });

try {
  run("git", [
    "clone",
    "--depth",
    "1",
    "--branch",
    parquetTestingBranch,
    parquetTestingRepo,
    checkout,
  ]);
  rmSync(join(checkout, ".git"), { recursive: true, force: true });
  mkdirSync(dirname(parquetTestingDest), { recursive: true });
  rmSync(parquetTestingDest, { recursive: true, force: true });
  cpSync(checkout, parquetTestingDest, { recursive: true });
  console.log(`fetched parquet-testing fixtures into ${parquetTestingDest}`);
  verifyVendoredIcebergChecksums();
} finally {
  rmSync(tempRoot, { recursive: true, force: true });
}

export function verifyVendoredIcebergChecksums(): void {
  const expected = readChecksumManifest();
  if (!existsSync(icebergReferenceDir)) {
    if (expected.size > 0) {
      throw new Error(
        `Iceberg checksum manifest has entries but ${icebergReferenceDir} is missing`,
      );
    }
    console.log("no vendored Iceberg reference fixtures to verify");
    return;
  }

  const actualFiles = listFiles(icebergReferenceDir).map((path) => externalRelativePath(path));
  if (expected.size === 0 && actualFiles.length > 0) {
    throw new Error(
      `Iceberg reference fixtures exist under ${icebergReferenceDir} but ${checksumPath} has no entries`,
    );
  }

  const actual = new Set(actualFiles);
  for (const [path, checksum] of expected) {
    if (!path.startsWith("iceberg-reference/")) {
      throw new Error(`External checksum path must be under iceberg-reference/: ${path}`);
    }
    const absolutePath = resolveExternalPath(path);
    if (!actual.has(path) || !existsSync(absolutePath)) {
      throw new Error(`Missing vendored Iceberg fixture listed in checksum manifest: ${path}`);
    }
    const actualChecksum = sha256File(absolutePath);
    if (actualChecksum !== checksum) {
      throw new Error(`Checksum mismatch for ${path}: expected ${checksum}, got ${actualChecksum}`);
    }
  }

  for (const path of actual) {
    if (!expected.has(path)) {
      throw new Error(`Vendored Iceberg fixture is missing from checksum manifest: ${path}`);
    }
  }

  console.log(`verified ${expected.size} vendored Iceberg fixture checksums`);
}

export function updateVendoredIcebergChecksums(): void {
  mkdirSync(externalRoot, { recursive: true });
  const files = existsSync(icebergReferenceDir)
    ? listFiles(icebergReferenceDir).map((path) => externalRelativePath(path))
    : [];
  const lines = [
    "# SHA-256 checksums for vendored external Iceberg reference fixtures.",
    "# Regenerate with: pnpm fixtures:external -- --update-checksums",
    ...files.map((path) => `${sha256File(resolveExternalPath(path))}  ${path}`),
  ];
  writeFileSync(checksumPath, `${lines.join("\n")}\n`);
  console.log(`wrote ${files.length} Iceberg fixture checksums to ${checksumPath}`);
}

function readChecksumManifest(): Map<string, string> {
  if (!existsSync(checksumPath)) return new Map();
  const checksums = new Map<string, string>();
  const body = readFileSync(checksumPath, "utf8");
  for (const [lineNumber, line] of body.split(/\r?\n/u).entries()) {
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) continue;
    const match = /^([a-f0-9]{64})\s+(.+)$/u.exec(trimmed);
    if (!match) {
      throw new Error(`Invalid checksum manifest line ${lineNumber + 1}: ${line}`);
    }
    const checksum = match[1] as string;
    const path = normalizeManifestPath(match[2] as string);
    if (checksums.has(path)) {
      throw new Error(`Duplicate checksum manifest path: ${path}`);
    }
    checksums.set(path, checksum);
  }
  return checksums;
}

function normalizeManifestPath(path: string): string {
  const normalized = path.replaceAll("\\", "/").replace(/^\.?\//u, "");
  if (normalized === "" || normalized.startsWith("../") || normalized.includes("/../")) {
    throw new Error(`Invalid checksum manifest path: ${path}`);
  }
  return normalized;
}

function resolveExternalPath(path: string): string {
  const absolutePath = resolve(externalRoot, path);
  const relativePath = relative(externalRoot, absolutePath);
  if (relativePath.startsWith("..") || relativePath === "" || relativePath.includes(`..${sep}`)) {
    throw new Error(`Checksum manifest path escapes fixtures/external: ${path}`);
  }
  return absolutePath;
}

function externalRelativePath(path: string): string {
  return relative(externalRoot, path).split(sep).join("/");
}

function listFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir).sort()) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...listFiles(path));
    else out.push(path);
  }
  return out;
}

function sha256File(path: string): string {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function run(command: string, args: string[]): void {
  const result = spawnSync(command, args, { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with status ${result.status}`);
  }
}
