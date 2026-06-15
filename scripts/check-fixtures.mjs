import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { relative, resolve } from "node:path";

const root = resolve(new URL("..", import.meta.url).pathname);
const fixtureData = resolve(root, "fixtures/data");

const before = snapshot(fixtureData);
const result = spawnSync("pnpm", ["fixtures"], {
  cwd: root,
  stdio: "inherit",
  shell: process.platform === "win32",
});
if (result.status !== 0) process.exit(result.status ?? 1);

const after = snapshot(fixtureData);
const diff = diffSnapshots(before, after);
if (diff.length > 0) {
  console.error("Fixture generation is not idempotent. Changed files:");
  for (const entry of diff) console.error(`  ${entry}`);
  process.exit(1);
}

function snapshot(dir) {
  const out = new Map();
  if (!existsSync(dir)) return out;
  for (const file of walk(dir)) {
    out.set(relative(dir, file), hashFile(file));
  }
  return out;
}

function* walk(dir) {
  for (const entry of readdirSync(dir).sort()) {
    const path = resolve(dir, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      yield* walk(path);
    } else if (stat.isFile()) {
      yield path;
    }
  }
}

function hashFile(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function diffSnapshots(before, after) {
  const paths = new Set([...before.keys(), ...after.keys()]);
  return [...paths].sort().filter((path) => before.get(path) !== after.get(path));
}
