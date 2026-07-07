import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(new URL("..", import.meta.url).pathname);
const base = process.argv[2] ?? process.env.GITHUB_BASE_REF ?? "main";
const baseRef = base.startsWith("origin/") ? base : `origin/${base}`;

const changedFiles = gitChangedFiles(baseRef);
const packageChanges = changedFiles.filter(
  (file) => file.startsWith("packages/") && !file.includes("/dist/"),
);

if (packageChanges.length === 0) {
  console.log("No package changes detected; changeset not required.");
  process.exit(0);
}

if (hasChangeset()) {
  console.log("Package changes detected and a changeset is present.");
  process.exit(0);
}

if (hasVersionedRelease(packageChanges, baseRef)) {
  console.log("Package changes detected and package version/changelog updates are present.");
  process.exit(0);
}

console.error("Package changes require a Changeset.");
console.error("Changed package files:");
for (const file of packageChanges) console.error(`  ${file}`);
console.error("Run `pnpm changeset` and commit the generated .changeset/*.md file.");
process.exit(1);

function gitChangedFiles(baseRef) {
  const mergeBase = git(["merge-base", "HEAD", baseRef], { allowFailure: true }).trim();
  const diffBase = mergeBase || baseRef;
  return git(["diff", "--name-only", diffBase, "HEAD"])
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
}

function hasChangeset() {
  const dir = resolve(root, ".changeset");
  if (!existsSync(dir)) return false;
  return readdirSync(dir).some((file) => file.endsWith(".md") && file !== "README.md");
}

function hasVersionedRelease(packageChanges, baseRef) {
  const packages = changedPackageNames(packageChanges);
  if (packages.length === 0) return false;
  return packages.every((packageName) => packageVersionChanged(packageName, baseRef));
}

function changedPackageNames(packageChanges) {
  return [
    ...new Set(
      packageChanges
        .map((file) => /^packages\/([^/]+)\//u.exec(file)?.[1])
        .filter((packageName) => packageName !== undefined),
    ),
  ];
}

function packageVersionChanged(packageName, baseRef) {
  const packageJsonPath = `packages/${packageName}/package.json`;
  const changelogPath = `packages/${packageName}/CHANGELOG.md`;
  if (!existsSync(resolve(root, packageJsonPath)) || !existsSync(resolve(root, changelogPath))) {
    return false;
  }
  const before = git(["show", `${baseRef}:${packageJsonPath}`], { allowFailure: true });
  if (before.length === 0) return false;
  const previousVersion = JSON.parse(before).version;
  const currentVersion = JSON.parse(readFileSync(resolve(root, packageJsonPath), "utf8")).version;
  if (typeof previousVersion !== "string" || typeof currentVersion !== "string") return false;
  if (previousVersion === currentVersion) return false;
  const changedFiles = new Set(packageChanges);
  return changedFiles.has(packageJsonPath) && changedFiles.has(changelogPath);
}

function git(args, options = {}) {
  const result = spawnSync("git", args, {
    cwd: root,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (result.status !== 0 && options.allowFailure !== true) {
    throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
  }
  return result.stdout;
}
