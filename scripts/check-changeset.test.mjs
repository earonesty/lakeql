import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

for (const scenario of [
  "bundled-release",
  "missing-host-release",
  "missing-plugin-release",
  "plugin-release",
]) {
  test(`release guard: ${scenario}`, (t) => {
    const root = mkdtempSync(join(tmpdir(), "lakeql-release-guard-"));
    t.after(() => rmSync(root, { recursive: true, force: true }));
    const git = (...args) => {
      const result = spawnSync("git", args, { cwd: root, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
    };
    mkdirSync(join(root, "scripts"));
    copyFileSync(
      new URL("./check-changeset.mjs", import.meta.url),
      join(root, "scripts/check-changeset.mjs"),
    );
    function manifest(name, version, isPrivate = false) {
      mkdirSync(join(root, "packages", name), { recursive: true });
      writeFileSync(
        join(root, "packages", name, "package.json"),
        JSON.stringify({ name, version, private: isPrivate }),
      );
    }
    function release(name, version) {
      manifest(name, version);
      writeFileSync(
        join(root, "packages", name, "CHANGELOG.md"),
        `# ${version}\nRelease changes.\n`,
      );
    }
    manifest("core", "0.5.3", true);
    manifest("lakeql", "0.9.0");
    manifest("lance", "0.3.0");
    git("init", "--initial-branch=main", "--quiet");
    git("config", "user.name", "Release guard test");
    git("config", "user.email", "release-guard@example.invalid");
    git("add", ".");
    git("commit", "--quiet", "-m", "baseline");
    git("update-ref", "refs/remotes/origin/main", "HEAD");
    if (scenario !== "plugin-release")
      writeFileSync(join(root, "packages/core/index.ts"), "export const changed = true;\n");
    if (scenario !== "missing-host-release") release("lakeql", "0.9.1");
    if (scenario === "missing-plugin-release")
      writeFileSync(join(root, "packages/lance/index.ts"), "export const changed = true;\n");
    if (scenario === "plugin-release") release("lance", "0.3.1");
    git("add", ".");
    git("commit", "--quiet", "-m", "change");
    const result = spawnSync(process.execPath, ["scripts/check-changeset.mjs", "main"], {
      cwd: root,
      encoding: "utf8",
    });
    const expected = scenario.startsWith("missing-") ? 1 : 0;
    assert.equal(result.status, expected, result.stdout + result.stderr);
  });
}
