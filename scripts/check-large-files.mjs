import { spawnSync } from "node:child_process";
import { stat } from "node:fs/promises";

const maxRegularBlobBytes = Number(process.env.LAKEQL_MAX_GIT_BLOB_BYTES ?? 1024 * 1024);

const files = git(["ls-files"]).stdout.trim().split("\n").filter(Boolean);
const failures = [];

for (const file of files) {
  const info = await stat(file).catch(() => undefined);
  if (!info?.isFile() || info.size <= maxRegularBlobBytes) continue;
  if (await isLfsPointer(file)) continue;
  failures.push({ file, bytes: info.size });
}

if (failures.length > 0) {
  console.error(`Tracked files larger than ${maxRegularBlobBytes} bytes must use Git LFS:`);
  for (const failure of failures) {
    console.error(`  ${failure.file} (${failure.bytes} bytes)`);
  }
  process.exit(1);
}

function git(args) {
  const result = spawnSync("git", args, { encoding: "utf8" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`git ${args.join(" ")} failed:\n${result.stderr}`);
  }
  return result;
}

async function isLfsPointer(file) {
  // Checkout hydrates LFS objects. Inspect the staged Git blob rather than
  // treating the hydrated payload as an oversized ordinary Git object.
  const object = `:${file}`;
  const size = Number(git(["cat-file", "-s", object]).stdout.trim());
  if (!Number.isSafeInteger(size) || size > 1024) return false;
  const pointer = git(["show", object]).stdout;
  return /^version https:\/\/git-lfs\.github\.com\/spec\/v1\noid sha256:[0-9a-f]{64}\nsize [0-9]+\n$/u.test(
    pointer,
  );
}
