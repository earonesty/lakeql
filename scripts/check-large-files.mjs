import { spawnSync } from "node:child_process";
import { readFile, stat } from "node:fs/promises";

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
  const head = await readFile(file, { encoding: "utf8", flag: "r" }).catch(() => "");
  return head.startsWith("version https://git-lfs.github.com/spec/v1\n");
}
