import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { checkHttpRanges } from "../scripts/check-http-ranges.mjs";
import { sampleBytes, sampleKey, samplePath, sampleUrl } from "./playground-sample.mjs";

if (process.env.CLOUDFLARE_API_TOKEN) {
  const result = spawnSync(
    "pnpm",
    [
      "dlx",
      "wrangler@4.100.0",
      "r2",
      "object",
      "put",
      `lakeql-public/${sampleKey}`,
      "--file",
      fileURLToPath(samplePath),
      "--content-type",
      "application/octet-stream",
      "--cache-control",
      "public, max-age=31536000, immutable, no-transform",
      "--remote",
    ],
    { stdio: "inherit" },
  );
  assert.equal(result.status, 0, "could not publish playground sample");
}
// Also validate when uploads are unavailable: never deploy a UI pointing to a missing fixture.
await checkHttpRanges(sampleUrl, sampleBytes);
