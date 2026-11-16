import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

// Verify the byte representation used by browsers, not just HEAD or Accept-Ranges.
export async function checkHttpRanges(url, expected) {
  for (const [start, end] of [
    [0, 1],
    [Math.floor(expected.length / 2), Math.floor(expected.length / 2) + 7],
    [expected.length - 8, expected.length - 1],
  ]) {
    const response = await fetch(url, {
      headers: { Range: `bytes=${start}-${end}`, Origin: "https://lakeql.com" },
      signal: AbortSignal.timeout(30_000),
    });
    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    try {
      assert.equal(response.status, 206, `${url}: range status`);
      assert.equal(
        response.headers.get("content-range"),
        `bytes ${start}-${end}/${expected.length}`,
        `${url}: Content-Range must describe the actual file`,
      );
      const encoding = response.headers.get("content-encoding");
      assert.ok(!encoding || encoding === "identity", `${url}: ranges must not be HTTP-compressed`);
      const origin = response.headers.get("access-control-allow-origin");
      assert.ok(origin === "*" || origin === "https://lakeql.com", `${url}: browser CORS origin`);
      const exposed = response.headers.get("access-control-expose-headers")?.toLowerCase() ?? "";
      assert.ok(
        exposed === "*" || exposed.split(",").some((name) => name.trim() === "content-range"),
        `${url}: browsers must be able to read Content-Range`,
      );
      // A broken server may ignore Range and send the entire object. Never buffer that response.
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        assert.ok(size <= end - start + 1, `${url}: oversized range body`);
        chunks.push(value);
      }
    } finally {
      await reader.cancel();
    }
    assert.deepEqual(
      Buffer.concat(chunks),
      expected.subarray(start, end + 1),
      `${url}: range bytes`,
    );
  }
  console.log(`verified browser ranges: ${url} (${expected.length} bytes)`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [, , url, fixture] = process.argv;
  assert.ok(url && fixture, "usage: node scripts/check-http-ranges.mjs URL FIXTURE");
  await checkHttpRanges(url, await readFile(fixture));
}
