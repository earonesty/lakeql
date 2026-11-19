import assert from "node:assert/strict";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import test from "node:test";
import { checkHttpRanges } from "./check-http-ranges.mjs";

const bytes = await readFile(new URL("../site/public/sales.parquet", import.meta.url));

for (const scenario of [
  "valid",
  "compressed-size",
  "wrong-bytes",
  "ignored-range",
  "hidden-size",
]) {
  test(`browser range contract: ${scenario}`, async (t) => {
    const server = createServer((request, response) => {
      const [, startText, endText] = /^bytes=(\d+)-(\d+)$/.exec(request.headers.range);
      const start = Number(startText);
      const end = Number(endText);
      response.setHeader("Access-Control-Allow-Origin", "https://lakeql.com");
      if (scenario !== "hidden-size") {
        response.setHeader("Access-Control-Expose-Headers", "Content-Range");
      }
      response.setHeader(
        "Content-Range",
        `bytes ${start}-${end}/${scenario === "compressed-size" ? 1330 : bytes.length}`,
      );
      response.statusCode = scenario === "ignored-range" ? 200 : 206;
      if (scenario === "ignored-range") response.end(bytes);
      else if (scenario === "compressed-size") response.end();
      else if (scenario === "wrong-bytes") response.end(Buffer.alloc(end - start + 1));
      else response.end(bytes.subarray(start, end + 1));
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    t.after(() => {
      server.closeAllConnections();
      server.close();
    });
    const url = `http://127.0.0.1:${server.address().port}/sales.parquet`;
    if (scenario === "valid") await checkHttpRanges(url, bytes);
    else {
      const errors = {
        "compressed-size": /Content-Range must describe the actual file/,
        "wrong-bytes": /range bytes/,
        "ignored-range": /range status/,
        "hidden-size": /browsers must be able to read Content-Range/,
      };
      await assert.rejects(checkHttpRanges(url, bytes), errors[scenario]);
    }
  });
}
