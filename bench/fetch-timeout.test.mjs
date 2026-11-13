import assert from "node:assert/strict";
import { createServer } from "node:http";
import { test } from "node:test";
import { fetchWithTimeout } from "./bench-utils.mjs";

test("benchmark timeout preserves caller cancellation and Request signal precedence", async () => {
  let received;
  const server = createServer((request, response) => {
    if (request.url === "/ok") response.end("ok");
    else received?.();
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    for (const requestSignal of [false, true]) {
      const controller = new AbortController();
      const reason = new Error("caller stopped read");
      const arrival = new Promise((resolve) => {
        received = resolve;
      });
      const pending = requestSignal
        ? fetchWithTimeout(
            new Request(`${base}/wait`, { signal: controller.signal }),
            undefined,
            10000,
          )
        : fetchWithTimeout(`${base}/wait`, { signal: controller.signal }, 10000);
      const rejection = assert.rejects(pending, (error) => error === reason);
      await arrival;
      controller.abort(reason);
      await rejection;
    }
    await assert.rejects(fetchWithTimeout(`${base}/wait`, undefined, 10), { name: "TimeoutError" });
    const aborted = new AbortController();
    aborted.abort();
    const response = await fetchWithTimeout(new Request(`${base}/ok`, { signal: aborted.signal }), {
      signal: null,
    });
    assert.equal(await response.text(), "ok");
  } finally {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
});
