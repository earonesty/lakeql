import { describe, expect, it } from "vitest";
import { eq } from "./expr.js";
import {
  assertBookmarkMatches,
  createBookmark,
  createOutputManifest,
  createTaskManifest,
  fingerprint,
  memoryCheckpointAdapter,
  signPaginationToken,
  stableStringify,
  verifyPaginationToken,
} from "./manifest.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type ScanAdapter, type ScanOptions } from "./query.js";
import type { Row } from "./types.js";

class EmptyScanner implements ScanAdapter {
  async *scan(_path: string, _options: ScanOptions): AsyncIterable<Row[]> {
    yield [];
  }
}

describe("task and output manifests", () => {
  it("builds deterministic task manifests from query plans", async () => {
    const store = memoryStore();
    await store.put("lake/country=US/b.parquet", new Uint8Array([1]));
    await store.put("lake/country=CA/a.parquet", new Uint8Array([1]));
    const lake = new Lake({
      store,
      scanner: new EmptyScanner(),
      queryId: () => "q_manifest",
    });

    const manifest = await lake
      .hive("lake/**/*.parquet")
      .select(["id"])
      .where(eq("country", "US"))
      .taskManifest("job_1");
    const repeated = await lake
      .hive("lake/**/*.parquet")
      .select(["id"])
      .where(eq("country", "US"))
      .taskManifest("job_1");

    expect(manifest).toEqual(repeated);
    expect(manifest).toMatchObject({
      version: 1,
      jobId: "job_1",
      tasks: [
        {
          outputRole: "rows",
          input: {
            path: "lake/country=US/b.parquet",
            partitionValues: { country: "US" },
            projectedColumns: ["country", "id"],
          },
        },
      ],
    });
    expect(manifest.tasks[0]?.id).toMatch(/^job_1-task-000000-/u);
    expect(manifest.planFingerprint).toMatch(/^fp_[0-9a-f]{16}$/u);
  });

  it("normalizes task and output manifest JSON for golden comparisons", () => {
    const taskManifest = createTaskManifest({
      jobId: "job_2",
      snapshot: "snapshot_2",
      outputRole: "data-file",
      tasks: [
        {
          path: "b.parquet",
          etag: "v2",
          rowGroupRanges: [
            { start: 10, end: 20 },
            { start: 0, end: 10 },
          ],
          projectedColumns: ["z", "a"],
          partitionValues: { country: "US", date: "2026-01-01" },
        },
      ],
    });
    const outputManifest = createOutputManifest({
      jobId: "job_2",
      planFingerprint: taskManifest.planFingerprint,
      entries: [
        {
          taskId: taskManifest.tasks[0]?.id ?? "",
          outputPath: "out/date=2026-01-01/file.parquet",
          partitionValues: { date: "2026-01-01", country: "US" },
          rowCount: 12,
          byteSize: 256,
          contentHash: "sha256:abc",
          etag: "out-v1",
          iceberg: {
            recordCount: 12,
            fileSizeInBytes: 256,
            partitionValues: { country: "US", date: "2026-01-01" },
          },
        },
      ],
    });

    expect(taskManifest.snapshot).toBe("snapshot_2");
    expect(taskManifest.tasks[0]?.outputRole).toBe("data-file");
    expect(stableStringify(taskManifest)).toContain('"projectedColumns":["a","z"]');
    expect(stableStringify(outputManifest)).toContain(
      '"partitionValues":{"country":"US","date":"2026-01-01"}',
    );
    expect(stableStringify(undefined)).toBe('"undefined"');
    expect(
      stableStringify({
        array: [1n, new Date("2026-01-01T00:00:00.000Z")],
        bytes: new Uint8Array([1, 2, 3]),
        missing: undefined,
        notFinite: Number.NaN,
      }),
    ).toBe('{"array":["1","2026-01-01T00:00:00.000Z"],"bytes":"AQID","notFinite":"NaN"}');
    expect(fingerprint(taskManifest)).toBe(fingerprint(JSON.parse(stableStringify(taskManifest))));
  });
});

describe("bookmarks and checkpoints", () => {
  it("creates bookmarks, detects stale plans, and signs pagination tokens", async () => {
    const bookmark = createBookmark({
      planFingerprint: "fp_0123456789abcdef",
      snapshot: "snapshot_1",
      position: {
        fileIndex: 1,
        rowGroup: 2,
        rowOffset: 3,
        taskId: "job-task-000001-deadbeef",
        outputManifestCursor: 4,
      },
    });

    expect(() => assertBookmarkMatches(bookmark, "fp_other")).toThrowError(/current query plan/u);
    assertBookmarkMatches(bookmark, "fp_0123456789abcdef");

    const token = await signPaginationToken(bookmark, "secret");
    await expect(verifyPaginationToken(token, "secret")).resolves.toEqual(bookmark);
    const byteSecret = new TextEncoder().encode("byte-secret");
    const compactBookmark = createBookmark({
      planFingerprint: "fp_compact",
      snapshot: "snapshot_2",
      position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
    });
    const byteSecretToken = await signPaginationToken(compactBookmark, byteSecret);
    await expect(verifyPaginationToken(byteSecretToken, byteSecret)).resolves.toEqual(
      compactBookmark,
    );
    await expect(verifyPaginationToken(`${token.slice(0, -1)}x`, "secret")).rejects.toMatchObject({
      code: "LAQL_BOOKMARK_INVALID",
    });
    await expect(verifyPaginationToken(token, "wrong")).rejects.toMatchObject({
      code: "LAQL_BOOKMARK_INVALID",
    });
    await expect(verifyPaginationToken("not-a-token", "secret")).rejects.toMatchObject({
      code: "LAQL_BOOKMARK_INVALID",
    });
  });

  it("rejects signed bookmark payloads with invalid structure", async () => {
    const invalidBookmarks: unknown[] = [
      { version: 2, planFingerprint: "fp", snapshot: "s", position: {} },
      { version: 1, planFingerprint: 1, snapshot: "s", position: {} },
      { version: 1, planFingerprint: "fp", snapshot: "s", position: "bad" },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: -1, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0, taskId: 2 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0, outputManifestCursor: -1 },
      },
    ];

    for (const bookmark of invalidBookmarks) {
      const token = await signRawPayload(stableStringify(bookmark), "secret");
      await expect(verifyPaginationToken(token, "secret")).rejects.toMatchObject({
        code: "LAQL_BOOKMARK_INVALID",
      });
    }
  });

  it("stores task checkpoints defensively and lists them in task order", async () => {
    const checkpoints = memoryCheckpointAdapter();
    await checkpoints.put({
      taskId: "job_3-task-000002-a",
      state: "running",
      idempotencyKey: "idem-2",
      updatedAtMs: 20,
    });
    await checkpoints.put({
      taskId: "job_3-task-000001-a",
      state: "complete",
      idempotencyKey: "idem-1",
      updatedAtMs: 10,
      output: {
        taskId: "job_3-task-000001-a",
        outputPath: "out/a.parquet",
        partitionValues: { country: "US" },
        rowCount: 1,
        byteSize: 2,
      },
    });

    const first = await checkpoints.get("job_3-task-000001-a");
    if (first?.output) first.output.partitionValues.country = "CA";

    const listed: string[] = [];
    for await (const checkpoint of checkpoints.list("job_3")) listed.push(checkpoint.taskId);

    await expect(checkpoints.get("job_3-task-000001-a")).resolves.toMatchObject({
      output: { partitionValues: { country: "US" } },
    });
    expect(listed).toEqual(["job_3-task-000001-a", "job_3-task-000002-a"]);
  });
});

async function signRawPayload(payload: string, secret: string): Promise<string> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    toArrayBuffer(encoder.encode(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, toArrayBuffer(encoder.encode(payload)));
  return `${base64UrlEncode(encoder.encode(payload))}.${base64UrlEncode(new Uint8Array(signature))}`;
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(copy).set(bytes);
  return copy;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}
