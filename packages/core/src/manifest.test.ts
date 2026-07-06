import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { col, eq, gt, isIn, like, lit } from "./expr.js";
import {
  advanceTaskCheckpoint,
  assertBookmarkMatches,
  createBookmark,
  createOutputManifest,
  createOutputManifestFromCheckpoints,
  createTaskManifest,
  fingerprint,
  memoryCheckpointAdapter,
  readOutputManifest,
  signPaginationToken,
  stableStringify,
  transitionTaskCheckpoint,
  verifyPaginationToken,
  writeOutputManifest,
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

  it("keeps task manifests stable across deterministic query variants", async () => {
    const store = memoryStore();
    for (const path of [
      "lake/date=2026-01-02/country=US/c.parquet",
      "lake/date=2026-01-01/country=CA/a.parquet",
      "lake/date=2026-01-01/country=US/b.parquet",
    ]) {
      await store.put(path, new Uint8Array([1]));
    }
    const lake = new Lake({
      store,
      scanner: new EmptyScanner(),
      queryId: () => "q_manifest_property",
    });
    const builders = [
      lake.hive("lake/**/*.parquet").select(["id"]).where(eq("country", "US")),
      lake
        .hive("lake/**/*.parquet")
        .where(isIn("country", ["CA", "US"]))
        .limit(2),
      lake.path("lake/**/*.parquet").select(["id", "amount"]).where(gt("amount", 10)),
      lake
        .path("lake/**/*.parquet")
        .where(like("country", "U%"))
        .orderBy([{ column: "id" }]),
    ];

    for (let index = 0; index < builders.length; index += 1) {
      const jobId = `job_variant_${index}`;
      const manifest = await builders[index]?.taskManifest(jobId);
      const repeated = await builders[index]?.taskManifest(jobId);

      expect(manifest).toEqual(repeated);
      expect(manifest?.tasks.map((task) => task.input.path)).toEqual(
        [...(manifest?.tasks.map((task) => task.input.path) ?? [])].sort(),
      );
      expect(stableStringify(manifest)).toBe(stableStringify(repeated));
      expect(manifest?.planFingerprint).toMatch(/^fp_[0-9a-f]{16}$/u);
    }
  });

  it("preserves row group counts in deterministic task fingerprints", () => {
    const task = {
      path: "lake/sales.parquet",
      size: 1234,
      rowGroupCount: 3,
      rowGroupRanges: [
        { start: 2, end: 3 },
        { start: 0, end: 1 },
      ],
      projectedColumns: ["amount", "id"],
      partitionValues: { country: "US", date: "2026-01-01" },
    };
    const reordered = {
      ...task,
      rowGroupRanges: [
        { start: 0, end: 1 },
        { start: 2, end: 3 },
      ],
      projectedColumns: ["id", "amount"],
      partitionValues: { date: "2026-01-01", country: "US" },
    };
    const withoutCount = {
      ...task,
      rowGroupCount: undefined,
    };
    const differentCount = {
      ...task,
      rowGroupCount: 4,
    };

    const manifest = createTaskManifest({ jobId: "job_row_groups", tasks: [task] });
    const repeated = createTaskManifest({ jobId: "job_row_groups", tasks: [reordered] });
    const noCountManifest = createTaskManifest({ jobId: "job_row_groups", tasks: [withoutCount] });
    const changedManifest = createTaskManifest({
      jobId: "job_row_groups",
      tasks: [differentCount],
    });

    expect(manifest).toEqual(repeated);
    expect(manifest.tasks[0]?.input).toMatchObject({
      rowGroupCount: 3,
      rowGroupRanges: [
        { start: 0, end: 1 },
        { start: 2, end: 3 },
      ],
      projectedColumns: ["amount", "id"],
      partitionValues: { country: "US", date: "2026-01-01" },
    });
    expect(stableStringify(manifest)).toContain('"rowGroupCount":3');
    expect(noCountManifest.tasks[0]?.input).not.toHaveProperty("rowGroupCount");
    expect(noCountManifest.planFingerprint).not.toBe(manifest.planFingerprint);
    expect(noCountManifest.tasks[0]?.id).not.toBe(manifest.tasks[0]?.id);
    expect(changedManifest.planFingerprint).not.toBe(manifest.planFingerprint);
    expect(changedManifest.tasks[0]?.id).not.toBe(manifest.tasks[0]?.id);
  });

  it("normalizes task and output manifest JSON for golden comparisons", () => {
    const taskManifest = goldenTaskManifest();
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
    expect(stableStringify(taskManifest)).toBe(
      goldenFixture("manifests/task-manifest.golden.json"),
    );
    expect(stableStringify(outputManifest)).toBe(
      goldenFixture("manifests/output-manifest.golden.json"),
    );
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

  it("persists and reads normalized output manifests through ObjectStore", async () => {
    const store = memoryStore();
    const manifest = createOutputManifest({
      jobId: "job_persist",
      planFingerprint: "fp_persist",
      entries: [
        {
          taskId: "job_persist-task-000001-b",
          outputPath: "out/b.parquet",
          partitionValues: { date: "2026-01-02", country: "CA" },
          rowCount: 2,
          byteSize: 20,
          contentHash: "sha256:b",
        },
        {
          taskId: "job_persist-task-000000-a",
          outputPath: "out/a.parquet",
          partitionValues: { country: "US", date: "2026-01-01" },
          rowCount: 1,
          byteSize: 10,
          etag: "etag-a",
          iceberg: {
            recordCount: 1,
            fileSizeInBytes: 10,
            partitionValues: { date: "2026-01-01", country: "US" },
          },
        },
      ],
    });

    const written = await writeOutputManifest(store, "manifests/out.json", manifest);
    expect(written).toMatchObject({ path: "manifests/out.json", byteSize: expect.any(Number) });
    await expect(store.head("manifests/out.json")).resolves.toMatchObject({
      contentType: "application/json",
    });
    await expect(readOutputManifest(store, "manifests/out.json")).resolves.toEqual(manifest);
    await expect(
      store
        .get("manifests/out.json")
        .then((bytes) => new TextDecoder().decode(bytes ?? new Uint8Array())),
    ).resolves.toBe(`${stableStringify(manifest)}\n`);
  });

  it("rejects missing or malformed persisted output manifests", async () => {
    const store = memoryStore();
    await expect(readOutputManifest(store, "missing.json")).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });

    await store.put("bad-json.json", new TextEncoder().encode("{"));
    await expect(readOutputManifest(store, "bad-json.json")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });

    await store.put("bad-shape.json", new TextEncoder().encode('{"version":1,"entries":[{}]}'));
    await expect(readOutputManifest(store, "bad-shape.json")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });

    await store.put(
      "bad-entry.json",
      new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          jobId: "job",
          planFingerprint: "fp",
          entries: [
            { taskId: "task", outputPath: "out", partitionValues: {}, rowCount: -1, byteSize: 1 },
          ],
        }),
      ),
    );
    await expect(readOutputManifest(store, "bad-entry.json")).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
    });
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

    expect(stableStringify(bookmark)).toBe(goldenFixture("manifests/bookmark.golden.json"));
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
    const resumableBookmark = createBookmark({
      planFingerprint: "fp_query",
      snapshot: "snapshot_3",
      query: {
        source: "data/*.parquet",
        select: ["id"],
        projections: { doubled: { kind: "arithmetic", op: "mul", left: col("id"), right: lit(2) } },
        where: eq("country", "US"),
        distinct: true,
        windows: {
          rn: {
            fn: "row_number",
            args: [],
            over: { partitionBy: [col("country")], orderBy: [{ expr: col("id") }] },
          },
        },
        qualify: gt("rn", 1),
        orderBy: [{ column: "id", direction: "asc" }],
        limit: 10,
        offset: 1,
        batchSize: 2,
        hive: true,
      },
      position: { fileIndex: 0, rowGroup: 0, rowOffset: 2 },
    });
    await expect(
      verifyPaginationToken(await signPaginationToken(resumableBookmark, "secret"), "secret"),
    ).resolves.toEqual(resumableBookmark);
    const operatorBookmark = createBookmark({
      planFingerprint: "fp_operator",
      snapshot: "snapshot_4",
      position: { fileIndex: 0, rowGroup: 1, rowOffset: 2 },
      operatorState: {
        limitEmitted: 3,
        groupBy: new Uint8Array([1, 2, 3]),
        topK: { spillRef: "topk-state" },
        sort: { spillRef: "sort-state" },
        sketches: { approx: new Uint8Array([4, 5, 6]) },
      },
    });
    const signedOperatorBookmark = await verifyPaginationToken(
      await signPaginationToken(operatorBookmark, "secret"),
      "secret",
    );
    expect(signedOperatorBookmark).toEqual(operatorBookmark);
    expect(stableStringify(operatorBookmark)).toContain('"groupBy":"AQID"');
    expect(stableStringify(operatorBookmark)).toContain('"sort":{"spillRef":"sort-state"}');
    expect(stableStringify(operatorBookmark)).toContain('"approx":"BAUG"');
    const inlineSortBookmark = createBookmark({
      planFingerprint: "fp_sort_operator",
      snapshot: "snapshot_5",
      position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      operatorState: {
        sort: new Uint8Array([7, 8, 9]),
      },
    });
    await expect(
      verifyPaginationToken(await signPaginationToken(inlineSortBookmark, "secret"), "secret"),
    ).resolves.toEqual(inlineSortBookmark);
    expect(stableStringify(inlineSortBookmark)).toContain('"sort":"BwgJ"');
    await expect(verifyPaginationToken(`${token.slice(0, -1)}x`, "secret")).rejects.toMatchObject({
      code: "LAKEQL_BOOKMARK_INVALID",
    });
    await expect(verifyPaginationToken(token, "wrong")).rejects.toMatchObject({
      code: "LAKEQL_BOOKMARK_INVALID",
    });
    await expect(verifyPaginationToken("not-a-token", "secret")).rejects.toMatchObject({
      code: "LAKEQL_BOOKMARK_INVALID",
    });
  });

  it("serializes resumable write state in signed bookmarks", async () => {
    const bookmark = createBookmark({
      planFingerprint: "fp_write",
      snapshot: "snapshot_write",
      position: {
        fileIndex: 0,
        rowGroup: 0,
        rowOffset: 0,
        taskId: "job-write-task-000001-a",
        outputManifestCursor: 1,
      },
      writeState: {
        taskState: "running",
        idempotencyKey: "attempt-1",
        multipart: {
          uploadId: "upload-123",
          path: "out/part.parquet",
          parts: [
            { partNumber: 2, etag: "etag-2", byteSize: 20 },
            { partNumber: 1, etag: "etag-1", byteSize: 10 },
          ],
        },
      },
    });

    expect(bookmark.writeState?.multipart?.parts.map((part) => part.partNumber)).toEqual([1, 2]);
    await expect(
      verifyPaginationToken(await signPaginationToken(bookmark, "secret"), "secret"),
    ).resolves.toEqual(bookmark);
    expect(stableStringify(bookmark)).toContain('"multipart"');
    expect(stableStringify(bookmark)).toContain('"partNumber":1');
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
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
        operatorState: "bad",
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
        operatorState: { limitEmitted: -1 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
        operatorState: { groupBy: { spillRef: "" } },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
        operatorState: { sort: { spillRef: "" } },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
        operatorState: { sketches: { approx: "not base64!" } },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "" },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", select: [1] },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", where: "bad" },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", orderBy: "bad" },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", orderBy: [{}] },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", orderBy: [{ column: "id", direction: "sideways" }] },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", orderBy: [{ column: "id", nulls: "middle" }] },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", limit: -1 },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", offset: -1 },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: "bad",
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: { taskState: "done" },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: { idempotencyKey: "" },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: { multipart: { uploadId: "", path: "out", parts: [] } },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: {
          multipart: {
            uploadId: "upload",
            path: "out",
            parts: [{ partNumber: 0, etag: "etag", byteSize: 1 }],
          },
        },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: {
          multipart: {
            uploadId: "upload",
            path: "out",
            parts: [{ partNumber: 1, etag: "", byteSize: 1 }],
          },
        },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        writeState: {
          multipart: {
            uploadId: "upload",
            path: "out",
            parts: [{ partNumber: 1, etag: "etag", byteSize: -1 }],
          },
        },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", batchSize: 0 },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
      {
        version: 1,
        planFingerprint: "fp",
        snapshot: "s",
        query: { source: "table", hive: "yes" },
        position: { fileIndex: 0, rowGroup: 0, rowOffset: 0 },
      },
    ];

    for (const bookmark of invalidBookmarks) {
      const token = await signRawPayload(stableStringify(bookmark), "secret");
      await expect(verifyPaginationToken(token, "secret")).rejects.toMatchObject({
        code: "LAKEQL_BOOKMARK_INVALID",
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

  it("aggregates output manifests from task checkpoints deterministically", async () => {
    const checkpoints = memoryCheckpointAdapter();
    await checkpoints.put({
      taskId: "job_6-task-000002-b",
      state: "output-written",
      idempotencyKey: "idem-2",
      updatedAtMs: 20,
      output: {
        taskId: "job_6-task-000002-b",
        outputPath: "out/b.parquet",
        partitionValues: { date: "2026-01-02", country: "CA" },
        rowCount: 2,
        byteSize: 20,
        contentHash: "sha256:b",
      },
    });
    await checkpoints.put({
      taskId: "job_6-task-000001-a",
      state: "complete",
      idempotencyKey: "idem-1",
      updatedAtMs: 10,
      output: {
        taskId: "job_6-task-000001-a",
        outputPath: "out/a.parquet",
        partitionValues: { country: "US", date: "2026-01-01" },
        rowCount: 1,
        byteSize: 10,
        contentHash: "sha256:a",
        etag: "etag-a",
      },
    });
    await checkpoints.put({
      taskId: "other-task-000001-a",
      state: "output-written",
      idempotencyKey: "idem-other",
      updatedAtMs: 30,
      output: {
        taskId: "other-task-000001-a",
        outputPath: "out/other.parquet",
        partitionValues: {},
        rowCount: 1,
        byteSize: 1,
      },
    });

    const manifest = await createOutputManifestFromCheckpoints({
      jobId: "job_6",
      planFingerprint: "fp_outputs",
      checkpoints,
    });
    expect(manifest.entries.map((entry) => entry.taskId)).toEqual([
      "job_6-task-000001-a",
      "job_6-task-000002-b",
    ]);
    expect(stableStringify(manifest)).toBe(
      '{"entries":[{"byteSize":10,"contentHash":"sha256:a","etag":"etag-a","outputPath":"out/a.parquet","partitionValues":{"country":"US","date":"2026-01-01"},"rowCount":1,"taskId":"job_6-task-000001-a"},{"byteSize":20,"contentHash":"sha256:b","outputPath":"out/b.parquet","partitionValues":{"country":"CA","date":"2026-01-02"},"rowCount":2,"taskId":"job_6-task-000002-b"}],"jobId":"job_6","planFingerprint":"fp_outputs","version":1}',
    );

    const firstEntry = manifest.entries[0];
    if (!firstEntry) throw new Error("expected output manifest entry");
    firstEntry.partitionValues.country = "MX";
    const repeated = await createOutputManifestFromCheckpoints({
      jobId: "job_6",
      planFingerprint: "fp_outputs",
      checkpoints,
    });
    expect(repeated.entries[0]?.partitionValues.country).toBe("US");
  });

  it("aggregates multiple output manifest entries from one checkpoint", async () => {
    const checkpoints = memoryCheckpointAdapter();
    await checkpoints.put({
      taskId: "job_8-task-000001-a",
      state: "complete",
      idempotencyKey: "idem-1",
      updatedAtMs: 10,
      outputs: [
        {
          taskId: "job_8-task-000001-a",
          outputPath: "out/c.parquet",
          partitionValues: { part: "c" },
          rowCount: 3,
          byteSize: 30,
        },
        {
          taskId: "job_8-task-000001-a",
          outputPath: "out/a.parquet",
          partitionValues: { part: "a" },
          rowCount: 1,
          byteSize: 10,
        },
      ],
    });
    await checkpoints.put({
      taskId: "job_8-task-000000-z",
      state: "complete",
      idempotencyKey: "idem-0",
      updatedAtMs: 5,
      output: {
        taskId: "job_8-task-000000-z",
        outputPath: "out/z.parquet",
        partitionValues: {},
        rowCount: 1,
        byteSize: 1,
      },
    });

    const manifest = await createOutputManifestFromCheckpoints({
      jobId: "job_8",
      planFingerprint: "fp_outputs_multi",
      checkpoints,
    });
    expect(manifest.entries.map((entry) => entry.outputPath)).toEqual([
      "out/z.parquet",
      "out/a.parquet",
      "out/c.parquet",
    ]);

    const outputCheckpoint = await checkpoints.get("job_8-task-000001-a");
    const firstOutput = outputCheckpoint?.outputs?.[0];
    if (firstOutput) firstOutput.partitionValues.part = "mutated";
    const repeated = await checkpoints.get("job_8-task-000001-a");
    expect(repeated?.outputs?.[0]?.partitionValues.part).toBe("c");
    expect(repeated?.outputs?.[1]?.partitionValues.part).toBe("a");
  });

  it("advances task checkpoints idempotently and permits stale requeue", () => {
    const planned = transitionTaskCheckpoint(undefined, {
      taskId: "job_4-task-000001-a",
      nextState: "planned",
      idempotencyKey: "idem-1",
      nowMs: 10,
    });
    const running = transitionTaskCheckpoint(planned, {
      taskId: "job_4-task-000001-a",
      nextState: "running",
      idempotencyKey: "idem-1",
      nowMs: 20,
    });
    const replay = transitionTaskCheckpoint(running, {
      taskId: "job_4-task-000001-a",
      nextState: "running",
      idempotencyKey: "idem-1",
      nowMs: 30,
    });
    const requeued = transitionTaskCheckpoint(running, {
      taskId: "job_4-task-000001-a",
      nextState: "running",
      idempotencyKey: "idem-2",
      nowMs: 100,
      staleTimeoutMs: 10,
    });
    const outputWritten = transitionTaskCheckpoint(requeued, {
      taskId: "job_4-task-000001-a",
      nextState: "output-written",
      idempotencyKey: "idem-2",
      nowMs: 110,
      output: {
        taskId: "job_4-task-000001-a",
        outputPath: "out/file.parquet",
        partitionValues: {},
        rowCount: 1,
        byteSize: 2,
      },
    });

    expect(replay).toEqual(running);
    expect(requeued).toMatchObject({ state: "running", idempotencyKey: "idem-2" });
    expect(outputWritten.output).toMatchObject({ outputPath: "out/file.parquet" });
    expect(stableStringify([planned, running, requeued, outputWritten])).toBe(
      goldenFixture("manifests/retry-log.golden.json"),
    );
  });

  it("persists checkpoint transitions and carries output metadata forward", async () => {
    const checkpoints = memoryCheckpointAdapter();
    const taskId = "job_7-task-000001-a";
    await advanceTaskCheckpoint(checkpoints, {
      taskId,
      nextState: "planned",
      idempotencyKey: "idem-1",
      nowMs: 10,
    });
    await advanceTaskCheckpoint(checkpoints, {
      taskId,
      nextState: "running",
      idempotencyKey: "idem-1",
      nowMs: 20,
    });
    await advanceTaskCheckpoint(checkpoints, {
      taskId,
      nextState: "output-written",
      idempotencyKey: "idem-1",
      nowMs: 30,
      output: {
        taskId,
        outputPath: "out/job-7.parquet",
        partitionValues: { country: "US" },
        rowCount: 7,
        byteSize: 70,
        contentHash: "sha256:7",
      },
    });
    await advanceTaskCheckpoint(checkpoints, {
      taskId,
      nextState: "manifest-recorded",
      idempotencyKey: "idem-1",
      nowMs: 40,
    });
    const complete = await advanceTaskCheckpoint(checkpoints, {
      taskId,
      nextState: "complete",
      idempotencyKey: "idem-1",
      nowMs: 50,
    });

    expect(complete).toMatchObject({
      state: "complete",
      output: { outputPath: "out/job-7.parquet", partitionValues: { country: "US" } },
    });
    await expect(
      createOutputManifestFromCheckpoints({
        jobId: "job_7",
        planFingerprint: "fp_job_7",
        checkpoints,
      }),
    ).resolves.toMatchObject({
      entries: [{ taskId, outputPath: "out/job-7.parquet", rowCount: 7 }],
    });
  });

  it("replays every checkpoint transition as one logical completion", async () => {
    const checkpoints = memoryCheckpointAdapter();
    const taskId = "job_10-task-000001-a";
    const output = {
      taskId,
      outputPath: "out/job-10.parquet",
      partitionValues: { country: "US" },
      rowCount: 10,
      byteSize: 100,
      contentHash: "sha256:10",
    };
    const transitions = [
      { nextState: "planned" as const, nowMs: 10 },
      { nextState: "running" as const, nowMs: 20 },
      { nextState: "output-written" as const, nowMs: 30, output },
      { nextState: "manifest-recorded" as const, nowMs: 40 },
      { nextState: "complete" as const, nowMs: 50 },
    ];

    for (const transition of transitions) {
      const first = await advanceTaskCheckpoint(checkpoints, {
        taskId,
        idempotencyKey: "idem-1",
        ...transition,
      });
      const replayed = await advanceTaskCheckpoint(checkpoints, {
        taskId,
        idempotencyKey: "idem-1",
        ...transition,
      });
      expect(replayed).toEqual(first);
    }

    await expect(checkpoints.get(taskId)).resolves.toMatchObject({
      state: "complete",
      output: { outputPath: "out/job-10.parquet" },
    });
    await expect(
      createOutputManifestFromCheckpoints({
        jobId: "job_10",
        planFingerprint: "fp_job_10",
        checkpoints,
      }),
    ).resolves.toMatchObject({
      entries: [{ taskId, outputPath: "out/job-10.parquet", rowCount: 10 }],
    });
  });

  it("rejects unsafe task checkpoint transitions", () => {
    const running = {
      taskId: "job_5-task-000001-a",
      state: "running" as const,
      idempotencyKey: "idem-1",
      updatedAtMs: 10,
    };

    expect(() =>
      transitionTaskCheckpoint(running, {
        taskId: "job_5-task-000001-a",
        nextState: "complete",
        idempotencyKey: "idem-1",
        nowMs: 20,
      }),
    ).toThrow(/not allowed/u);
    expect(() =>
      transitionTaskCheckpoint(running, {
        taskId: "job_5-task-000001-a",
        nextState: "output-written",
        idempotencyKey: "idem-2",
        nowMs: 20,
        staleTimeoutMs: 100,
      }),
    ).toThrow(/idempotency/u);
    expect(() =>
      transitionTaskCheckpoint(running, {
        taskId: "other-task-000001-a",
        nextState: "output-written",
        idempotencyKey: "idem-1",
        nowMs: 20,
      }),
    ).toThrow(/does not match/u);
  });
});

function goldenTaskManifest() {
  return createTaskManifest({
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
}

function goldenFixture(name: string): string {
  return readFileSync(new URL(`../../../fixtures/data/${name}`, import.meta.url), "utf8").trim();
}

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
