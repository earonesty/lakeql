import { readFileSync } from "node:fs";
import {
  and,
  between,
  createOutputManifest,
  eq,
  fn,
  isIn,
  isNull,
  LaQLError,
  like,
  lit,
  memoryStore,
  not,
  type Row,
} from "@laql/core";
import { fixturePath, HIVE, ICEBERG } from "@laql/fixtures";
import { beforeAll, describe, expect, it } from "vitest";
import type { IcebergCommitCatalog, IcebergCommitInput } from "./index.js";
import { applyIcebergDeletes, loadIcebergTable, scanPlannedIcebergRows } from "./index.js";

const store = memoryStore();

beforeAll(async () => {
  await store.put(ICEBERG.metadataFile, readFileSync(fixturePath(ICEBERG.metadataFile)));
});

describe("loadIcebergTable", () => {
  it("loads metadata and plans the current snapshot deterministically", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    const plan = table.planFiles({
      where: eq("country", "US"),
      select: ["id", "nation"],
      readMode: "ignore-unsupported-deletes",
    });

    expect(plan).toMatchObject({
      snapshotId: 2,
      schemaId: 2,
      manifestsRead: 1,
      manifestsSkipped: 0,
      filesPlanned: 2,
      filesSkipped: 1,
      deleteFilesPlanned: 0,
      deleteFilesIgnored: 0,
    });
    expect(plan.files.map((file) => file.path)).toEqual([HIVE.files[0], HIVE.files[2]]);
    expect(plan.files.map((file) => file.sequenceNumber)).toEqual([1, 3]);
    expect(plan.files[0]?.projectedFieldIds).toEqual([1, 3]);

    const strictDeletedPartitionPlan = table.planFiles({
      where: eq("country", "CA"),
    });
    expect(strictDeletedPartitionPlan.files[0]).toMatchObject({
      path: HIVE.files[1],
      deleteFiles: [{ content: "equality-delete", path: "deletes/country-ca.eq.parquet" }],
    });
    expect(strictDeletedPartitionPlan).toMatchObject({
      deleteFilesPlanned: 1,
      deleteFilesIgnored: 0,
    });

    const deletedPartitionPlan = table.planFiles({
      where: eq("country", "CA"),
      readMode: "ignore-unsupported-deletes",
    });
    expect(deletedPartitionPlan.files[0]).toMatchObject({
      path: HIVE.files[1],
      deleteFiles: [{ content: "equality-delete", path: "deletes/country-ca.eq.parquet" }],
    });
    expect(deletedPartitionPlan).toMatchObject({
      deleteFilesPlanned: 1,
      deleteFilesIgnored: 0,
    });
    const ignoredDeletePlan = table.planFiles({
      where: eq("country", "CA"),
      readMode: "ignore-deletes",
    });
    expect(ignoredDeletePlan.files[0]?.deleteFiles).toBeUndefined();
    expect(ignoredDeletePlan).toMatchObject({
      deleteFilesPlanned: 0,
      deleteFilesIgnored: 1,
    });
  });

  it("selects snapshots by id, ref, and timestamp", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });

    expect(table.planFiles({ snapshotId: 1 }).files.map((file) => file.path)).toEqual([
      HIVE.files[0],
      HIVE.files[1],
    ]);
    expect(table.planFiles({ ref: "previous" }).snapshotId).toBe(1);
    expect(table.planFiles({ asOfTimestampMs: 1_767_225_600_000 }).snapshotId).toBe(1);
  });

  it("prunes partitions for supported predicate shapes and keeps unknowns conservative", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });

    expect(
      table
        .planFiles({
          snapshotId: 1,
          where: isIn("country", ["CA"]),
          readMode: "ignore-deletes",
        })
        .files.map((file) => file.path),
    ).toEqual([HIVE.files[1]]);

    expect(
      table
        .planFiles({
          snapshotId: 1,
          where: and(between("country", "CA", "US"), not(isNull("country"))),
          readMode: "ignore-deletes",
        })
        .files.map((file) => file.path),
    ).toEqual([HIVE.files[0], HIVE.files[1]]);

    expect(
      table.planFiles({ snapshotId: 1, where: like("country", "U%"), readMode: "ignore-deletes" })
        .files[0]?.path,
    ).toBe(HIVE.files[0]);

    expect(
      table.planFiles({ snapshotId: 1, where: lit(true), readMode: "ignore-deletes" }).files,
    ).toHaveLength(2);
    expect(
      table.planFiles({ snapshotId: 1, where: eq("amount", 10), readMode: "ignore-deletes" }).files,
    ).toHaveLength(2);
    expect(
      table.planFiles({ snapshotId: 1, where: eq("country", "MX"), readMode: "ignore-deletes" }),
    ).toMatchObject({ manifestsRead: 0, manifestsSkipped: 1, filesSkipped: 2 });
    expect(
      table.planFiles({
        snapshotId: 1,
        where: fn("lower", lit("US")),
        readMode: "ignore-deletes",
      }).files,
    ).toHaveLength(2);
  });

  it("throws typed errors for unknown metadata references", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });

    expect(() => table.planFiles({ ref: "missing" })).toThrow(/Unknown Iceberg ref/u);
    expect(() => table.planFiles({ snapshotId: 999 })).toThrow(/Unknown Iceberg snapshot/u);
    expect(() => table.planFiles({ asOfTimestampMs: 1 })).toThrow(/No Iceberg snapshot/u);
    expect(() => table.planFiles({ select: ["missing"], readMode: "ignore-deletes" })).toThrow(
      /Unknown Iceberg column/u,
    );
  });

  it("classifies unknown Iceberg delete files conservatively", async () => {
    const unknownDeleteStore = memoryStore();
    await unknownDeleteStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            {
              "schema-id": 1,
              fields: [{ id: 1, name: "id", type: "int", required: true }],
            },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [
                {
                  path: "manifest-1.json",
                  files: [
                    {
                      path: "data/a.parquet",
                      sequenceNumber: 1,
                      recordCount: 1,
                      deleteFiles: [{ content: "future-delete", path: "deletes/future.bin" }],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      ),
    );
    const table = await loadIcebergTable({
      store: unknownDeleteStore,
      metadataPath: "metadata.json",
    });

    expect(() => table.planFiles()).toThrowError(LaQLError);
    expect(() => table.planFiles()).toThrow(/delete files/u);
    expect(
      table.planFiles({ readMode: "ignore-unsupported-deletes" }).files[0]?.deleteFiles,
    ).toBeUndefined();
    expect(table.planFiles({ readMode: "ignore-unsupported-deletes" })).toMatchObject({
      deleteFilesPlanned: 0,
      deleteFilesIgnored: 1,
    });
  });

  it("applies decoded position, equality, and deletion-vector deletes to data file rows", () => {
    const rows = [
      { id: 1, country: "US", amount: 10 },
      { id: 2, country: "CA", amount: 20 },
      { id: 3, country: "US", amount: 30 },
      { id: 4, country: "MX", amount: 40 },
      { id: 5, country: "US", amount: 50 },
    ];

    expect(
      applyIcebergDeletes({
        dataFilePath: "data/a.parquet",
        rows,
        rowOffset: 10,
        positionDeletes: [
          { path: "data/a.parquet", position: 11 },
          { path: "data/b.parquet", position: 12 },
        ],
        equalityDeletes: [
          { columns: ["country"], row: { country: "MX" } },
          { columns: ["country", "amount"], row: { country: "US", amount: 30 } },
        ],
        deletionVectors: [
          { path: "data/a.parquet", positions: [14] },
          { path: "data/b.parquet", positions: [10] },
        ],
      }),
    ).toEqual([{ id: 1, country: "US", amount: 10 }]);
  });

  it("validates decoded delete inputs before applying them", () => {
    expect(() =>
      applyIcebergDeletes({
        dataFilePath: "data/a.parquet",
        rows: [{ id: 1 }],
        positionDeletes: [{ path: "data/a.parquet", position: -1 }],
      }),
    ).toThrowError(LaQLError);

    expect(() =>
      applyIcebergDeletes({
        dataFilePath: "data/a.parquet",
        rows: [{ id: 1 }],
        deletionVectors: [{ path: "data/a.parquet", positions: [1.5] }],
      }),
    ).toThrow(/delete position/u);

    expect(() =>
      applyIcebergDeletes({
        dataFilePath: "data/a.parquet",
        rows: [{ id: 1 }],
        equalityDeletes: [{ columns: [], row: {} }],
      }),
    ).toThrow(/requires columns/u);

    expect(() =>
      applyIcebergDeletes({
        dataFilePath: "data/a.parquet",
        rows: [{ id: 1 }],
        equalityDeletes: [{ columns: ["missing"], row: { missing: 1 } }],
      }),
    ).toThrow(/Unknown Iceberg equality delete column/u);
  });

  it("scans planned data files through decoded delete readers", async () => {
    async function* dataBatches() {
      yield [
        { id: 1, country: "US", amount: 10 },
        { id: 2, country: "CA", amount: 20 },
      ];
      yield [
        { id: 3, country: "US", amount: 30 },
        { id: 4, country: "MX", amount: 40 },
        { id: 5, country: "US", amount: 50 },
      ];
    }
    const deleteReads: string[] = [];
    const batches: Row[][] = [];

    for await (const batch of scanPlannedIcebergRows({
      plan: [
        {
          path: "data/a.parquet",
          sequenceNumber: 1,
          partition: {},
          recordCount: 5,
          projectedFieldIds: [1, 2, 3],
          snapshotId: 1,
          deleteFiles: [
            { content: "position-delete", path: "deletes/a.pos.parquet" },
            { content: "equality-delete", path: "deletes/a.eq.parquet" },
            { content: "deletion-vector", path: "deletes/a.dv" },
          ],
        },
      ],
      readDataFile: async () => dataBatches(),
      readDeleteFile: async (deleteFile, dataFile) => {
        deleteReads.push(`${dataFile.path}:${deleteFile.path}`);
        if (deleteFile.content === "position-delete") {
          return { positionDeletes: [{ path: dataFile.path, position: 1 }] };
        }
        if (deleteFile.content === "deletion-vector") {
          return { deletionVectors: [{ path: dataFile.path, positions: [4] }] };
        }
        return { equalityDeletes: [{ columns: ["country"], row: { country: "MX" } }] };
      },
    })) {
      batches.push(batch);
    }

    expect(deleteReads).toEqual([
      "data/a.parquet:deletes/a.pos.parquet",
      "data/a.parquet:deletes/a.eq.parquet",
      "data/a.parquet:deletes/a.dv",
    ]);
    expect(batches).toEqual([
      [{ id: 1, country: "US", amount: 10 }],
      [{ id: 3, country: "US", amount: 30 }],
    ]);
  });

  it("appends files by writing a new snapshot and metadata file", async () => {
    const appendStore = memoryStore();
    await appendStore.put(ICEBERG.metadataFile, readFileSync(fixturePath(ICEBERG.metadataFile)));
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const result = await table.appendFiles({
      jobId: "job_append",
      nowMs: 1_767_398_400_000,
      files: [
        {
          path: "appends/date=2026-01-03/country=US/part-000.parquet",
          partition: { date: "2026-01-03", country: "US" },
          recordCount: 2,
          fileSizeInBytes: 123,
        },
      ],
    });

    expect(result).toMatchObject({
      previousSnapshotId: 2,
      snapshotId: 3,
      metadataPath: "iceberg/warehouse/places/metadata/v3.metadata.json",
      manifestPath: "iceberg/warehouse/places/metadata/job_append-3.manifest.json",
    });

    const appended = await loadIcebergTable({
      store: appendStore,
      metadataPath: result.metadataPath,
    });
    const plan = appended.planFiles({
      snapshotId: 3,
      where: eq("country", "US"),
      readMode: "ignore-unsupported-deletes",
    });
    expect(plan.files.map((file) => file.path)).toEqual([
      HIVE.files[0],
      HIVE.files[2],
      "appends/date=2026-01-03/country=US/part-000.parquet",
    ]);
    expect(plan.files.at(-1)).toMatchObject({
      sequenceNumber: 4,
      partition: { country: "US", date: "2026-01-03" },
      recordCount: 2,
      snapshotId: 3,
    });
    await expect(appendStore.head(result.manifestPath)).resolves.toMatchObject({
      contentType: "application/json",
    });
  });

  it("appends Iceberg data files from output manifest entries", async () => {
    const appendStore = memoryStore();
    await appendStore.put(ICEBERG.metadataFile, readFileSync(fixturePath(ICEBERG.metadataFile)));
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const manifest = createOutputManifest({
      jobId: "job_manifest_append",
      planFingerprint: "fp_manifest_append",
      entries: [
        {
          taskId: "task-0",
          outputPath: "appends/date=2026-01-04/country=US/part-000.parquet",
          partitionValues: { country: "US", date: "2026-01-04" },
          rowCount: 2,
          byteSize: 456,
          iceberg: {
            recordCount: 2,
            fileSizeInBytes: 456,
            partitionValues: { country: "US", date: "2026-01-04" },
          },
        },
      ],
    });

    const result = await table.appendOutputManifest({ manifest, nowMs: 1_767_484_800_000 });

    expect(result).toMatchObject({
      snapshotId: 3,
      manifestPath: "iceberg/warehouse/places/metadata/job_manifest_append-3.manifest.json",
    });
    const appended = await loadIcebergTable({
      store: appendStore,
      metadataPath: result.metadataPath,
    });
    expect(
      appended.planFiles({ snapshotId: 3, where: eq("date", "2026-01-04") }).files.at(-1),
    ).toMatchObject({
      path: "appends/date=2026-01-04/country=US/part-000.parquet",
      partition: { country: "US", date: "2026-01-04" },
      recordCount: 2,
    });
  });

  it("rejects output manifest append entries without Iceberg metadata", async () => {
    const appendStore = memoryStore();
    await appendStore.put(ICEBERG.metadataFile, readFileSync(fixturePath(ICEBERG.metadataFile)));
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const manifest = createOutputManifest({
      jobId: "job_missing_iceberg",
      planFingerprint: "fp_missing_iceberg",
      entries: [
        {
          taskId: "task-0",
          outputPath: "appends/missing.parquet",
          partitionValues: {},
          rowCount: 1,
          byteSize: 1,
        },
      ],
    });

    await expect(table.appendOutputManifest({ manifest })).rejects.toMatchObject({
      code: "LAQL_VALIDATION_ERROR",
    });
  });

  it("supports default append paths for metadata without refs", async () => {
    const appendStore = memoryStore();
    await appendStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            {
              "schema-id": 1,
              fields: [{ id: 1, name: "id", type: "int", required: true }],
            },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [
                {
                  path: "manifest-1.json",
                  files: [{ path: "data/a.parquet", sequenceNumber: 7, recordCount: 1 }],
                },
              ],
            },
          ],
        }),
      ),
    );
    const table = await loadIcebergTable({ store: appendStore, metadataPath: "metadata.json" });
    const result = await table.appendFiles({
      files: [{ path: "data/b.parquet", recordCount: 1, fileSizeInBytes: 2 }],
      nowMs: 2,
    });

    expect(result).toMatchObject({
      snapshotId: 2,
      metadataPath: "v2.metadata.json",
      manifestPath: "append-2.manifest.json",
    });
    const appended = await loadIcebergTable({
      store: appendStore,
      metadataPath: result.metadataPath,
    });
    expect(appended.planFiles({ snapshotId: 2 }).files.map((file) => file.sequenceNumber)).toEqual([
      7, 8,
    ]);
  });

  it("turns failed catalog compare-and-swap into a commit conflict", async () => {
    const conflictStore = memoryStore();
    await conflictStore.put(ICEBERG.metadataFile, readFileSync(fixturePath(ICEBERG.metadataFile)));
    const table = await loadIcebergTable({
      store: conflictStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const calls: IcebergCommitInput[] = [];
    const catalog: IcebergCommitCatalog = {
      async commitAppend(input) {
        calls.push(input);
        return false;
      },
    };

    await expect(
      table.appendFiles({
        catalog,
        files: [
          {
            path: "appends/conflict.parquet",
            partition: {},
            recordCount: 1,
            fileSizeInBytes: 10,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAQL_ICEBERG_COMMIT_CONFLICT" });

    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      currentMetadataPath: ICEBERG.metadataFile,
      nextMetadataPath: "iceberg/warehouse/places/metadata/v3.metadata.json",
      expectedSnapshotId: 2,
      nextSnapshotId: 3,
    });
    await expect(
      conflictStore.head("iceberg/warehouse/places/metadata/v3.metadata.json"),
    ).resolves.toBeNull();
  });

  it("fails loudly for missing or malformed metadata", async () => {
    await expect(loadIcebergTable({ store, metadataPath: "missing.json" })).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });

    await store.put("bad.json", new TextEncoder().encode('{"format-version":1}'));
    await expect(loadIcebergTable({ store, metadataPath: "bad.json" })).rejects.toMatchObject({
      code: "LAQL_CATALOG_ERROR",
    });

    await store.put("bad-syntax.json", new TextEncoder().encode("{"));
    await expect(
      loadIcebergTable({ store, metadataPath: "bad-syntax.json" }),
    ).rejects.toMatchObject({
      code: "LAQL_CATALOG_ERROR",
    });

    await store.put("null.json", new TextEncoder().encode("null"));
    await expect(loadIcebergTable({ store, metadataPath: "null.json" })).rejects.toMatchObject({
      code: "LAQL_CATALOG_ERROR",
    });

    await store.put("missing-arrays.json", new TextEncoder().encode('{"format-version":2}'));
    await expect(
      loadIcebergTable({ store, metadataPath: "missing-arrays.json" }),
    ).rejects.toMatchObject({ code: "LAQL_CATALOG_ERROR" });

    await store.put(
      "invalid-required.json",
      new TextEncoder().encode('{"format-version":2,"schemas":[],"snapshots":[]}'),
    );
    await expect(
      loadIcebergTable({ store, metadataPath: "invalid-required.json" }),
    ).rejects.toMatchObject({ code: "LAQL_CATALOG_ERROR" });
  });

  it("validates append inputs and snapshot schemas", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    await expect(table.appendFiles({ files: [] })).rejects.toMatchObject({
      code: "LAQL_VALIDATION_ERROR",
    });

    const badSchemaStore = memoryStore();
    await badSchemaStore.put(
      "bad-schema.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [{ "schema-id": 1, fields: [] }],
          snapshots: [{ "snapshot-id": 1, "timestamp-ms": 1, "schema-id": 99, manifests: [] }],
        }),
      ),
    );
    const badSchema = await loadIcebergTable({
      store: badSchemaStore,
      metadataPath: "bad-schema.json",
    });
    expect(() => badSchema.planFiles()).toThrow(/Unknown Iceberg schema/u);
  });
});
