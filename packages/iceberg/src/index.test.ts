import { readFileSync } from "node:fs";
import { and, between, eq, isIn, isNull, LaQLError, like, lit, memoryStore, not } from "@laql/core";
import { fixturePath, HIVE, ICEBERG } from "@laql/fixtures";
import { beforeAll, describe, expect, it } from "vitest";
import { loadIcebergTable } from "./index.js";

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
    });
    expect(plan.files.map((file) => file.path)).toEqual([HIVE.files[0], HIVE.files[2]]);
    expect(plan.files.map((file) => file.sequenceNumber)).toEqual([1, 3]);
    expect(plan.files[0]?.projectedFieldIds).toEqual([1, 3]);
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
  });

  it("throws typed errors for strict delete handling and unknown metadata references", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });

    expect(() => table.planFiles()).toThrowError(LaQLError);
    expect(() => table.planFiles()).toThrow(/delete files/u);
    expect(() => table.planFiles({ ref: "missing" })).toThrow(/Unknown Iceberg ref/u);
    expect(() => table.planFiles({ snapshotId: 999 })).toThrow(/Unknown Iceberg snapshot/u);
    expect(() => table.planFiles({ asOfTimestampMs: 1 })).toThrow(/No Iceberg snapshot/u);
    expect(() => table.planFiles({ select: ["missing"], readMode: "ignore-deletes" })).toThrow(
      /Unknown Iceberg column/u,
    );
  });

  it("fails loudly for missing or malformed metadata", async () => {
    await expect(loadIcebergTable({ store, metadataPath: "missing.json" })).rejects.toMatchObject({
      code: "LAQL_OBJECT_NOT_FOUND",
    });

    await store.put("bad.json", new TextEncoder().encode('{"format-version":1}'));
    await expect(loadIcebergTable({ store, metadataPath: "bad.json" })).rejects.toMatchObject({
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
});
