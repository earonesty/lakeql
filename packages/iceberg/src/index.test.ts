import { Buffer } from "node:buffer";
import { readFileSync } from "node:fs";
import avro from "avsc";
import {
  and,
  between,
  createOutputManifest,
  eq,
  fn,
  gt,
  isIn,
  isNull,
  LakeqlError,
  like,
  lit,
  memoryCache,
  memoryStore,
  not,
  type ObjectStore,
  type Row,
  stableStringify,
} from "lakeql-core";
import { fixturePath, ICEBERG } from "lakeql-fixtures";
import { beforeAll, describe, expect, it } from "vitest";
import {
  ParquetScanAdapter,
  readIcebergParquetDeletes,
  readParquetObjectBatches,
  readParquetObjects,
  writeParquet,
} from "../../parquet/src/index.js";
import type { IcebergCommitCatalog, IcebergCommitInput, IcebergRestLoadContext } from "./index.js";
import {
  applyIcebergDeletes,
  icebergGlueCatalog,
  icebergNessieCatalog,
  icebergRestCatalog,
  loadIcebergTable,
  loadIcebergTableFromObjectStore,
  loadIcebergTableFromRest,
  planFiles,
  scanPlannedIcebergRows,
} from "./index.js";

const store = memoryStore();

const avroBigIntLongType = avro.types.LongType.__with({
  fromBuffer: (bytes: Buffer) => bytes.readBigInt64LE(),
  toBuffer: (value: bigint | number) => {
    const bytes = Buffer.alloc(8);
    bytes.writeBigInt64LE(BigInt(value));
    return bytes;
  },
  fromJSON: (value: string | number | bigint) => BigInt(value),
  toJSON: (value: bigint | number) => value.toString(),
  isValid: (value: unknown) =>
    typeof value === "bigint" || (typeof value === "number" && Number.isSafeInteger(value)),
  compare: (left: bigint | number, right: bigint | number) => {
    const leftBigInt = BigInt(left);
    const rightBigInt = BigInt(right);
    return leftBigInt === rightBigInt ? 0 : leftBigInt < rightBigInt ? -1 : 1;
  },
});

beforeAll(async () => {
  await putIcebergWarehouse(store);
});

async function putIcebergWarehouse(target: ObjectStore): Promise<void> {
  await target.put(ICEBERG.metadataFile, readFileSync(fixturePath(ICEBERG.metadataFile)));
  await target.put(
    ICEBERG.manifestRefMetadataFile,
    readFileSync(fixturePath(ICEBERG.manifestRefMetadataFile)),
  );
  await target.put(
    ICEBERG.manifestListMetadataFile,
    readFileSync(fixturePath(ICEBERG.manifestListMetadataFile)),
  );
  await target.put(ICEBERG.v1MetadataFile, readFileSync(fixturePath(ICEBERG.v1MetadataFile)));
  await target.put(
    ICEBERG.v1ManifestListFile,
    readFileSync(fixturePath(ICEBERG.v1ManifestListFile)),
  );
  await target.put(ICEBERG.v1ManifestFile, readFileSync(fixturePath(ICEBERG.v1ManifestFile)));
  await target.put(ICEBERG.manifestListFile, readFileSync(fixturePath(ICEBERG.manifestListFile)));
  await target.put(
    ICEBERG.multiManifestMetadataFile,
    readFileSync(fixturePath(ICEBERG.multiManifestMetadataFile)),
  );
  for (const manifestFile of ICEBERG.manifestFiles) {
    await target.put(manifestFile, readFileSync(fixturePath(manifestFile)));
  }
  for (const manifestFile of ICEBERG.legacyManifestFiles) {
    await target.put(manifestFile, readFileSync(fixturePath(manifestFile)));
  }
  await target.put(
    ICEBERG.equalityDeleteFile,
    readFileSync(fixturePath(ICEBERG.equalityDeleteFile)),
  );
  await target.put(
    ICEBERG.positionDeleteFile,
    readFileSync(fixturePath(ICEBERG.positionDeleteFile)),
  );
  for (const file of ICEBERG.dataFiles) {
    await target.put(file, readFileSync(fixturePath(file)));
  }
}

async function* asyncGenerator<T>(values: T[]): AsyncIterable<T> {
  yield* values;
}

async function avroObjectContainer(schema: unknown, records: unknown[]): Promise<Uint8Array> {
  const type = avro.Type.forSchema(schema, {
    typeHook: (innerSchema: unknown) =>
      innerSchema === "long" ||
      (typeof innerSchema === "object" &&
        innerSchema !== null &&
        !Array.isArray(innerSchema) &&
        "type" in innerSchema &&
        innerSchema.type === "long")
        ? avroBigIntLongType
        : undefined,
  });
  const encoder = new avro.streams.BlockEncoder(type, { codec: "null" });
  const chunks: Uint8Array[] = [];
  encoder.on("data", (chunk: Uint8Array) => chunks.push(chunk));
  const done = new Promise<void>((resolve, reject) => {
    encoder.on("end", resolve);
    encoder.on("error", reject);
  });
  for (const record of records) encoder.write(record);
  encoder.end();
  await done;
  const length = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const out = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

describe("loadIcebergTable", () => {
  it("loads a table through the Iceberg REST catalog API", async () => {
    const restStore = memoryStore();
    await putIcebergWarehouse(restStore);
    const metadata = JSON.parse(readFileSync(fixturePath(ICEBERG.metadataFile), "utf8")) as unknown;
    const calls: RestFetchCall[] = [];
    const fakeFetch = restFetch(calls, () =>
      jsonResponse({
        "metadata-location": ICEBERG.metadataFile,
        metadata,
      }),
    );

    const table = await loadIcebergTableFromRest({
      store: restStore,
      url: "https://catalog.example/warehouse",
      prefix: "prod",
      namespace: ["accounting", "tax"],
      table: "places",
      token: "token_123",
      fetch: fakeFetch,
    });

    expect(table.metadataPath).toBe(ICEBERG.metadataFile);
    expect(table.planFiles({ ref: "main" }).snapshotId).toBe(2);
    expect(calls).toMatchObject([
      {
        url: "https://catalog.example/warehouse/v1/prod/namespaces/accounting%1Ftax/tables/places",
        method: "GET",
      },
    ]);
    expect(calls[0]?.headers.get("authorization")).toBe("Bearer token_123");
  });

  it("loads a delegated REST table through warehouse discovery and a store factory", async () => {
    const restStore = memoryStore();
    await putIcebergWarehouse(restStore);
    const metadata = JSON.parse(readFileSync(fixturePath(ICEBERG.metadataFile), "utf8")) as unknown;
    const calls: RestFetchCall[] = [];
    const fakeFetch = restFetch(calls, (input) => {
      const url = String(input);
      if (url.endsWith("/v1/config?warehouse=places")) {
        return jsonResponse({
          defaults: {},
          overrides: { prefix: "places" },
        });
      }
      return jsonResponse(
        {
          "metadata-location": ICEBERG.metadataFile,
          metadata,
          config: {
            "client.region": "us-east-1",
            "s3.access-key-id": "AKID",
          },
          "storage-credentials": [
            {
              prefix: "s3://bucket/warehouse/places",
              config: { "s3.session-token": "TOKEN" },
            },
          ],
        },
        200,
        { etag: '"table-etag"' },
      );
    });
    const contexts: IcebergRestLoadContext[] = [];

    const table = await loadIcebergTableFromRest({
      url: "https://catalog.example",
      warehouse: "places",
      namespace: ["datasets"],
      table: "places_os",
      accessDelegation: ["vended-credentials"],
      fetch: fakeFetch,
      storeFactory: (context) => {
        contexts.push(context);
        return restStore;
      },
    });

    expect(table.planFiles({ ref: "main" }).snapshotId).toBe(2);
    expect(contexts).toMatchObject([
      {
        "metadata-location": ICEBERG.metadataFile,
        config: {
          "client.region": "us-east-1",
          "s3.access-key-id": "AKID",
        },
        "storage-credentials": [
          {
            prefix: "s3://bucket/warehouse/places",
            config: { "s3.session-token": "TOKEN" },
          },
        ],
        etag: '"table-etag"',
      },
    ]);
    expect(calls.map((call) => call.url)).toEqual([
      "https://catalog.example/v1/config?warehouse=places",
      "https://catalog.example/v1/places/namespaces/datasets/tables/places_os",
    ]);
    expect(calls[1]?.headers.get("X-Iceberg-Access-Delegation")).toBe("vended-credentials");
  });

  it("requires REST table loaders to receive or construct an object store", async () => {
    const metadata = JSON.parse(readFileSync(fixturePath(ICEBERG.metadataFile), "utf8")) as unknown;

    await expect(
      loadIcebergTableFromRest({
        url: "https://catalog.example",
        namespace: "prod",
        table: "places",
        fetch: async () =>
          jsonResponse({
            "metadata-location": ICEBERG.metadataFile,
            metadata,
          }),
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  });

  it("lists tables through the Iceberg REST catalog API", async () => {
    const calls: RestFetchCall[] = [];
    const catalog = icebergRestCatalog({
      url: "https://catalog.example/warehouse",
      prefix: "prod",
      namespace: ["accounting", "tax"],
      table: "places",
      token: "token_123",
      fetch: restFetch(calls, () =>
        jsonResponse({
          identifiers: [
            { namespace: ["accounting", "tax"], name: "places" },
            { namespace: ["accounting", "tax"], name: "events" },
          ],
        }),
      ),
    });

    await expect(catalog.listTables()).resolves.toEqual([
      { namespace: ["accounting", "tax"], name: "places" },
      { namespace: ["accounting", "tax"], name: "events" },
    ]);
    expect(calls).toMatchObject([
      {
        url: "https://catalog.example/warehouse/v1/prod/namespaces/accounting%1Ftax/tables",
        method: "GET",
      },
    ]);
    expect(calls[0]?.headers.get("authorization")).toBe("Bearer token_123");
  });

  it("rejects malformed REST catalog table listings", async () => {
    const catalog = icebergRestCatalog({
      url: "https://catalog.example",
      namespace: "prod.analytics",
      table: "places",
      fetch: async () => jsonResponse({ identifiers: [{ namespace: ["prod"], table: "places" }] }),
    });

    await expect(catalog.listTables()).rejects.toMatchObject({
      code: "LAKEQL_CATALOG_ERROR",
    });
  });

  it("loads the current metadata file from an object-store table location", async () => {
    const catalogStore = memoryStore();
    await putIcebergWarehouse(catalogStore);
    await catalogStore.put(
      "iceberg/warehouse/places/metadata/version-hint.text",
      new TextEncoder().encode("2\n"),
    );

    const table = await loadIcebergTableFromObjectStore({
      store: catalogStore,
      tableLocation: "iceberg/warehouse/places/",
    });

    expect(table.metadataPath).toBe(ICEBERG.metadataFile);
    expect(table.planFiles({ ref: "main" }).snapshotId).toBe(2);
  });

  it("falls back to the highest vN metadata file when the version hint is absent", async () => {
    const catalogStore = memoryStore();
    await catalogStore.put(
      "tables/events/metadata/v1.metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "events",
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
              manifests: [],
            },
          ],
        }),
      ),
    );
    await putIcebergWarehouse(catalogStore);

    const table = await loadIcebergTableFromObjectStore({
      store: catalogStore,
      tableLocation: "iceberg/warehouse/places",
    });

    expect(table.metadataPath).toBe(ICEBERG.metadataFile);
    expect(table.snapshot()["snapshot-id"]).toBe(2);
  });

  it("loads metadata and plans the current snapshot deterministically", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    const plan = table.planFiles({
      where: eq("country", "US"),
      select: ["id", "nation"],
      readMode: "ignore-unsupported-deletes",
    });
    expect(
      planFiles(table, {
        where: eq("country", "US"),
        select: ["id", "nation"],
        readMode: "ignore-unsupported-deletes",
      }),
    ).toEqual(plan);

    expect(plan).toMatchObject({
      snapshotId: 2,
      schemaId: 2,
      manifestsRead: 1,
      manifestsSkipped: 0,
      filesPlanned: 2,
      filesSkipped: 1,
      deleteFilesPlanned: 1,
      deleteFilesIgnored: 0,
    });
    expect(plan.files.map((file) => file.path)).toEqual([
      ICEBERG.dataFiles[0],
      ICEBERG.dataFiles[2],
    ]);
    expect(plan.files.map((file) => file.sequenceNumber)).toEqual([1, 3]);
    expect(plan.files[0]?.projectedFieldIds).toEqual([1, 3]);
    expect(plan.files[0]?.deleteFiles).toEqual([
      { content: "position-delete", path: ICEBERG.positionDeleteFile },
    ]);

    const strictDeletedPartitionPlan = table.planFiles({
      where: eq("country", "CA"),
    });
    expect(strictDeletedPartitionPlan.files[0]).toMatchObject({
      path: ICEBERG.dataFiles[1],
      deleteFiles: [{ content: "equality-delete", path: ICEBERG.equalityDeleteFile }],
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
      path: ICEBERG.dataFiles[1],
      deleteFiles: [{ content: "equality-delete", path: ICEBERG.equalityDeleteFile }],
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

  it("loads format-version 1 metadata for read-only planning", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.v1MetadataFile });
    const plan = table.planFiles({
      where: eq("country", "US"),
      select: ["id", "country"],
    });

    expect(table.metadata["format-version"]).toBe(1);
    expect(plan).toMatchObject({
      snapshotId: 1,
      schemaId: 1,
      manifestsRead: 1,
      manifestsSkipped: 0,
      filesPlanned: 1,
      filesSkipped: 1,
      deleteFilesPlanned: 0,
      deleteFilesIgnored: 0,
    });
    expect(plan.files).toEqual([
      {
        path: ICEBERG.dataFiles[0],
        sequenceNumber: 0,
        partition: { country: "US", date: "2026-01-01" },
        recordCount: 4,
        fileSizeInBytes: 257,
        projectedFieldIds: [1, 3],
        snapshotId: 1,
      },
    ]);
    await expect(
      table.appendFiles({
        files: [
          {
            path: `${ICEBERG.tableLocation}/v1-append.parquet`,
            recordCount: 1,
            fileSizeInBytes: 1,
          },
        ],
      }),
    ).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
      details: { formatVersion: 1 },
    });
  });

  it("hydrates Iceberg manifests referenced from metadata", async () => {
    const table = await loadIcebergTable({
      store,
      metadataPath: ICEBERG.manifestRefMetadataFile,
    });

    const plan = table.planFiles({
      where: eq("country", "US"),
      select: ["id", "nation"],
      readMode: "ignore-unsupported-deletes",
    });

    expect(plan).toMatchObject({
      snapshotId: 2,
      manifestsRead: 1,
      filesPlanned: 2,
      filesSkipped: 1,
      deleteFilesPlanned: 1,
    });
    expect(plan.files.map((file) => file.path)).toEqual([
      ICEBERG.dataFiles[0],
      ICEBERG.dataFiles[2],
    ]);
    expect(
      stableStringify({
        snapshotId: plan.snapshotId,
        files: plan.files,
      }),
    ).toBe(readFileSync(fixturePath(ICEBERG.plannedFilesGolden), "utf8").trim());
  });

  it("hydrates Iceberg manifest lists referenced from snapshots", async () => {
    const table = await loadIcebergTable({
      store,
      metadataPath: ICEBERG.manifestListMetadataFile,
    });

    const plan = table.planFiles({
      where: eq("country", "US"),
      select: ["id", "nation"],
      readMode: "ignore-unsupported-deletes",
    });

    expect(plan).toMatchObject({
      snapshotId: 2,
      manifestsRead: 1,
      filesPlanned: 2,
      filesSkipped: 1,
      deleteFilesPlanned: 1,
    });
    expect(plan.files.map((file) => file.path)).toEqual([
      ICEBERG.dataFiles[0],
      ICEBERG.dataFiles[2],
    ]);

    const arrayListStore = memoryStore();
    await arrayListStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "manifest-list.json",
            },
          ],
        }),
      ),
    );
    await arrayListStore.put(
      "manifest-list.json",
      new TextEncoder().encode(
        JSON.stringify([
          {
            path: "manifest.json",
            files: [{ path: "data/a.parquet", sequenceNumber: 1, recordCount: 1 }],
          },
        ]),
      ),
    );
    const arrayListTable = await loadIcebergTable({
      store: arrayListStore,
      metadataPath: "metadata.json",
    });
    expect(arrayListTable.planFiles().files.map((file) => file.path)).toEqual(["data/a.parquet"]);
  });

  it("hydrates Avro Iceberg manifest lists and manifests", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "snap-1.avro",
            },
          ],
        }),
      ),
    );
    await avroStore.put(
      "snap-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_file",
          fields: [
            { name: "manifest_path", type: "string" },
            { name: "added_snapshot_id", type: "long" },
          ],
        },
        [{ manifest_path: "manifest-1.avro", added_snapshot_id: 9_223_372_036_854_775_807n }],
      ),
    );
    await avroStore.put(
      "manifest-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_entry",
          fields: [
            { name: "status", type: "int" },
            { name: "sequence_number", type: "long" },
            {
              name: "data_file",
              type: {
                type: "record",
                name: "data_file",
                fields: [
                  { name: "content", type: "int" },
                  { name: "file_path", type: "string" },
                  {
                    name: "file_format",
                    type: { type: "enum", name: "file_format", symbols: ["PARQUET"] },
                  },
                  { name: "key_metadata", type: ["null", "bytes"], default: null },
                  { name: "split_offsets", type: { type: "array", items: "long" } },
                  { name: "column_sizes", type: { type: "map", values: "long" } },
                  { name: "nan_value_counts", type: { type: "map", values: "long" } },
                  { name: "lower_bounds", type: { type: "map", values: "bytes" } },
                  { name: "flags", type: { type: "array", items: "boolean" } },
                  { name: "quality", type: "float" },
                  { name: "weight", type: "double" },
                  { name: "checksum", type: { type: "fixed", name: "checksum", size: 2 } },
                  {
                    name: "partition",
                    type: {
                      type: "record",
                      name: "partition",
                      fields: [{ name: "country", type: ["null", "string"], default: null }],
                    },
                  },
                  { name: "record_count", type: "long" },
                  { name: "file_size_in_bytes", type: "long" },
                ],
              },
            },
            { name: "data_file_again", type: "data_file" },
          ],
        },
        [
          {
            status: 1,
            sequence_number: 7,
            data_file: {
              content: 0,
              file_path: "data/us.parquet",
              file_format: "PARQUET",
              key_metadata: null,
              split_offsets: [4],
              column_sizes: { id: 12 },
              nan_value_counts: {},
              lower_bounds: { id: Buffer.from([1]) },
              flags: [true, false],
              quality: 1.5,
              weight: 2.5,
              checksum: Buffer.from([1, 2]),
              partition: { country: "US" },
              record_count: 3,
              file_size_in_bytes: 123,
            },
            data_file_again: {
              content: 0,
              file_path: "data/us.parquet",
              file_format: "PARQUET",
              key_metadata: null,
              split_offsets: [4],
              column_sizes: { id: 12 },
              nan_value_counts: {},
              lower_bounds: { id: Buffer.from([1]) },
              flags: [true, false],
              quality: 1.5,
              weight: 2.5,
              checksum: Buffer.from([1, 2]),
              partition: { country: "US" },
              record_count: 3,
              file_size_in_bytes: 123,
            },
          },
          {
            status: 2,
            sequence_number: 8,
            data_file: {
              content: 0,
              file_path: "data/deleted.parquet",
              file_format: "PARQUET",
              key_metadata: Buffer.from([9]),
              split_offsets: [],
              column_sizes: {},
              nan_value_counts: {},
              lower_bounds: {},
              flags: [],
              quality: 0,
              weight: 0,
              checksum: Buffer.from([0, 0]),
              partition: { country: "US" },
              record_count: 1,
              file_size_in_bytes: 10,
            },
            data_file_again: {
              content: 0,
              file_path: "data/deleted.parquet",
              file_format: "PARQUET",
              key_metadata: Buffer.from([9]),
              split_offsets: [],
              column_sizes: {},
              nan_value_counts: {},
              lower_bounds: {},
              flags: [],
              quality: 0,
              weight: 0,
              checksum: Buffer.from([0, 0]),
              partition: { country: "US" },
              record_count: 1,
              file_size_in_bytes: 10,
            },
          },
          {
            status: 1,
            sequence_number: 9,
            data_file: {
              content: 1,
              file_path: "data/delete.parquet",
              file_format: "PARQUET",
              key_metadata: null,
              split_offsets: [],
              column_sizes: {},
              nan_value_counts: {},
              lower_bounds: {},
              flags: [],
              quality: 0,
              weight: 0,
              checksum: Buffer.from([0, 1]),
              partition: { country: "US" },
              record_count: 1,
              file_size_in_bytes: 10,
            },
            data_file_again: {
              content: 1,
              file_path: "data/delete.parquet",
              file_format: "PARQUET",
              key_metadata: null,
              split_offsets: [],
              column_sizes: {},
              nan_value_counts: {},
              lower_bounds: {},
              flags: [],
              quality: 0,
              weight: 0,
              checksum: Buffer.from([0, 1]),
              partition: { country: "US" },
              record_count: 1,
              file_size_in_bytes: 10,
            },
          },
        ],
      ),
    );

    const table = await loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" });
    expect(table.planFiles()).toMatchObject({
      files: [
        {
          path: "data/us.parquet",
          sequenceNumber: 7,
          partition: { country: "US" },
          recordCount: 3,
          fileSizeInBytes: 123,
        },
      ],
    });
  });

  it("rejects invalid Avro Iceberg manifest list entries", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "snap-1.avro",
            },
          ],
        }),
      ),
    );
    await avroStore.put(
      "snap-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_file",
          fields: [{ name: "missing_manifest_path", type: "string" }],
        },
        [{ missing_manifest_path: "manifest-1.avro" }],
      ),
    );

    await expect(
      loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" }),
    ).rejects.toThrow("Iceberg Avro manifest list entry is invalid");
  });

  it("rejects invalid Avro Iceberg manifest records", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "manifest-1.avro" }],
            },
          ],
        }),
      ),
    );
    await avroStore.put(
      "manifest-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_entry",
          fields: [
            { name: "status", type: "int" },
            { name: "data_file", type: "string" },
          ],
        },
        [{ status: 1, data_file: "not-a-record" }],
      ),
    );

    await expect(
      loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" }),
    ).rejects.toThrow("Iceberg Avro manifest entry is missing data_file");
  });

  it("rejects primitive Avro Iceberg manifest records", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "manifest-1.avro" }],
            },
          ],
        }),
      ),
    );
    await avroStore.put("manifest-1.avro", await avroObjectContainer("string", ["not-a-record"]));

    await expect(
      loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" }),
    ).rejects.toThrow("Iceberg Avro manifest entry is invalid");
  });

  it("rejects Avro Iceberg data files with unsafe exposed counts", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "manifest-1.avro" }],
            },
          ],
        }),
      ),
    );
    await avroStore.put(
      "manifest-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_entry",
          fields: [
            { name: "status", type: "int" },
            {
              name: "data_file",
              type: {
                type: "record",
                name: "data_file",
                fields: [
                  { name: "content", type: "int" },
                  { name: "file_path", type: "string" },
                  { name: "record_count", type: "long" },
                ],
              },
            },
          ],
        },
        [
          {
            status: 1,
            data_file: {
              content: 0,
              file_path: "data/us.parquet",
              record_count: 9_223_372_036_854_775_807n,
            },
          },
        ],
      ),
    );

    await expect(
      loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" }),
    ).rejects.toThrow("Iceberg Avro data file has invalid fields");
  });

  it("hydrates Avro Iceberg manifests with omitted optional planning fields", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "manifest-1.avro" }],
            },
          ],
        }),
      ),
    );
    await avroStore.put(
      "manifest-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_entry",
          fields: [
            { name: "status", type: "int" },
            {
              name: "data_file",
              type: {
                type: "record",
                name: "data_file",
                fields: [
                  { name: "file_path", type: "string" },
                  {
                    name: "partition",
                    type: {
                      type: "record",
                      name: "partition",
                      fields: [{ name: "country", type: ["null", "string"], default: null }],
                    },
                  },
                  { name: "record_count", type: "int" },
                  { name: "file_size_in_bytes", type: "int" },
                ],
              },
            },
          ],
        },
        [
          {
            status: 1,
            data_file: {
              file_path: "data/defaults.parquet",
              partition: { country: null },
              record_count: 2,
              file_size_in_bytes: 10,
            },
          },
        ],
      ),
    );

    const table = await loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" });
    expect(table.planFiles().files).toMatchObject([
      {
        path: "data/defaults.parquet",
        sequenceNumber: 0,
        partition: {},
        recordCount: 2,
        fileSizeInBytes: 10,
      },
    ]);
  });

  it("hydrates Avro Iceberg manifests with file sequence fallback fields", async () => {
    const avroStore = memoryStore();
    await avroStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "manifest-1.avro" }],
            },
          ],
        }),
      ),
    );
    await avroStore.put(
      "manifest-1.avro",
      await avroObjectContainer(
        {
          type: "record",
          name: "manifest_entry",
          fields: [
            { name: "status", type: "int" },
            { name: "file_sequence_number", type: "long" },
            {
              name: "data_file",
              type: {
                type: "record",
                name: "data_file",
                fields: [
                  { name: "content", type: "int" },
                  { name: "file_path", type: "string" },
                  { name: "partition", type: "null" },
                  { name: "record_count", type: "long" },
                ],
              },
            },
          ],
        },
        [
          {
            status: 1,
            file_sequence_number: 4,
            data_file: {
              content: 0,
              file_path: "data/fallback.parquet",
              partition: null,
              record_count: 2,
            },
          },
        ],
      ),
    );

    const table = await loadIcebergTable({ store: avroStore, metadataPath: "metadata.json" });
    expect(table.planFiles().files).toMatchObject([
      {
        path: "data/fallback.parquet",
        sequenceNumber: 4,
        partition: {},
        recordCount: 2,
      },
    ]);
  });

  it("reuses manifest-list and manifest reads across hydrated snapshots", async () => {
    const baseStore = memoryStore();
    const reads = new Map<string, number>();
    const countedStore: ObjectStore = {
      async get(path) {
        reads.set(path, (reads.get(path) ?? 0) + 1);
        return await baseStore.get(path);
      },
      getRange(path, range) {
        return baseStore.getRange(path, range);
      },
      put(path, body, options) {
        return baseStore.put(path, body, options);
      },
      delete(path) {
        return baseStore.delete(path);
      },
      list(prefix, options) {
        return baseStore.list(prefix, options);
      },
      head(path) {
        return baseStore.head(path);
      },
    };
    await baseStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 2,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "manifest-list.json",
            },
            {
              "snapshot-id": 2,
              "timestamp-ms": 2,
              "schema-id": 1,
              "manifest-list": "manifest-list.json",
            },
          ],
        }),
      ),
    );
    await baseStore.put(
      "manifest-list.json",
      new TextEncoder().encode(JSON.stringify([{ path: "manifests/shared.json" }])),
    );
    await baseStore.put(
      "manifests/shared.json",
      new TextEncoder().encode(
        JSON.stringify({
          path: "manifests/shared.json",
          files: [
            {
              path: "data/shared.parquet",
              sequenceNumber: 1,
              partition: {},
              recordCount: 2,
              fileSizeInBytes: 10,
            },
          ],
        }),
      ),
    );

    const table = await loadIcebergTable({ store: countedStore, metadataPath: "metadata.json" });

    expect(table.planFiles({ snapshotId: 1 }).files).toHaveLength(1);
    expect(table.planFiles({ snapshotId: 2 }).files).toHaveLength(1);
    expect(reads.get("metadata.json")).toBe(1);
    expect(reads.get("manifest-list.json")).toBe(1);
    expect(reads.get("manifests/shared.json")).toBe(1);
  });

  it("reuses Iceberg manifest metadata from a caller-provided durable cache", async () => {
    const baseStore = memoryStore();
    const reads = new Map<string, number>();
    const countedStore: ObjectStore = {
      async get(path) {
        reads.set(path, (reads.get(path) ?? 0) + 1);
        return await baseStore.get(path);
      },
      getRange(path, range) {
        return baseStore.getRange(path, range);
      },
      put(path, body, options) {
        return baseStore.put(path, body, options);
      },
      delete(path) {
        return baseStore.delete(path);
      },
      list(prefix, options) {
        return baseStore.list(prefix, options);
      },
      head(path) {
        return baseStore.head(path);
      },
    };
    await baseStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "manifest-list.json",
            },
          ],
        }),
      ),
    );
    await baseStore.put(
      "manifest-list.json",
      new TextEncoder().encode(JSON.stringify([{ path: "manifests/shared.json" }])),
    );
    await baseStore.put(
      "manifests/shared.json",
      new TextEncoder().encode(
        JSON.stringify({
          path: "manifests/shared.json",
          files: [
            {
              path: "data/shared.parquet",
              sequenceNumber: 1,
              partition: {},
              recordCount: 2,
              fileSizeInBytes: 10,
            },
          ],
        }),
      ),
    );
    const cache = memoryCache<unknown>();

    const first = await loadIcebergTable({
      store: countedStore,
      metadataPath: "metadata.json",
      cache,
    });
    const second = await loadIcebergTable({
      store: countedStore,
      metadataPath: "metadata.json",
      cache,
    });

    expect(first.planFiles().files).toHaveLength(1);
    expect(second.planFiles().files).toHaveLength(1);
    expect(reads.get("metadata.json")).toBe(2);
    expect(reads.get("manifest-list.json")).toBe(1);
    expect(reads.get("manifests/shared.json")).toBe(1);
  });

  it("counts manifest pruning against generated Iceberg metadata fixtures", async () => {
    const table = await loadIcebergTable({
      store,
      metadataPath: ICEBERG.multiManifestMetadataFile,
    });

    const plan = table.planFiles({
      where: eq("country", "US"),
      readMode: "ignore-unsupported-deletes",
    });

    expect(plan).toMatchObject({
      manifestsRead: 1,
      manifestsSkipped: 1,
      filesPlanned: 2,
      filesSkipped: 1,
    });
    expect(plan.files.map((file) => file.path)).toEqual([
      ICEBERG.dataFiles[0],
      ICEBERG.dataFiles[2],
    ]);
  });

  it("locks Iceberg planned files with Parquet row-group task ranges", async () => {
    const table = await loadIcebergTable({
      store,
      metadataPath: ICEBERG.multiManifestMetadataFile,
    });
    const plan = table.planFiles({
      where: eq("country", "US"),
      readMode: "ignore-unsupported-deletes",
    });
    const scanner = new ParquetScanAdapter(store);
    const tasks = await Promise.all(
      plan.files.map(async (file) => ({
        path: file.path,
        partition: file.partition,
        rowGroupRanges: (
          await scanner.planTask(file.path, {
            where: gt("amount", 100),
            partitionValues: file.partition,
          })
        ).rowGroupRanges,
      })),
    );

    expect(stableStringify({ snapshotId: plan.snapshotId, tasks })).toBe(
      readFileSync(fixturePath(ICEBERG.plannedTasksGolden), "utf8").trim(),
    );
  });

  it("selects snapshots by id, ref, and timestamp", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });

    expect(table.planFiles({ snapshotId: 1 }).files.map((file) => file.path)).toEqual([
      ICEBERG.dataFiles[0],
      ICEBERG.dataFiles[1],
    ]);
    expect(table.planFiles({ ref: "previous" }).snapshotId).toBe(1);
    expect(table.planFiles({ asOfTimestampMs: 1_767_225_600_000 }).snapshotId).toBe(1);
  });

  it("projects rows through Iceberg schema source-id evolution", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });

    expect(
      table.projectRow(
        { id: 1, amount: 10, country: "US", ignored: true },
        { select: ["id", "nation"] },
      ),
    ).toEqual({ id: 1, nation: "US" });
    expect(table.projectRow({ id: 1, amount: 10, nation: "USA" }, { select: ["nation"] })).toEqual({
      nation: "USA",
    });
    expect(table.projectRow({ id: 1, country: "US" })).toEqual({
      id: 1,
      amount: null,
      nation: "US",
    });
    expect(() => table.projectRow({ id: 1 }, { select: ["missing"] })).toThrow(
      /Unknown Iceberg column/u,
    );
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
    ).toEqual([ICEBERG.dataFiles[1]]);

    expect(
      table
        .planFiles({
          snapshotId: 1,
          where: and(between("country", "CA", "US"), not(isNull("country"))),
          readMode: "ignore-deletes",
        })
        .files.map((file) => file.path),
    ).toEqual([ICEBERG.dataFiles[0], ICEBERG.dataFiles[1]]);

    expect(
      table.planFiles({ snapshotId: 1, where: like("country", "U%"), readMode: "ignore-deletes" })
        .files[0]?.path,
    ).toBe(ICEBERG.dataFiles[0]);

    expect(
      table.planFiles({ snapshotId: 1, where: lit(true), readMode: "ignore-deletes" }).files,
    ).toHaveLength(2);
    expect(
      table.planFiles({ snapshotId: 1, where: eq("amount", 10), readMode: "ignore-deletes" }).files,
    ).toHaveLength(2);
    expect(
      table
        .planFiles({
          snapshotId: 1,
          where: and(eq("country", "US"), eq("amount", 10)),
          readMode: "ignore-deletes",
        })
        .files.map((file) => file.path),
    ).toEqual([ICEBERG.dataFiles[0]]);
    expect(
      table.planFiles({ snapshotId: 1, where: gt("country", 30), readMode: "ignore-deletes" })
        .files,
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
    const deleteStore = memoryStore();
    await deleteStore.put(
      "future-delete.metadata.json",
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
    await deleteStore.put(
      "deletion-vector.metadata.json",
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
                      deleteFiles: [{ content: "deletion-vector", path: "deletes/a.dv" }],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      ),
    );

    for (const metadataPath of ["future-delete.metadata.json", "deletion-vector.metadata.json"]) {
      const table = await loadIcebergTable({
        store: deleteStore,
        metadataPath,
      });

      expect(() => table.planFiles()).toThrowError(LakeqlError);
      expect(() => table.planFiles()).toThrow(/delete files/u);
      expect(
        table.planFiles({ readMode: "ignore-unsupported-deletes" }).files[0]?.deleteFiles,
      ).toBeUndefined();
      expect(table.planFiles({ readMode: "ignore-unsupported-deletes" })).toMatchObject({
        deleteFilesPlanned: 0,
        deleteFilesIgnored: 1,
      });
    }
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
    ).toThrowError(LakeqlError);

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

  it("applies position deletes against absolute file row positions from batches", async () => {
    const batches: Row[][] = [];

    for await (const batch of scanPlannedIcebergRows({
      plan: [
        {
          path: "data/a.parquet",
          sequenceNumber: 1,
          partition: {},
          recordCount: 5,
          projectedFieldIds: [1],
          snapshotId: 1,
          deleteFiles: [{ content: "position-delete", path: "deletes/a.pos.parquet" }],
        },
      ],
      readDataFile: async () =>
        asyncGenerator([
          {
            rowOffset: 2,
            rows: [{ id: 3 }, { id: 4 }, { id: 5 }],
          },
        ]),
      readDeleteFile: async () => ({ positionDeletes: [{ path: "data/a.parquet", position: 3 }] }),
    })) {
      batches.push(batch);
    }

    expect(batches).toEqual([[{ id: 3 }, { id: 5 }]]);
  });

  it("aborts planned Iceberg row scans before reads and between delete/data reads", async () => {
    const plan = [
      {
        path: "data/a.parquet",
        sequenceNumber: 1,
        partition: {},
        recordCount: 1,
        projectedFieldIds: [1],
        snapshotId: 1,
        deleteFiles: [{ content: "position-delete", path: "deletes/a.pos.parquet" }],
      },
    ];
    const alreadyAborted = new AbortController();
    alreadyAborted.abort("stop");

    await expect(async () => {
      for await (const _batch of scanPlannedIcebergRows({
        plan,
        signal: alreadyAborted.signal,
        readDataFile: async () => [{ id: 1 }],
        readDeleteFile: async () => ({ positionDeletes: [] }),
      })) {
        // The abort check happens before reads start.
      }
    }).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });

    const abortAfterDelete = new AbortController();
    let dataReads = 0;
    await expect(async () => {
      for await (const _batch of scanPlannedIcebergRows({
        plan,
        signal: abortAfterDelete.signal,
        readDataFile: async () => {
          dataReads += 1;
          return [{ id: 1 }];
        },
        readDeleteFile: async () => {
          abortAfterDelete.abort("after-delete");
          return { positionDeletes: [] };
        },
      })) {
        // The abort check happens after delete reads and before data reads.
      }
    }).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    expect(dataReads).toBe(0);
  });

  it("times out planned Iceberg row scans at await boundaries", async () => {
    await expect(async () => {
      for await (const _batch of scanPlannedIcebergRows({
        plan: [
          {
            path: "data/a.parquet",
            sequenceNumber: 1,
            partition: {},
            recordCount: 1,
            projectedFieldIds: [1],
            snapshotId: 1,
          },
        ],
        maxElapsedMs: 1,
        readDataFile: async () => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return [{ id: 1 }];
        },
        readDeleteFile: async () => ({}),
      })) {
        // The timeout is checked after the slow data read returns.
      }
    }).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  });

  it("applies fixture equality delete files while scanning planned Parquet rows", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    const plan = table.planFiles({ where: eq("country", "CA") });
    const rows: Row[] = [];

    for await (const batch of scanPlannedIcebergRows({
      plan,
      readDataFile: async (file) => {
        const dataRows = await readParquetObjects(store, file.path);
        return dataRows.map((row) => ({ ...file.partition, ...row }));
      },
      readDeleteFile: async (deleteFile) => readIcebergParquetDeletes(store, deleteFile),
    })) {
      rows.push(...batch);
    }

    expect(plan).toMatchObject({ deleteFilesPlanned: 1 });
    expect(rows).toEqual([]);
  });

  it("applies fixture position delete files while scanning planned Parquet rows", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    const plan = table.planFiles({ where: eq("country", "US") });
    const rows: Row[] = [];

    for await (const batch of scanPlannedIcebergRows({
      plan,
      readDataFile: async (file) => {
        const dataRows = await readParquetObjects(store, file.path);
        return dataRows.map((row) => ({ ...file.partition, ...row }));
      },
      readDeleteFile: async (deleteFile) => readIcebergParquetDeletes(store, deleteFile),
    })) {
      rows.push(...batch);
    }

    expect(plan).toMatchObject({ deleteFilesPlanned: 1 });
    expect(rows.map((row) => row.id)).toEqual([0, 2, 3, 200, 201, 202, 203]);
  });

  it("keeps fixture position deletes correct when Parquet reads start after pruned rows", async () => {
    const rows: Row[] = [];

    for await (const batch of scanPlannedIcebergRows({
      plan: [
        {
          path: ICEBERG.dataFiles[0],
          sequenceNumber: 1,
          partition: {},
          recordCount: 4,
          projectedFieldIds: [1],
          snapshotId: 1,
          deleteFiles: [{ content: "position-delete", path: ICEBERG.positionDeleteFile }],
        },
      ],
      readDataFile: async (file) => readParquetObjectBatches(store, file.path, { rowStart: 1 }),
      readDeleteFile: async (deleteFile) => readIcebergParquetDeletes(store, deleteFile),
    })) {
      rows.push(...batch);
    }

    expect(rows.map((row) => row.id)).toEqual([2, 3]);
  });

  it("appends files by writing a new snapshot and metadata file", async () => {
    const appendStore = memoryStore();
    await putIcebergWarehouse(appendStore);
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const result = await table.appendFiles({
      jobId: "job_append",
      nowMs: 1_767_398_400_000,
      nextSnapshotId: 3,
      files: [
        {
          path: `${ICEBERG.tableLocation}/appends/date=2026-01-03/country=US/part-000.parquet`,
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
    const appendedFromCatalog = await loadIcebergTableFromObjectStore({
      store: appendStore,
      tableLocation: "iceberg/warehouse/places",
    });
    const plan = appended.planFiles({
      snapshotId: 3,
      where: eq("country", "US"),
      readMode: "ignore-unsupported-deletes",
    });
    expect(appendedFromCatalog.metadataPath).toBe(result.metadataPath);
    expect(plan.files.map((file) => file.path)).toEqual([
      ICEBERG.dataFiles[0],
      ICEBERG.dataFiles[2],
      `${ICEBERG.tableLocation}/appends/date=2026-01-03/country=US/part-000.parquet`,
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
    await putIcebergWarehouse(appendStore);
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
          outputPath: `${ICEBERG.tableLocation}/appends/date=2026-01-04/country=US/part-000.parquet`,
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

    const result = await table.appendOutputManifest({
      manifest,
      nowMs: 1_767_484_800_000,
      nextSnapshotId: 3,
    });

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
      path: `${ICEBERG.tableLocation}/appends/date=2026-01-04/country=US/part-000.parquet`,
      partition: { country: "US", date: "2026-01-04" },
      recordCount: 2,
    });
  });

  it("reads appended Parquet rows through Iceberg time travel", async () => {
    const appendStore = memoryStore();
    await putIcebergWarehouse(appendStore);
    const dataPath = `${ICEBERG.tableLocation}/appends/date=2026-01-06/country=US/part-000.parquet`;
    const written = await writeParquet(appendStore, dataPath, {
      columnData: [
        { name: "id", data: [901, 902], type: "INT32" },
        { name: "date", data: ["2026-01-06", "2026-01-06"], type: "STRING" },
        { name: "country", data: ["US", "US"], type: "STRING" },
        { name: "amount", data: [91, 92], type: "INT32" },
      ],
    });
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });

    const result = await table.appendFiles({
      jobId: "job_readback_append",
      nowMs: 1_767_657_600_000,
      nextSnapshotId: 3,
      files: [
        {
          path: dataPath,
          partition: { date: "2026-01-06", country: "US" },
          recordCount: 2,
          fileSizeInBytes: written.byteSize,
        },
      ],
    });
    const appended = await loadIcebergTable({
      store: appendStore,
      metadataPath: result.metadataPath,
    });

    expect(
      appended.planFiles({ snapshotId: 2, readMode: "ignore-unsupported-deletes" }).files,
    ).not.toEqual(expect.arrayContaining([expect.objectContaining({ path: dataPath })]));
    const plan = appended.planFiles({
      snapshotId: 3,
      where: eq("date", "2026-01-06"),
      readMode: "ignore-unsupported-deletes",
    });
    const rows: Row[] = [];
    for await (const batch of scanPlannedIcebergRows({
      plan,
      readDataFile: async (file) => readParquetObjects(appendStore, file.path),
      readDeleteFile: async (deleteFile) => readIcebergParquetDeletes(appendStore, deleteFile),
    })) {
      rows.push(...batch);
    }

    expect(plan.files.map((file) => file.path)).toContain(dataPath);
    expect(rows).toEqual([
      { id: 901, date: "2026-01-06", country: "US", amount: 91 },
      { id: 902, date: "2026-01-06", country: "US", amount: 92 },
    ]);
  });

  it("appends files through the Iceberg REST catalog API", async () => {
    const appendStore = memoryStore();
    await putIcebergWarehouse(appendStore);
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const calls: RestFetchCall[] = [];
    const catalog = icebergRestCatalog({
      url: "https://catalog.example",
      namespace: "prod.analytics",
      table: "places",
      fetch: restFetch(calls, () =>
        jsonResponse({
          "metadata-location": "catalog/committed/v3.metadata.json",
          metadata: {},
        }),
      ),
    });

    const result = await table.appendFiles({
      catalog,
      jobId: "job_rest_append",
      nowMs: 1_767_571_200_000,
      nextSnapshotId: 3,
      files: [
        {
          path: `${ICEBERG.tableLocation}/appends/date=2026-01-05/country=US/part-000.parquet`,
          partition: { country: "US", date: "2026-01-05" },
          recordCount: 2,
          fileSizeInBytes: 789,
        },
      ],
    });

    expect(result).toMatchObject({
      snapshotId: 3,
      metadataPath: "catalog/committed/v3.metadata.json",
      manifestPath: "iceberg/warehouse/places/metadata/job_rest_append-3.manifest.json",
    });
    await expect(appendStore.head(result.manifestPath)).resolves.toMatchObject({
      contentType: "application/json",
    });
    await expect(
      appendStore.head("iceberg/warehouse/places/metadata/v3.metadata.json"),
    ).resolves.toMatchObject({
      contentType: "application/json",
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe(
      "https://catalog.example/v1/namespaces/prod%1Fanalytics/tables/places",
    );
    expect(calls[0]?.method).toBe("POST");
    expect(calls[0]?.body).toMatchObject({
      identifier: { namespace: ["prod", "analytics"], name: "places" },
      requirements: [{ type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": 2 }],
      updates: [
        {
          action: "add-snapshot",
          snapshot: {
            "snapshot-id": 3,
            "parent-snapshot-id": 2,
            "timestamp-ms": 1_767_571_200_000,
            "manifest-list": "iceberg/warehouse/places/metadata/job_rest_append-3.manifest.json",
            summary: { operation: "append", "added-data-files": "1", "added-records": "2" },
          },
        },
        {
          action: "set-snapshot-ref",
          "ref-name": "main",
          type: "branch",
          "snapshot-id": 3,
        },
      ],
    });
  });

  it("falls back to the proposed metadata path when REST commit returns no body", async () => {
    const appendStore = memoryStore();
    await putIcebergWarehouse(appendStore);
    const table = await loadIcebergTable({
      store: appendStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const catalog = icebergRestCatalog({
      url: "https://catalog.example",
      namespace: "prod",
      table: "places",
      fetch: async () => new Response(null, { status: 204 }),
    });

    const result = await table.appendFiles({
      catalog,
      nextSnapshotId: 3,
      files: [
        {
          path: `${ICEBERG.tableLocation}/appends/rest-no-body.parquet`,
          partition: {},
          recordCount: 1,
          fileSizeInBytes: 10,
        },
      ],
    });

    expect(result.metadataPath).toBe("iceberg/warehouse/places/metadata/v3.metadata.json");
  });

  it("rejects output manifest append entries without Iceberg metadata", async () => {
    const appendStore = memoryStore();
    await putIcebergWarehouse(appendStore);
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
          outputPath: `${ICEBERG.tableLocation}/appends/missing.parquet`,
          partitionValues: {},
          rowCount: 1,
          byteSize: 1,
        },
      ],
    });

    await expect(table.appendOutputManifest({ manifest })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
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
                  deleteFiles: [{ content: "position-delete", path: "deletes/a.pos.parquet" }],
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
      nextSnapshotId: 2,
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
    expect(appended.metadata.snapshots.at(-1)?.manifests?.[0]?.deleteFiles).toEqual([
      { content: "position-delete", path: "deletes/a.pos.parquet" },
    ]);
  });

  it("turns failed catalog compare-and-swap into a commit conflict", async () => {
    const conflictStore = memoryStore();
    await putIcebergWarehouse(conflictStore);
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
        nextSnapshotId: 3,
        files: [
          {
            path: `${ICEBERG.tableLocation}/appends/conflict.parquet`,
            partition: {},
            recordCount: 1,
            fileSizeInBytes: 10,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ICEBERG_COMMIT_CONFLICT" });

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

  it("rejects stale object-store append commits", async () => {
    const conflictStore = memoryStore();
    await putIcebergWarehouse(conflictStore);
    const metadata = JSON.parse(readFileSync(fixturePath(ICEBERG.metadataFile), "utf8")) as Record<
      string,
      unknown
    >;
    await conflictStore.put(
      ICEBERG.metadataFile,
      new TextEncoder().encode(JSON.stringify(metadata)),
    );
    const table = await loadIcebergTable({
      store: conflictStore,
      metadataPath: ICEBERG.metadataFile,
    });
    metadata["current-snapshot-id"] = 1;
    await conflictStore.put(
      ICEBERG.metadataFile,
      new TextEncoder().encode(JSON.stringify(metadata)),
    );

    await expect(
      table.appendFiles({
        files: [
          {
            path: `${ICEBERG.tableLocation}/appends/stale.parquet`,
            partition: {},
            recordCount: 1,
            fileSizeInBytes: 10,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ICEBERG_COMMIT_CONFLICT" });
  });

  it("requires conditional writes for default object-store appends", async () => {
    const backingStore = memoryStore();
    await putIcebergWarehouse(backingStore);
    const unsafeStore: ObjectStore = {
      get: (path) => backingStore.get(path),
      getRange: (path, range) => backingStore.getRange(path, range),
      put: (path, body, options) => backingStore.put(path, body, options),
      delete: (path) => backingStore.delete(path),
      list: (prefix, options) => backingStore.list(prefix, options),
      head: (path) => backingStore.head(path),
    };
    const table = await loadIcebergTable({
      store: unsafeStore,
      metadataPath: ICEBERG.metadataFile,
    });

    await expect(
      table.appendFiles({
        nextSnapshotId: 3,
        files: [
          {
            path: `${ICEBERG.tableLocation}/appends/unsafe-store.parquet`,
            partition: {},
            recordCount: 1,
            fileSizeInBytes: 10,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });
  });

  it("treats version-hint compare-and-swap failure as an append conflict", async () => {
    const conflictStore = memoryStore();
    await putIcebergWarehouse(conflictStore);
    const table = await loadIcebergTable({
      store: conflictStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const originalConditionalPut = conflictStore.conditionalPut.bind(conflictStore);
    let failNextHintUpdate = true;
    conflictStore.conditionalPut = (path, body, options) => {
      if (path.endsWith("version-hint.text") && failNextHintUpdate) {
        failNextHintUpdate = false;
        return Promise.resolve(false);
      }
      return originalConditionalPut(path, body, options);
    };

    await expect(
      table.appendFiles({
        nextSnapshotId: 3,
        files: [
          {
            path: `${ICEBERG.tableLocation}/appends/cas-conflict.parquet`,
            partition: {},
            recordCount: 1,
            fileSizeInBytes: 10,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ICEBERG_COMMIT_CONFLICT" });
  });

  it("turns REST catalog commit conflicts into Iceberg commit conflicts", async () => {
    const conflictStore = memoryStore();
    await putIcebergWarehouse(conflictStore);
    const table = await loadIcebergTable({
      store: conflictStore,
      metadataPath: ICEBERG.metadataFile,
    });
    const catalog = icebergRestCatalog({
      url: "https://catalog.example",
      namespace: "prod",
      table: "places",
      fetch: async () => jsonResponse({ error: { message: "conflict" } }, 409),
    });

    await expect(
      table.appendFiles({
        catalog,
        files: [
          {
            path: `${ICEBERG.tableLocation}/appends/rest-conflict.parquet`,
            partition: {},
            recordCount: 1,
            fileSizeInBytes: 10,
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ICEBERG_COMMIT_CONFLICT" });
  });

  it("rejects malformed Iceberg REST load responses", async () => {
    await expect(
      loadIcebergTableFromRest({
        store,
        url: "https://catalog.example",
        namespace: "prod",
        table: "places",
        fetch: async () => jsonResponse({ metadata: {} }),
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    await expect(
      loadIcebergTableFromRest({
        store,
        url: "https://catalog.example",
        namespace: "prod",
        table: "places",
        fetch: async () => new Response("nope", { status: 200 }),
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    await expect(
      loadIcebergTableFromRest({
        store,
        url: "https://catalog.example",
        namespace: "prod",
        table: "places",
        fetch: async () => jsonResponse({ error: { message: "boom" } }, 500),
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    expect(() =>
      icebergRestCatalog({
        url: "https://catalog.example",
        namespace: "",
        table: "places",
      }),
    ).toThrow(/namespace/u);
  });

  it("exposes Glue and Nessie catalog stubs through the IcebergCatalog contract", async () => {
    const catalogs = [
      icebergGlueCatalog({ region: "us-east-1", namespace: "prod", table: "places" }),
      icebergNessieCatalog({ url: "https://nessie.example", namespace: ["prod"], table: "places" }),
    ];

    for (const catalog of catalogs) {
      await expect(catalog.loadTable(store)).rejects.toMatchObject({
        code: "LAKEQL_CATALOG_ERROR",
      });
      await expect(catalog.listTables()).rejects.toMatchObject({
        code: "LAKEQL_CATALOG_ERROR",
      });
      await expect(catalog.commitAppend({} as IcebergCommitInput)).rejects.toMatchObject({
        code: "LAKEQL_CATALOG_ERROR",
      });
    }

    expect(() => icebergGlueCatalog({ region: "", namespace: "prod", table: "places" })).toThrow(
      /region/u,
    );
    expect(() =>
      icebergNessieCatalog({ url: "https://nessie.example", namespace: [], table: "places" }),
    ).toThrow(/namespace/u);
  });

  it("fails loudly for missing or malformed metadata", async () => {
    await expect(loadIcebergTable({ store, metadataPath: "missing.json" })).rejects.toMatchObject({
      code: "LAKEQL_OBJECT_NOT_FOUND",
    });

    await store.put("bad.json", new TextEncoder().encode('{"format-version":3}'));
    await expect(loadIcebergTable({ store, metadataPath: "bad.json" })).rejects.toMatchObject({
      code: "LAKEQL_CATALOG_ERROR",
    });

    await store.put("bad-syntax.json", new TextEncoder().encode("{"));
    await expect(
      loadIcebergTable({ store, metadataPath: "bad-syntax.json" }),
    ).rejects.toMatchObject({
      code: "LAKEQL_CATALOG_ERROR",
    });

    await store.put("null.json", new TextEncoder().encode("null"));
    await expect(loadIcebergTable({ store, metadataPath: "null.json" })).rejects.toMatchObject({
      code: "LAKEQL_CATALOG_ERROR",
    });

    await store.put("missing-arrays.json", new TextEncoder().encode('{"format-version":2}'));
    await expect(
      loadIcebergTable({ store, metadataPath: "missing-arrays.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    await store.put(
      "invalid-required.json",
      new TextEncoder().encode('{"format-version":2,"schemas":[],"snapshots":[]}'),
    );
    await expect(
      loadIcebergTable({ store, metadataPath: "invalid-required.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    const badHintStore = memoryStore();
    await badHintStore.put(
      "tables/bad/metadata/version-hint.text",
      new TextEncoder().encode("two"),
    );
    await expect(
      loadIcebergTableFromObjectStore({ store: badHintStore, tableLocation: "tables/bad" }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    await expect(
      loadIcebergTableFromObjectStore({ store: memoryStore(), tableLocation: "tables/missing" }),
    ).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });

    const missingManifestStore = memoryStore();
    await missingManifestStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "missing.manifest.json" }],
            },
          ],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store: missingManifestStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });

    const malformedManifestStore = memoryStore();
    await malformedManifestStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "bad.manifest.json" }],
            },
          ],
        }),
      ),
    );
    await malformedManifestStore.put("bad.manifest.json", new TextEncoder().encode("{}"));
    await expect(
      loadIcebergTable({ store: malformedManifestStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    const missingManifestListStore = memoryStore();
    await missingManifestListStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "missing.manifest-list.json",
            },
          ],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store: missingManifestListStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_OBJECT_NOT_FOUND" });

    const malformedManifestListStore = memoryStore();
    await malformedManifestListStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "bad.manifest-list.json",
            },
          ],
        }),
      ),
    );
    await malformedManifestListStore.put("bad.manifest-list.json", new TextEncoder().encode("{}"));
    await expect(
      loadIcebergTable({ store: malformedManifestListStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });

    await malformedManifestListStore.put(
      "bad.manifest-list.json",
      new TextEncoder().encode(JSON.stringify({ manifests: [{}] })),
    );
    await expect(
      loadIcebergTable({ store: malformedManifestListStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_CATALOG_ERROR" });
  });

  it("rejects unsupported Iceberg metadata features before planning", async () => {
    const baseMetadata = {
      "format-version": 2,
      "table-uuid": "table",
      location: "memory",
      "current-snapshot-id": 1,
      schemas: [{ "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] }],
      snapshots: [{ "snapshot-id": 1, "timestamp-ms": 1, "schema-id": 1, manifests: [] }],
    };

    await store.put(
      "unsupported-partition-transform.json",
      new TextEncoder().encode(
        JSON.stringify({
          ...baseMetadata,
          "partition-specs": [
            {
              "spec-id": 1,
              fields: [
                { "source-id": 1, "field-id": 1000, name: "id_bucket", transform: "bucket[16]" },
              ],
            },
          ],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store, metadataPath: "unsupported-partition-transform.json" }),
    ).rejects.toMatchObject({
      code: "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
      details: { transform: "bucket[16]" },
    });

    await store.put(
      "unsupported-sort-order.json",
      new TextEncoder().encode(
        JSON.stringify({
          ...baseMetadata,
          "sort-orders": [
            {
              "order-id": 1,
              fields: [{ "source-id": 1, transform: "identity", direction: "asc" }],
            },
          ],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store, metadataPath: "unsupported-sort-order.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE" });

    await store.put(
      "unsupported-feature-flags.json",
      new TextEncoder().encode(
        JSON.stringify({
          ...baseMetadata,
          features: ["deletion-vectors"],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store, metadataPath: "unsupported-feature-flags.json" }),
    ).rejects.toMatchObject({
      code: "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
      details: { featureProperty: "features" },
    });

    await store.put(
      "supported-empty-sort-and-identity-partition.json",
      new TextEncoder().encode(
        JSON.stringify({
          ...baseMetadata,
          "partition-specs": [
            {
              "spec-id": 1,
              fields: [{ "source-id": 1, "field-id": 1000, name: "id", transform: "identity" }],
            },
          ],
          "sort-orders": [{ "order-id": 0, fields: [] }],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store, metadataPath: "supported-empty-sort-and-identity-partition.json" }),
    ).resolves.toMatchObject({
      metadata: {
        "partition-specs": [{ "spec-id": 1 }],
        "sort-orders": [{ "order-id": 0 }],
      },
    });
  });

  it("rejects unsupported Iceberg manifest-list content types", async () => {
    const manifestListStore = memoryStore();
    await manifestListStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              "manifest-list": "manifest-list.json",
            },
          ],
        }),
      ),
    );
    await manifestListStore.put(
      "manifest-list.json",
      new TextEncoder().encode(
        JSON.stringify({ manifests: [{ path: "manifest.json", content: 9 }] }),
      ),
    );

    await expect(
      loadIcebergTable({ store: manifestListStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({
      code: "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
      details: { content: 9 },
    });
  });

  it("applies read controls while loading Iceberg metadata and manifests", async () => {
    let activeReads = 0;
    let peakReads = 0;
    const controlledStore: ObjectStore = {
      get: async (path) => {
        activeReads += 1;
        peakReads = Math.max(peakReads, activeReads);
        if (path.endsWith(".manifest.json")) {
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
        activeReads -= 1;
        if (path === "metadata.json") {
          return new TextEncoder().encode(
            JSON.stringify({
              "format-version": 2,
              "table-uuid": "table",
              location: "memory",
              "current-snapshot-id": 1,
              schemas: [
                { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
              ],
              snapshots: [
                {
                  "snapshot-id": 1,
                  "timestamp-ms": 1,
                  "schema-id": 1,
                  manifests: [{ path: "a.manifest.json" }, { path: "b.manifest.json" }],
                },
              ],
            }),
          );
        }
        if (path.endsWith(".manifest.json")) {
          return new TextEncoder().encode(JSON.stringify({ path, files: [] }));
        }
        return null;
      },
      getRange: async () => new Uint8Array(),
      put: async () => {},
      delete: async () => {},
      list: async function* () {},
      head: async () => null,
    };

    await expect(
      loadIcebergTable({
        store: controlledStore,
        metadataPath: "metadata.json",
        maxConcurrentReads: 1,
      }),
    ).resolves.toMatchObject({ metadataPath: "metadata.json" });
    expect(peakReads).toBe(1);
  });

  it("aborts Iceberg metadata loading with signal and timeout controls", async () => {
    const slowStore: ObjectStore = {
      get: async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return new TextEncoder().encode("{}");
      },
      getRange: async () => new Uint8Array(),
      put: async () => {},
      delete: async () => {},
      list: async function* () {},
      head: async () => null,
    };
    const controller = new AbortController();
    controller.abort("stop");

    await expect(
      loadIcebergTable({
        store: slowStore,
        metadataPath: "metadata.json",
        signal: controller.signal,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
    await expect(
      loadIcebergTable({ store: slowStore, metadataPath: "metadata.json", maxElapsedMs: 1 }),
    ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  });

  it("rejects unsafe manifest-sourced paths before store reads", async () => {
    const unsafeDataPathStore = memoryStore();
    await unsafeDataPathStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [
                {
                  path: "manifest.json",
                  files: [
                    {
                      path: "https://evil.test/data.parquet",
                      sequenceNumber: 1,
                      recordCount: 1,
                      deleteFiles: [{ content: "position-delete", path: "../delete.parquet" }],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store: unsafeDataPathStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });

    const unsafeManifestRefStore = memoryStore();
    await unsafeManifestRefStore.put(
      "metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "memory",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [{ path: "//evil.test/manifest.json" }],
            },
          ],
        }),
      ),
    );
    await expect(
      loadIcebergTable({ store: unsafeManifestRefStore, metadataPath: "metadata.json" }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  });

  it("normalizes absolute object-store paths within the table location", async () => {
    const absoluteStore = memoryStore();
    await absoluteStore.put(
      "warehouse/places/metadata/metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "s3://lakeql-bucket/warehouse/places",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [
                {
                  path: "s3://lakeql-bucket/warehouse/places/metadata/manifest.json",
                  files: [
                    {
                      path: "s3://lakeql-bucket/warehouse/places/data/part-000.parquet",
                      sequenceNumber: 1,
                      recordCount: 2,
                      fileSizeInBytes: 123,
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
      store: absoluteStore,
      metadataPath: "warehouse/places/metadata/metadata.json",
    });
    expect(table.planFiles().files).toEqual([
      expect.objectContaining({
        path: "warehouse/places/data/part-000.parquet",
        recordCount: 2,
        fileSizeInBytes: 123,
      }),
    ]);
  });

  it("rejects absolute object-store paths outside the table authority", async () => {
    const crossBucketStore = memoryStore();
    await crossBucketStore.put(
      "warehouse/places/metadata/metadata.json",
      new TextEncoder().encode(
        JSON.stringify({
          "format-version": 2,
          "table-uuid": "table",
          location: "s3://lakeql-bucket/warehouse/places",
          "current-snapshot-id": 1,
          schemas: [
            { "schema-id": 1, fields: [{ id: 1, name: "id", type: "int", required: true }] },
          ],
          snapshots: [
            {
              "snapshot-id": 1,
              "timestamp-ms": 1,
              "schema-id": 1,
              manifests: [
                {
                  path: "s3://other-bucket/warehouse/places/metadata/manifest.json",
                  files: [],
                },
              ],
            },
          ],
        }),
      ),
    );

    await expect(
      loadIcebergTable({
        store: crossBucketStore,
        metadataPath: "warehouse/places/metadata/metadata.json",
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  });

  it("validates append inputs and snapshot schemas", async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    await expect(table.appendFiles({ files: [] })).rejects.toMatchObject({
      code: "LAKEQL_VALIDATION_ERROR",
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

interface RestFetchCall {
  url: string;
  method: string;
  headers: Headers;
  body?: unknown;
}

function restFetch(
  calls: RestFetchCall[],
  handler: (input: RequestInfo | URL, init?: RequestInit) => Response,
): typeof fetch {
  return async (input, init) => {
    calls.push({
      url: String(input),
      method: init?.method ?? "GET",
      headers: new Headers(init?.headers),
      ...(typeof init?.body === "string" ? { body: JSON.parse(init.body) as unknown } : {}),
    });
    return handler(input, init);
  };
}

function jsonResponse(value: unknown, status = 200, headers: HeadersInit = {}): Response {
  const responseHeaders = new Headers(headers);
  responseHeaders.set("content-type", "application/json");
  return new Response(JSON.stringify(value), {
    status,
    headers: responseHeaders,
  });
}
