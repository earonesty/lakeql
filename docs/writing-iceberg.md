# Writing Iceberg

Iceberg append support accepts already-written data files and creates the next metadata and manifest objects through an `IcebergCommitCatalog`.

```ts
const result = await table.appendFiles({
  jobId: "job_append_1",
  files: [
    {
      path: "data/new-file.parquet",
      partition: { country: "US" },
      recordCount: 100,
      fileSizeInBytes: 4096,
    },
  ],
});
```

Use `icebergRestCatalog` to commit appends through an Iceberg REST catalog. LaQL
writes the new manifest and metadata objects to the configured `ObjectStore`,
then posts a table update with an `assert-ref-snapshot-id` requirement for the
current `main` branch:

```ts
import { icebergRestCatalog } from "@laql/iceberg";

const catalog = icebergRestCatalog({
  url: "https://catalog.example",
  prefix: "warehouse",
  namespace: ["prod", "analytics"],
  table: "places",
  token: process.env.ICEBERG_CATALOG_TOKEN,
});

await table.appendFiles({
  catalog,
  jobId: "job_append_1",
  files,
});
```

When a catalog returns a committed `metadataPath`, `appendFiles()` reports that
catalog path in its result; otherwise it reports the deterministic next metadata
path LaQL wrote.

Output manifests produced by write tasks can be committed directly when their entries include Iceberg metadata:

```ts
await table.appendOutputManifest({
  manifest: outputManifest,
});
```

The default catalog writes through the configured ObjectStore. Production catalogs should enforce compare-and-swap semantics around the current metadata pointer.
